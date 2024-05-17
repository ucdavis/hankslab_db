# -*- coding: utf-8 -*-
"""
local database for basic RL tasks protocol

@author: tanner stevenson
"""

import base_db
import numpy as np
from pyutils import utils

class LocalDB_BasicRLTasks(base_db.LocalDB_Base):

    def __init__(self, task_name, save_locally=True, data_dir=None):
        self.__task_name = task_name
        super().__init__(save_locally, data_dir)

    @property
    def protocol_name(self):
        ''' BasicRLTasks '''
        return 'BasicRLTasks'

    @property
    def task_name(self):
        return self.__task_name

    def _format_sess_data(self, sess_data):
        ''' Format the session data based on the protocol '''

        sess_data['chose_left'] = sess_data['choice'] == 'left'
        sess_data['chose_right'] = sess_data['choice'] == 'right'
        
        match self.task_name:
            case 'pavlovCond':
                sess_data['rewarded'] = sess_data['reward'] > 0
                # simplify some column names
                sess_data.rename(columns={'tone_start': 'rel_tone_start_time'}, inplace=True)
                sess_data['abs_tone_start_time'] = sess_data['rel_tone_start_time'] + sess_data['stim_start_time']
            case 'twoArmBandit':
                # add columns for ease of analysis
                sess_data['rewarded'] = sess_data['reward'] > 0
                probs = sess_data[['p_reward_left', 'p_reward_right']]
                sess_data['side_prob'] = probs.iloc[:,0].apply(lambda x: '{:.0f}'.format(x*100)) + '/' + probs.iloc[:,1].apply(lambda x: '{:.0f}'.format(x*100))
                sess_data['block_prob'] = np.max(probs, axis=1).apply(lambda x: '{:.0f}'.format(x*100)) + '/' + np.min(probs, axis=1).apply(lambda x: '{:.0f}'.format(x*100))
                sess_data['high_side'] = (probs.iloc[:,0] > probs.iloc[:,1]).apply(lambda x: 'left' if x else 'right')
                sess_data['chose_high'] = sess_data['choice'] == sess_data['high_side']
                sess_data['choice_prob'] = sess_data.apply(
                    lambda x: x['p_reward_left'] if x['chose_left'] else x['p_reward_right'] if x['chose_right'] else np.nan, axis=1)
                # get previous reward probability of current choice, excluding no responses 
                # this only works if we format one session at a time
                sess_data['choice_prev_prob'] = np.nan
                resp_sel = sess_data['hit'] == True
                sess_data_resp = sess_data[resp_sel]
                prev_p_right = sess_data_resp['p_reward_right'][:-1].to_numpy()
                prev_p_left = sess_data_resp['p_reward_left'][:-1].to_numpy()
                chose_left_resp = sess_data_resp['chose_left'][1:] 
                chose_left_sel = np.insert(sess_data['chose_left'][1:], 0, False)
                chose_right_sel = np.insert(sess_data['chose_right'][1:], 0, False)
                sess_data.loc[chose_left_sel,'choice_prev_prob'] = prev_p_left[chose_left_resp]
                sess_data.loc[chose_right_sel,'choice_prev_prob'] = prev_p_right[~chose_left_resp]
            case 'temporalChoice':
                # fixes for older sessions with bugs
                sess_data['fast_port'] = sess_data['fast_port'].apply(lambda x: x[0] if utils.is_list(x) else x)
                if 'instruct_trial' not in sess_data:
                    sess_data['instruct_trial'] = sess_data['response_port'].apply(lambda x: len(x) == 1)
                
                # collapse reward rate and delay information across response sides
                sess_data['fast_reward_rate'] = sess_data.apply(
                    lambda x: x['reward_rate_left'] if x['fast_port'] == 'left' else x['reward_rate_right'], axis=1)
                sess_data['slow_reward_rate'] = sess_data.apply(
                    lambda x: x['reward_rate_right'] if x['fast_port'] == 'left' else x['reward_rate_left'], axis=1)
                sess_data['slow_delay'] = np.max(sess_data[['reward_delay_left', 'reward_delay_right']], axis=1)
                
                # Get block rates and rewards as fast/slow
                sess_data['block_rates'] = sess_data.apply(lambda x: '{:.0f}/{:.0f}'.format(x['fast_reward_rate'], x['slow_reward_rate']), axis=1)
                sess_data['block_rates_delay'] = sess_data.apply(lambda x: '{}-{:.0f}'.format(x['block_rates'], x['slow_delay']), axis=1)

                poss_rewards = sess_data[['fast_reward_rate', 'slow_reward_rate']] * np.sort(sess_data[['trial_length_left', 'trial_length_right']].to_numpy(), axis=1)
                sess_data['block_rewards'] = poss_rewards.apply(lambda x: '{:.0f}/{:.0f}'.format(x['fast_reward_rate'], x['slow_reward_rate']), axis=1)
                sess_data['block_rewards_delay'] = sess_data.apply(lambda x: '{}-{:.0f}'.format(x['block_rewards'], x['slow_delay']), axis=1)
                
                sess_data['chose_fast_port'] = sess_data['choice'] == sess_data['fast_port']
                sess_data['chose_slow_port'] = ~sess_data['chose_fast_port'] & sess_data['hit']
                sess_data['choice_delay'] = sess_data.apply(
                    lambda x: x['reward_delay_left'] if x['chose_left'] else x['reward_delay_right'] if x['chose_right'] else np.nan, axis=1)
                sess_data['choice_rate'] = sess_data.apply(
                    lambda x: x['reward_rate_left'] if x['chose_left'] else x['reward_rate_right'] if x['chose_right'] else np.nan, axis=1)
                sess_data['cpoke_in_latency'] = sess_data['cpoke_in_time'] - sess_data['cport_on_time']
                
                
        return sess_data
