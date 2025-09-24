# -*- coding: utf-8 -*-
"""
local database for tone categorization delayed response protocol

@author: tanner stevenson
"""

import base_db
import numpy as np
import pandas as pd
import pyutils.utils as utils

class LocalDB_ToneCatDelayResp(base_db.LocalDB_Base):

    def __init__(self, save_locally=True, data_dir=None):
        super().__init__(save_locally, data_dir)

    @property
    def protocol_name(self):
        ''' ToneCatDelayResp '''
        return 'ToneCatDelayResp'

    def _format_sess_data(self, sess_data):
        ''' Format the session data based on the ToneCatDelayResp protocol to extract relevant timepoints '''

        # separate parsed event history into states and events dictionaries for use later
        #peh = sess_data['parsed_events'].transform({'states': lambda x: x['States'], 'events': lambda x: x['Events']})

        # simplify some column names
        sess_data.rename(columns={'viol': 'bail',
                                  'tone_start_times': 'rel_tone_start_times'}, inplace=True)

        # old behavior didn't have cport on
        if not 'cport_on_time' in sess_data.columns:
            sess_data['cport_on_time'] = 0.016

        sess_data['cpoke_in_time'] = sess_data['cpoke_in_time'].apply(lambda x: x if utils.is_scalar(x) else np.nan)
        sess_data['cpoke_out_time'] = sess_data['cpoke_out_time'].apply(lambda x: x if utils.is_scalar(x) else np.nan)
        sess_data['cpoke_in_latency'] = sess_data['cpoke_in_time'] - sess_data['cport_on_time']
        sess_data['next_cpoke_in_latency'] = np.append(sess_data['cpoke_in_latency'][1:].to_numpy(), np.nan)
        sess_data['cpoke_out_latency'] = sess_data['cpoke_out_time'] - sess_data['response_cue_time']
        
        # determine absolute onset of tones
        sess_data['abs_tone_start_times'] = sess_data['rel_tone_start_times'] + sess_data['stim_start_time']

        if 'tone_end_times' in sess_data.columns:
            sess_data.rename(columns={'tone_end_times': 'rel_tone_end_times'}, inplace=True)
            sess_data['abs_tone_end_times'] = sess_data['rel_tone_end_times'] + sess_data['stim_start_time']
            
        # if they poked out before a tone was heard, set to nan
        poke_out_pre_tone = sess_data['cpoke_out_time'] < sess_data['abs_tone_start_times']
        sess_data.loc[poke_out_pre_tone, 'abs_tone_start_times'] = np.nan
        sess_data.loc[poke_out_pre_tone, 'abs_tone_end_times'] = np.nan

        # determine side and time of a response poke after a bail
        # bail_sel = sess_data['bail'] == True
        # TODO: Update this for the new ITI implementation where ITI is taken at the beginning of the trial
        # sess_data.loc[bail_sel, ['choice', 'response_time']] = sess_data.loc[bail_sel].apply(self.__get_bail_response_info, axis=1, result_type='expand')
        
        choices = sess_data['choice']
        sess_data['chose_left'] = choices == 'left'
        sess_data['chose_right'] = choices == 'right'
        sess_data['rewarded'] = sess_data['reward'] > 0

        # Previous and future trial information
        # this only works when formatting one session at a time
        resp_sel = sess_data['choice'] != 'none'
        
        sess_data['prev_choice'] = None
        sess_data['prev_tone_info'] = None
        sess_data['prev_correct_port'] = None
        sess_data.iloc[1:, sess_data.columns.get_loc('prev_choice')] = choices.iloc[:-1]
        sess_data.iloc[1:, sess_data.columns.get_loc('prev_tone_info')] = sess_data['tone_info'].iloc[:-1]
        sess_data.iloc[1:, sess_data.columns.get_loc('prev_correct_port')] = sess_data['correct_port'].iloc[:-1]
        
        sess_data['next_choice'] = None
        sess_data['next_tone_info'] = None
        sess_data['next_correct_port'] = None
        sess_data.iloc[:-1, sess_data.columns.get_loc('next_choice')] = choices.iloc[1:]
        sess_data.iloc[:-1, sess_data.columns.get_loc('next_tone_info')] = sess_data['tone_info'].iloc[1:]
        sess_data.iloc[:-1, sess_data.columns.get_loc('next_correct_port')] = sess_data['correct_port'].iloc[1:]

        sess_data['incongruent'] = sess_data['tone_info'].apply(lambda x: x[0] != x[-1] if utils.is_list(x) and len(x) == 2 else False)

        sess_data['switch'] = False
        sess_data['stay'] = False
        sess_data['next_switch'] = False
        sess_data['next_stay'] = False
        
        prev_resp_sel = resp_sel.to_numpy().copy()
        prev_resp_sel[resp_sel.idxmax()] = False
        next_resp_sel = resp_sel.to_numpy().copy()
        next_resp_sel[resp_sel[::-1].idxmax()] = False
        
        resp_choices = choices[resp_sel].to_numpy()
        switch = resp_choices[1:] != resp_choices[:-1]
        sess_data.loc[prev_resp_sel, 'switch'] = switch
        sess_data.loc[prev_resp_sel, 'stay'] = ~switch
        sess_data.loc[next_resp_sel, 'next_switch'] = switch
        sess_data.loc[next_resp_sel, 'next_stay'] = ~switch
        
        # fix bugs/account for protocol variability
        # some response times were None
        sess_data['response_time'] = sess_data['response_time'].apply(lambda x: x if not x is None else np.nan)
        sess_data['RT'] = sess_data['RT'].apply(lambda x: x if not x is None else np.nan)
        
        # fix for old sessions not having a reward time
        if not 'reward_time' in sess_data.columns:
            if sess_data['sessid'].iloc[0] < 95035:
                sess_data['reward_time'] = sess_data['response_time']
            else:
                sess_data['reward_time'] = sess_data['response_time'] + 0.5
                
        # fill in reward times for unrewarded trials, has been fixed in newer version of protocol
        unrew_sel = (sess_data['reward'] == 0) & resp_sel
        if any(np.isnan(sess_data.loc[unrew_sel, 'reward_time'])):
            reward_delay = np.nanmean(sess_data['reward_time'] - sess_data['response_time'])
            sess_data.loc[unrew_sel, 'reward_time'] = sess_data.loc[unrew_sel, 'response_time'] + reward_delay

        return sess_data

    def __get_bail_response_info(self, row):
        ''' Parse bail events to determine the choice and side of a response after a bail '''
        response_time = np.nan
        choice = 'none'

        peh = row['parsed_events']
        bail_time = peh['States']['Bail'][0]
        if bail_time is not None:
            if 'Port1In' in peh['Events']:
                left_pokes = np.array(peh['Events']['Port1In'])
            else:
                left_pokes = np.array([])

            if 'Port3In' in peh['Events']:
                right_pokes = np.array(peh['Events']['Port3In'])
            else:
                right_pokes = np.array([])

            post_bail_left_pokes = left_pokes[left_pokes > bail_time]
            post_bail_right_pokes = right_pokes[right_pokes > bail_time]

            if len(post_bail_left_pokes) > 0 and len(post_bail_right_pokes) > 0:
                first_left = post_bail_left_pokes[0]
                first_right = post_bail_right_pokes[0]

                if first_left < first_right:
                    choice = 'left'
                    response_time = first_left
                else:
                    choice = 'right'
                    response_time = first_right

            elif len(post_bail_left_pokes) > 0:
                choice = 'left'
                response_time = post_bail_left_pokes[0]

            elif len(post_bail_right_pokes) > 0:
                choice = 'right'
                response_time = post_bail_right_pokes[0]


        return {'choice': choice, 'response_time': response_time}
