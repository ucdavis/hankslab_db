# -*- coding: utf-8 -*-
"""
local database for tone categorization delayed response protocol

@author: tanner stevenson
"""

import base_db
import numpy as np
import pandas as pd

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

        # determine absolute onset of tones
        sess_data['abs_tone_start_times'] = sess_data['rel_tone_start_times'] + sess_data['stim_start_time']
        
        # determine side and time of a response poke after a bail
        bail_sel = sess_data['bail'] == True
        # TODO: Update this for the new ITI implementation where ITI is taken at the beginning of the trial
        sess_data.loc[bail_sel, ['choice', 'response_time']] = sess_data.loc[bail_sel].apply(self.__get_bail_response_info, axis=1, result_type='expand')
        
        # determine side of previous stimulus, correct choice, and made choice of the previous full trial
        sess_data['prev_choice_tone_info'] = None
        sess_data['prev_choice_correct_port'] = None
        sess_data['prev_choice_side'] = None
        
        sess_ids = sess_data['sessid'].unique()
        for sess_id in sess_ids:
            sess_sel = sess_data['sessid'] == sess_id
            ind_sess_data = sess_data[sess_sel]
            
            resp_sel = (ind_sess_data['bail'] == False) & (ind_sess_data['choice'] != 'none')
            
            sess_data_resp = ind_sess_data[resp_sel]
            prev_stim = np.insert(sess_data_resp['tone_info'][:-1].to_numpy(), 0, None)
            prev_port = np.insert(sess_data_resp['correct_port'][:-1].to_numpy(), 0, None)
            prev_side = np.insert(sess_data_resp['choice'][:-1].to_numpy(), 0, None)
            
            sess_data.loc[sess_sel & resp_sel,'prev_choice_tone_info'] = prev_stim
            sess_data.loc[sess_sel & resp_sel,'prev_choice_correct_port'] = prev_port
            sess_data.loc[sess_sel & resp_sel,'prev_choice_side'] = prev_side
            
            sess_data.loc[sess_sel, 'prev_choice_tone_info'] = sess_data.loc[sess_sel, 'prev_choice_tone_info'].ffill()
            sess_data.loc[sess_sel, 'prev_choice_correct_port'] = sess_data.loc[sess_sel, 'prev_choice_correct_port'].ffill()
            sess_data.loc[sess_sel, 'prev_choice_side'] = sess_data.loc[sess_sel, 'prev_choice_side'].ffill()
            

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
