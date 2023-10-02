# -*- coding: utf-8 -*-
"""
local database for tone categorization delayed response protocol

@author: tanner stevenson
"""

import base_db

class LocalDB_ToneCatDelayResp(base_db.LocalDB_Base):

    def __init__(self, save_locally=True, reload=False, data_dir=None):
        super().__init__(save_locally, reload, data_dir)

    @property
    def protocol_name(self):
        ''' ToneCatDelayResp '''
        return 'ToneCatDelayResp'

    def _format_sess_data(self, sess_data):
        ''' Format the session data based on the ToneCatDelayResp protocol to extract relevant timepoints '''

        # separate parsed event history into states and events dictionaries for use later
        #peh = sess_data['parsed_events'].transform({'states': lambda x: x['States'], 'events': lambda x: x['Events']})

        if len(sess_data) > 0:
            # simplify some column names
            sess_data.rename(columns={'viol': 'bail',
                                      'tone_start_times': 'rel_tone_start_times'}, inplace=True)

            # determine absolute onset of tones
            sess_data['abs_tone_start_times'] = sess_data['rel_tone_start_times'] + sess_data['stim_start_time']

        return sess_data
