# -*- coding: utf-8 -*-
"""
local database for poisson clicks detect protocol

@author: tanner stevenson
"""

import base_db
import numpy as np
import pyutils.utils as utils


class LocalDB_PClicksDiscrim(base_db.LocalDB_Base):

    def __init__(self, save_locally=True, data_dir=None):
        super().__init__(save_locally, data_dir)

    @property
    def protocol_name(self):
        ''' PClicksDetect '''
        return 'PClicksDiscrim'

    def _format_sess_data(self, sess_data):
        ''' Format the session data based on the PClicksDiscrim protocol to extract relevant timepoints '''

        # simplify some column names
        sess_data.rename(columns={'viol': 'bail',
                                  'left': 'rel_click_times_left',
                                  'right': 'rel_click_times_right'}, inplace=True)

        sess_data[['rel_click_times_left', 'rel_click_times_right']] = sess_data[['rel_click_times_left', 'rel_click_times_right']].map(lambda x: [x] if utils.is_scalar(x) else x).map(np.array)
        # determine absolute onset of clicks
        sess_data['abs_click_times_left'] = sess_data['rel_click_times_left'] + sess_data['stim_start_time']
        sess_data['abs_click_times_right'] = sess_data['rel_click_times_right'] + sess_data['stim_start_time']

        return sess_data
