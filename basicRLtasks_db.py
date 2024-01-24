# -*- coding: utf-8 -*-
"""
local database for tone categorization delayed response protocol

@author: tanner stevenson
"""

import base_db

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

        match self.task_name:
            case 'pavlovCond':
                # simplify some column names
                sess_data.rename(columns={'rewarded': 'reward_tone',
                                          'tone_start': 'rel_tone_start_time'}, inplace=True)
                sess_data['abs_tone_start_time'] = sess_data['rel_tone_start_time'] + sess_data['stim_start_time']

        return sess_data
