# -*- coding: utf-8 -*-
"""
Local database base class for behavioral, ephys and fiber photometry data so there is no need to pull data from the server every time it is needed

@author: tanner stevenson
"""

from abc import ABC, abstractmethod
import os.path as path
import glob
import pandas as pd
import numpy as np
import pickle
import db_access
import pyutils.utils as utils
from collections import Counter


# make this class abstract so other local database classed can inherit and implement their unique
# handling of behaviorally relevant method variables
class LocalDB_Base(ABC):

    def __init__(self, save_locally=True, data_dir=None):
        '''
        Initialize the local database

        Parameters
        ----------
        save_locally : Whether to save the data locally. The default is True.
        reload : Whether to reload the data from the database. The default is False.
        data_dir : The directory where the local data will be persisted. The default is ~/db_data/[protocol_name].

        '''

        self._save_locally = save_locally

        if data_dir is None or not path.exists(data_dir):
            data_dir = path.join(utils.get_user_home(), 'db_data', self.protocol_name)

        self.__data_dir = data_dir
        self.__load_local_data()

    #%% Properties

    @property
    def data_dir(self):
        return self.__data_dir

    @property
    def local_sessions(self):
        ''' Get all local session ids, with associated subject ids and dates '''
        return self.__local_data['sessions']

    @property
    def local_units(self):
        ''' Get all local unit ids, with associated subject and session ids '''
        return self.__local_data['units']

    @property
    def local_fp_data(self):
        ''' Get all local fiber photometry ids, with associated subject and session ids '''
        return self.__local_data['fp_data']

    @property
    def local_subjects(self):
        ''' Get all local subject ids and their respective local unit, session, and fp ids '''
        sess_ids = self.__local_data['sessions'].groupby('subjid').agg(list)['sessid'].apply(lambda x: sorted(x))
        unit_ids = self.__local_data['units'].groupby('subjid').agg(list)['unitid'].apply(lambda x: sorted(x))
        fp_ids = self.__local_data['fp_data'].groupby('subjid').agg(list)['fpid'].apply(lambda x: sorted(x))
        return pd.concat([sess_ids, unit_ids, fp_ids])

    #%% Public Methods

    def get_behavior_data(self, sess_ids, reload=False):
        '''
        Gets behavioral data for the given session ids and optionally persists the retreived data

        Parameters
        ----------
        sess_ids : list of session ids to retreive
        reload : Whether to reload the local information from the database. Default False.

        Returns
        -------
        A pandas table of behavioral data
        '''

        if utils.is_scalar(sess_ids):
            sess_ids = [sess_ids]

        sess_ids = sorted(sess_ids)
        beh_data = pd.DataFrame()

        for sess_id in sess_ids:
            # see if data already exists
            data_path = self._get_sess_beh_path(sess_id)

            if path.exists(data_path) and not reload:
                sess_data = pd.read_pickle(data_path)
            else:  # reload data
                sess_data = db_access.get_session_data(sess_id)
                if len(sess_data) == 0:
                    continue

                sess_data = self._format_sess_data(sess_data)

                if self._save_locally:
                    utils.check_make_dir(data_path)
                    sess_data.to_pickle(data_path)

                self.__update_local_sessions(sess_data)

            beh_data = pd.concat([beh_data, sess_data], ignore_index=True)

        if len(beh_data) > 0:
            beh_data = beh_data.sort_values(['sessid', 'trial']).reset_index(drop=True)

        return beh_data


    def get_subj_behavior_data(self, subj_ids, reload=False):
        '''
        Gets all behavioral data with unit data for the given subject ids

        Parameters
        ----------
        subj_ids : list of subject ids

        Returns
        -------
        A pandas table of behavioral data
        '''

        sess_ids = db_access.get_subj_unit_sess_ids(subj_ids)
        return self.get_behavior_data(utils.flatten(sess_ids), reload)


    def get_unit_behavior_data(self, unit_ids, reload=False):
        '''
        Gets all behavioral data for the given unit ids

        Parameters
        ----------
        unit_ids : list of unit ids

        Returns
        -------
        A pandas table of behavioral data
        '''

        sess_ids = db_access.get_unit_sess_ids(unit_ids)
        return self.get_behavior_data(sess_ids.keys(), reload)


    def get_local_behavior_data(self):
        '''
        Gets all behavioral data stored locally

        Returns
        -------
        A pandas table of behavioral data
        '''

        return self.get_behavior_data(self.local_sessions['sessid'])


    def get_protocol_unit_subject_ids(self):
        ''' Get all subject ids from the database that have unit data for the particular protocol '''
        return db_access.get_unit_protocol_subj_ids(self.protocol_name)


    def get_unit_data(self, unit_ids, reload=False):
        '''
        Gets unit data for the given ids and optionally persists the retreived data

        Parameters
        ----------
        unit_ids : list of unit ids to retreive
        reload : Whether to reload the local information from the database. Default False.

        Returns
        -------
        A pandas table of unit data
        '''

        unit_ids = sorted(unit_ids)
        unit_data = pd.DataFrame()

        # see if we need to pull any units from the database
        if reload:
            missing_units = unit_ids
        else:
            missing_units = np.setdiff1d(unit_ids, self.local_units['unitid'])

        unit_sess_ids = db_access.get_unit_sess_ids(unit_ids)

        # go through the units by session and either load existing or save new data
        for sess_id, sess_unit_ids in unit_sess_ids.items():
            # load local data
            data_path = self._get_sess_unit_path(sess_id)
            if path.exists(data_path):
                sess_unit_data = pd.read_pickle(data_path)
            else:
                sess_unit_data = None

            # see if we need to load any new unit data
            missing_sess_units = np.intersect1d(sess_unit_ids, missing_units)
            if len(missing_sess_units) > 0:
                new_unit_data = db_access.get_unit_data(missing_sess_units)
                new_unit_data = self._format_unit_data(new_unit_data)

                if sess_unit_data is None:
                    # this is the first time we've loaded data for this session
                    sess_unit_data = new_unit_data
                else:  # we have data already
                    if reload:  # we need to remove reloaded rows
                        sess_unit_data.drop(sess_unit_data[sess_unit_data['unitid'].isin(
                            new_unit_data['unitid'])].index, inplace=True)

                    # append new rows preserving any additional columns
                    sess_unit_data = pd.concat([sess_unit_data, new_unit_data],
                                               ignore_index=True).sort_values('unitid')

                # save the new session unit data
                if self._save_locally:
                    utils.check_make_dir(data_path)
                    sess_unit_data.to_pickle(data_path)

                self.__update_local_units(new_unit_data)

            # now that we have all of our session unit data, parse it down to the units of interest
            unit_data = pd.concat([unit_data, sess_unit_data[sess_unit_data['unitid'].isin(unit_ids)]])

        return unit_data.sort_values('unitid').reset_index()


    def get_subj_unit_data(self, subj_ids, reload=False):
        '''
        Gets all unit data for the given subject ids

        Parameters
        ----------
        subj_ids : list of subject ids

        Returns
        -------
        A pandas table of unit data
        '''

        unit_ids = db_access.get_subj_unit_ids(subj_ids)
        return self.get_unit_data(utils.flatten(unit_ids), reload)


    def get_sess_unit_data(self, sess_ids, reload=False):
        '''
        Gets all unit data for the given session ids

        Parameters
        ----------
        sess_ids : list of session ids

        Returns
        -------
        A pandas table of unit data
        '''

        unit_ids = db_access.get_sess_unit_ids(sess_ids)
        return self.get_unit_data(utils.flatten(unit_ids), reload)


    def get_local_unit_data(self):
        '''
        Gets all unit data stored locally

        Returns
        -------
        A pandas table of unit data
        '''

        return self.get_unit_data(self.local_units['unitid'])


    def get_fp_data(self, fp_ids, reload=False):
        '''
        Gets fiber photometry data for the given ids and optionally persists the retreived data

        Parameters
        ----------
        fp_ids : list of ids to retreive fiber photometry data for
        reload : Whether to reload the local information from the database. Default False.

        Returns
        -------
        A dictionary of fiber photometry data and metadata associated with it, keyed by subject id and session id
        '''

        fp_ids = sorted(fp_ids)
        fp_data = pd.DataFrame()

        # see if we need to pull any units from the database
        if reload:
            missing_ids = fp_ids
        else:
            missing_ids = np.setdiff1d(fp_ids, self.local_fp_data['fpid'])

        fp_sess_ids = db_access.get_fp_sess_ids(fp_ids)

        # go through the fp ids by session and either load existing or save new data
        for sess_id, sess_fp_ids in fp_sess_ids.items():
            # load local data
            data_path = self._get_sess_fp_path(sess_id)
            if path.exists(data_path):
                sess_fp_data = pd.read_pickle(data_path)
            else:
                sess_fp_data = None

            # see if we need to load any new fp data
            missing_sess_fp = np.intersect1d(sess_fp_ids, missing_ids)
            if len(missing_sess_fp) > 0:
                new_fp_data = db_access.get_fp_data(missing_sess_fp)
                new_fp_data = self._format_fp_data(new_fp_data)

                if sess_fp_data is None:
                    # this is the first time we've loaded data for this session
                    sess_fp_data = new_fp_data
                else:  # we have data already
                    if reload:  # we need to remove reloaded rows
                        sess_fp_data.drop(sess_fp_data[sess_fp_data['fpid'].isin(
                            new_fp_data['fpid'])].index, inplace=True)

                    # append new rows preserving any additional columns
                    sess_fp_data = pd.concat([sess_fp_data, new_fp_data],
                                               ignore_index=True).sort_values('fpid')

                # save the new session unit data
                if self._save_locally:
                    utils.check_make_dir(data_path)
                    sess_fp_data.to_pickle(data_path)

                self.__update_local_fp_data(new_fp_data)

            # now that we have all of our session fp data, parse it down to the fpids of interest
            fp_data = pd.concat([fp_data, sess_fp_data[sess_fp_data['fpid'].isin(fp_ids)]])

        # reformat the fp data into a nested dictionary for ease of manipulation and viewing
        subj_sess_ids = fp_data.groupby('subjid')['sessid'].agg(list).apply(np.unique).to_dict()

        # get the implant information for each subject and region
        implant_info = {subjid: fp_data[fp_data['subjid'] == subjid][['region', 'side', 'AP', 'ML', 'DV', 'fiber_type']].drop_duplicates().set_index('region').to_dict('index') for subjid in subj_sess_ids.keys()}

        # get the fiber photometry data organized by subject and session
        fp_data_dict = {subj_id: {sess_id: {} for sess_id in sess_ids} for subj_id, sess_ids in subj_sess_ids.items()}
        for subj_id, sess_ids in subj_sess_ids.items():
            for sess_id in sess_ids:
                sess_fp_data = fp_data[(fp_data['subjid'] == subj_id) & (fp_data['sessid'] == sess_id)]

                # get region-agnostic information
                fp_data_dict[subj_id][sess_id]['trial_start_ts'] = sess_fp_data.iloc[0]['trial_start_timestamps']
                time_data = sess_fp_data.iloc[0]['time_data']
                fp_data_dict[subj_id][sess_id]['time'] = time_data['start'] + np.arange(time_data['length'])*time_data['dt']
                fp_data_dict[subj_id][sess_id]['dec_info'] = time_data['dec_info']

                # get signals by region
                regions = np.unique(sess_fp_data['region'])
                signals = {}
                comments = {}
                for region in regions:
                    signals[region] = sess_fp_data[sess_fp_data['region'] == region]['fp_data'].iat[0]
                    comments[region] = sess_fp_data[sess_fp_data['region'] == region]['comments']

                fp_data_dict[subj_id][sess_id]['raw_signals'] = signals
                fp_data_dict[subj_id][sess_id]['comments'] = comments

        return {'implant_info': implant_info, 'fp_data': fp_data_dict}


    def get_sess_fp_data(self, sess_ids, reload=False):
        '''
        Gets all fp data for the given session ids

        Parameters
        ----------
        sess_ids : list of session ids

        Returns
        -------
        A pandas table of fp data
        '''

        fp_ids = db_access.get_sess_fp_ids(sess_ids)
        return self.get_fp_data(utils.flatten(fp_ids), reload)


    def get_local_fp_data(self):
        '''
        Gets all fp data stored locally

        Returns
        -------
        A pandas table of fp data
        '''

        return self.get_fp_data(self.local_fp_data['unitid'])

    ## Abstract Properties and Methods ##

    @property
    @abstractmethod
    def protocol_name(self):
        ''' The name of the protocol for this particular local db '''
        pass

    @abstractmethod
    def _format_sess_data(self, sess_data):
        ''' Format the session data appropriately based on the particular protocol '''
        pass

    def _format_unit_data(self, unit_data):
        # convert timestamps from us to s
        unit_data['spike_timestamps'] = unit_data['spike_timestamps']/1e6
        unit_data['trial_start_timestamps'] = unit_data['trial_start_timestamps']/1e6
        return unit_data

    def _format_fp_data(self, fp_data):
        fp_data.rename(columns={'id': 'fpid'}, inplace=True)
        fp_data[['AP', 'ML', 'DV']] = fp_data[['AP', 'ML', 'DV']].astype(float)
        fp_data['side'] = fp_data['ML'].apply(lambda x: 'left' if x > 0 else 'right')
        return fp_data

    ## Private Infrastructure Methods ##

    def __load_local_data(self):
        self.__local_data_path = path.join(self.data_dir, 'local_data.pkl')

        reload = False
        if path.exists(self.__local_data_path):
            with open(self.__local_data_path, 'rb') as f:
                self.__local_data = pickle.load(f)

            reload = Counter(self.__local_data.keys()) != Counter(['units', 'fp_data', 'sessions'])
        else:
            reload = True

        if reload:
            self.__local_data = {'units': pd.DataFrame(columns=['unitid', 'subjid', 'sessid']),
                                 'fp_data': pd.DataFrame(columns=['fpid', 'subjid', 'sessid']),
                                 'sessions': pd.DataFrame(columns=['sessid', 'subjid', 'sessiondate'])}

            # check if any data is already persisted and recreate file
            path_search = self._get_sess_unit_path('*')
            files = glob.glob(path_search)
            for file in files:
                self.__update_local_units(pd.read_pickle(file), False)

            path_search = self._get_sess_fp_path('*')
            files = glob.glob(path_search)
            for file in files:
                self.__update_local_sessions(pd.read_pickle(file), False)

            path_search = self._get_sess_beh_path('*')
            files = glob.glob(path_search)
            for file in files:
                self.__update_local_sessions(pd.read_pickle(file), False)

            self.__save_local_data()


    def __save_local_data(self):
        if self._save_locally:
            utils.check_make_dir(self.__local_data_path)

            with open(self.__local_data_path, 'wb') as f:
                pickle.dump(self.__local_data, f)

    def __update_local_units(self, unit_data, save=True):
        self.__local_data['units'] = pd.concat([self.__local_data['units'], unit_data[['unitid', 'subjid', 'sessid']]],
                                               ignore_index=True).sort_values('unitid')
        if save:
            self.__save_local_data()

    def __update_local_fp_data(self, fp_data, save=True):
        self.__local_data['fp_data'] = pd.concat([self.__local_data['fp_data'], fp_data[['fpid', 'subjid', 'sessid']]],
                                                 ignore_index=True).sort_values('fpid')
        if save:
            self.__save_local_data()

    def __update_local_sessions(self, sess_data, save=True):
        self.__local_data['sessions'] = pd.concat([self.__local_data['sessions'], sess_data[['sessid', 'subjid', 'sessiondate']].drop_duplicates()],
                                                  ignore_index=True).sort_values('sessid')
        if save:
            self.__save_local_data()

    def _get_sess_unit_path(self, sess_id):
        return path.join(self.data_dir, 'units', 'unit_data_{0}.pkl'.format(str(sess_id)))

    def _get_sess_fp_path(self, sess_id):
        return path.join(self.data_dir, 'fp', 'fp_data_{0}.pkl'.format(str(sess_id)))

    def _get_sess_beh_path(self, sess_id):
        return path.join(self.data_dir, 'beh', 'sess_data_{0}.pkl'.format(str(sess_id)))
