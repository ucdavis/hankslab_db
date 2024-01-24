# -*- coding: utf-8 -*-
"""
Data access layer for hanks lab database

@author: tanner stevenson
"""

import mysql.connector
import os.path as path
import pandas as pd
import numpy as np
import numbers
import json
import time
import pyutils.utils as utils
import math
from datetime import date

## Unit and Session Data ##


def get_unit_data(unit_ids):
    '''Gets all ephys data for the given unit ids'''

    if utils.is_scalar(unit_ids):
        unit_ids = [unit_ids]

    print('Retrieving {0} units...'.format(len(unit_ids)))
    start = time.perf_counter()

    db = __get_connector()
    cur = db.cursor(dictionary=True, buffered=True)

    query = 'select * from met.units where unitid in ({0})'

    max_rows = 500  # number of rows to retrieve at once

    if len(unit_ids) < max_rows:
        cur.execute(query.format(','.join([str(i) for i in unit_ids])))
        db_data = cur.fetchall()
    else:
        n_iter = math.ceil(len(unit_ids)/max_rows)
        batch_start = time.perf_counter()
        for i in range(n_iter):

            # get batch of unit ids to load
            if i < n_iter:
                batch_ids = unit_ids[i*max_rows:(i+1)*max_rows]
            else:
                batch_ids = unit_ids[i*max_rows:]

            # load data
            cur.execute(query.format(','.join([str(i) for i in batch_ids])))
            rows = cur.fetchall()

            if i == 0:
                db_data = rows
            else:
                db_data = db_data + rows

            print('Retrieved {0}/{1} units in {2:.1f} s'.format(i*max_rows+cur.rowcount,
                  len(unit_ids), time.perf_counter()-batch_start))

    # read out data stored in json
    for i, row in enumerate(db_data):
        db_data[i]['spike_timestamps'] = np.array(__parse_json(row['spike_timestamps']))
        db_data[i]['trial_start_timestamps'] = np.array(__parse_json(row['trial_start_timestamps']))
        db_data[i]['waveform'] = __parse_json(row['waveform'])

    # convert to data table
    unit_data = pd.DataFrame.from_dict(db_data)

    db.close()

    print('Retrieved {0} units in {1:.1f} s'.format(len(unit_ids), time.perf_counter()-start))

    return unit_data.sort_values('unitid')


def get_session_data(session_ids):
    '''Gets all behavioral data for the given session ids'''

    if utils.is_scalar(session_ids):
        session_ids = [session_ids]

    if len(session_ids) > 1:
        print('Retrieving {0} sessions...'.format(len(session_ids)))

    start = time.perf_counter()

    db = __get_connector()
    cur_sess = db.cursor(dictionary=True, buffered=True)
    cur_trial = db.cursor(dictionary=True, buffered=True)

    id_str = ','.join([str(i) for i in session_ids])

    sess_query = ('select sessid, subjid, sessiondate, starttime, protocol, startstage, rigid '
                  'from beh.sessions where sessid in ({0}) order by sessid').format(id_str)

    trial_query = ('select sessid, trialtime, trialnum, data, parsed_events from beh.trials '
                   'where sessid in ({0}) order by sessid, trialnum')

    # get all session data
    cur_sess.execute(sess_query)
    sess_rows = cur_sess.fetchall()

    sess_data = []

    sess_start = time.perf_counter()
    for i, sess in enumerate(sess_rows):

        # fetch all trials for this session
        cur_trial.execute(trial_query.format(sess['sessid']))
        trials = cur_trial.fetchall()

        for trial in trials:
            # read out data stored in json
            trial['parsed_events'] = __parse_json(trial['parsed_events'])
            # remove data into its own dictionary
            trial_data = __parse_json(trial.pop('data'))
            trial_data.pop('n_done_trials')  # this is redundant

            # preload keys and values so we can edit the dictionary in the for loop
            key_val_list = list(trial_data.items())
            for key, value in key_val_list:
                # convert all lists of numbers to numpy arrays
                if not utils.is_scalar(value) and not len(value) == 0 and isinstance(value[0], numbers.Number):
                    trial_data[key] = np.array(value)
                # flatten any dictionary entries in trial data
                if utils.is_dict(value):
                    trial_data.update(**value)
                    # remove the original dictionary entry
                    trial_data.pop(key)

            # merge all dictionaries into single row
            sess_data.append({**sess, **trial, **trial_data})

        if (i % 5 == 0 or i == len(sess_rows)-1) and not i == 0:
            print('Retrieved {0}/{1} sessions in {2:.1f} s'.format(i +
                  1, len(session_ids), time.perf_counter()-sess_start))

    sess_data = pd.DataFrame.from_dict(sess_data)
    sess_data.rename(columns={'trialnum': 'trial'}, inplace=True)

    db.close()

    print('Retrieved {0} sessions in {1:.1f} s'.format(len(session_ids), time.perf_counter()-start))

    if len(sess_data) > 0:
        sess_data.sort_values(['sessid', 'trial'], inplace=True, ignore_index=True)

    return sess_data


## Unit and Session IDs ##

def get_unit_protocol_subj_ids(protocol):
    ''' Gets all subject ids with unit information for a particular protocol '''
    db = __get_connector()
    cur = db.cursor(buffered=True)

    cur.execute('select distinct a.subjid from beh.sessions a, met.units b where protocol=\'{0}\' and a.sessid=b.sessid'
                .format(protocol))
    ids = cur.fetchall()

    # flatten list of tuples
    return sorted([i[0] for i in ids])


def get_subj_unit_ids(subj_ids):
    '''Gets all unit ids for the given subject ids. Returns a dictionary of unit ids indexed by subject id'''

    if utils.is_scalar(subj_ids):
        subj_ids = [subj_ids]

    db = __get_connector()
    cur = db.cursor(buffered=True, dictionary=True)

    cur.execute('select subjid, unitid from met.units where subjid in ({0})'
                .format(','.join([str(i) for i in subj_ids])))
    ids = cur.fetchall()

    # group the unit ids by subject
    # Note: this is much faster than repeatedly querying the database
    df = pd.DataFrame.from_dict(ids)
    # group unit ids into a sorted list by subject id
    df = df.groupby('subjid').agg(list)['unitid'].apply(lambda x: sorted(x))

    return df.to_dict()


def get_sess_unit_ids(sess_ids):
    '''Gets all unit ids for the given session ids.
    Returns a dictionary of unit ids indexed by session id'''

    if utils.is_scalar(sess_ids):
        sess_ids = [sess_ids]

    db = __get_connector()
    cur = db.cursor(buffered=True, dictionary=True)

    cur.execute('select sessid, unitid from met.units where sessid in ({0})'
                .format(','.join([str(i) for i in sess_ids])))
    ids = cur.fetchall()

    # group the unit ids by session
    # Note: this is much faster than repeatedly querying the database
    df = pd.DataFrame.from_dict(ids)
    # group unit ids into a sorted list by session id
    df = df.groupby('sessid').agg(list)['unitid'].apply(lambda x: sorted(x))

    return df.to_dict()


def get_subj_unit_sess_ids(subj_ids):
    '''Gets all session ids that have unit data for the given subject ids.
    Returns a dictionary of session ids indexed by subject id'''

    if utils.is_scalar(subj_ids):
        subj_ids = [subj_ids]

    db = __get_connector()
    cur = db.cursor(buffered=True, dictionary=True)

    cur.execute('select distinct sessid, subjid from met.units where subjid in ({0})'
                .format(','.join([str(i) for i in subj_ids])))
    ids = cur.fetchall()

    # group the session ids by subject
    # Note: this is much faster than repeatedly querying the database
    df = pd.DataFrame.from_dict(ids)
    # group session ids into a sorted list by subject id
    df = df.groupby('subjid').agg(list)['sessid'].apply(lambda x: sorted(x))

    return df.to_dict()


def get_unit_sess_ids(unit_ids):
    '''Gets all session ids for the given unit ids.
    Returns a dictionary of unit ids indexed by session id'''

    if utils.is_scalar(unit_ids):
        unit_ids = [unit_ids]

    db = __get_connector()
    cur = db.cursor(buffered=True, dictionary=True)

    cur.execute('select sessid, unitid from met.units where unitid in ({0})'
                .format(','.join([str(i) for i in unit_ids])))
    ids = cur.fetchall()

    # group the unit ids by session
    # Note: this is much faster than repeatedly querying the database
    df = pd.DataFrame.from_dict(ids)
    # group unit ids into a sorted list by session id
    df = df.groupby('sessid').agg(list)['unitid'].apply(lambda x: sorted(x))

    return df.to_dict()


def get_subj_sess_ids(subj_ids, stage=None, date_start=None, date_end=None):
    '''Gets all session ids for the given subject ids, optionally filtering on a stage number,
    start date or end date. If no stage is provided, will pull only the last stage in the database.
    Returns a dictionary of session ids indexed by subject id'''

    if utils.is_scalar(subj_ids):
        subj_ids = [subj_ids]

    db = __get_connector()
    cur = db.cursor(buffered=True, dictionary=True)

    if stage is None:
        cur.execute('select max(startstage) from beh.sessions where subjid in ({0})'
                    .format(','.join([str(i) for i in subj_ids])))
        stage = cur.fetchall()
        stage = list(stage[0].values())[0]

    if date_start is None:
        date_start = date(1900,1,1).isoformat()

    if date_end is None:
        date_end = date.today().isoformat()

    # get session and subject ids but filter out sessions without trials
    cur.execute('''select sessid, subjid from beh.sessions as a where
                startstage={0} and subjid in ({1}) and sessiondate >= \'{2}\' and sessiondate <= \'{3}\'
                and exists (select 1 from beh.trials as b where a.sessid=b.sessid)'''
                .format(str(stage), ','.join([str(i) for i in subj_ids]), date_start, date_end))
    ids = cur.fetchall()

    # group the session ids by subject
    # Note: this is much faster than repeatedly querying the database
    df = pd.DataFrame.from_dict(ids)
    # group session ids into a sorted list by subject id
    df = df.groupby('subjid').agg(list)['sessid'].apply(lambda x: sorted(x))

    return df.to_dict()


def get_active_subj_stage(protocol=None, subj_ids=None, stage=None):
    '''Gets all active subjects and their current protocol and stage.
    Can optionally filter results by protocol, subject ids, and stage'''

    db = __get_connector()

    # first get all active subjects
    cur = db.cursor(buffered=True)
    cur.execute('select subjid from met.animals where not status = \'dead\'')
    active_rats = cur.fetchall()
    active_rats = np.array([i[0] for i in active_rats])

    # optionally filter subjects
    if not subj_ids is None:
        active_rats = active_rats[[subj in subj_ids for subj in active_rats]]

    # get all the data
    cur = db.cursor(buffered=True, dictionary=True)
    cur.execute('SELECT subjid, startstage, protocol FROM beh.sessions WHERE sessid IN (SELECT MAX(sessid) FROM beh.sessions GROUP BY subjid) AND subjid IN ({0}) ORDER BY subjid'
                .format(','.join([str(i) for i in active_rats])))
    data = cur.fetchall()

    # format into a dataframe
    df = pd.DataFrame.from_dict(data).rename(columns={'startstage': 'stage'})

    # optionally filter based on protocol and stage
    if not stage is None:
        df = df[df['stage'] == stage]

    if not protocol is None:
        df = df[df['protocol'].str.fullmatch(protocol, case=False)]

    return df


## PRIVATE METHODS ##


def __get_connector():
    '''Private method to get the database connection'''

    config_path = path.join(path.expanduser('~'), '.dbconf')
    conn_info = {}

    if path.exists(config_path):
        # read db connection information from file
        with open(config_path) as f:
            for line in f:
                if '=' in line:
                    prop_val = line.split('=')
                    conn_info[prop_val[0].strip()] = prop_val[1].strip()

        try:
            con = mysql.connector.connect(
                host=conn_info['host'],
                user=conn_info['user'],
                password=conn_info['passwd'])

        except BaseException as e:
            print('Could not connect to the database: {0}'.format(e.msg))
            raise
    else:
        # create a new db config
        # get values from user
        val = ''
        while val == '':
            val = input('Enter database host address: ')
        conn_info['host'] = val

        val = ''
        while val == '':
            val = input('Enter username: ')
        conn_info['user'] = val

        val = ''
        while val == '':
            val = input('Enter password: ')
        conn_info['passwd'] = val

        # try connecting before saving file
        try:
            con = mysql.connector.connect(
                host=conn_info['host'],
                user=conn_info['user'],
                password=conn_info['passwd'])

        except BaseException as e:
            print('Could not connect to the database: {0}'.format(e.msg))
            raise

        # write file
        with open(config_path, 'w') as config:
            config.write('[client]\n')
            for name, val in conn_info.items():
                config.write('{0} = {1}\n'.format(name, val))

    return con


def __parse_json(x):
    '''Private method to convert json to values'''
    return json.loads(x.decode('utf-8'))['vals']
