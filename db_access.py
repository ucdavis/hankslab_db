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
from dateutil import parser

# %% Get Behavioral or Physiological Data

def get_session_data(session_ids):
    '''Gets all behavioral data for the given session ids'''

    if utils.is_scalar(session_ids):
        session_ids = [session_ids]

    if len(session_ids) > 1:
        print('Retrieving {0} sessions...'.format(len(session_ids)))

    start = time.perf_counter()

    db = _get_connector()
    cur = db.cursor(dictionary=True, buffered=True)

    id_str = ','.join([str(i) for i in session_ids])

    sess_query = ('select sessid, subjid, sessiondate, starttime, protocol, startstage, rigid '
                  'from beh.sessions where sessid in ({0}) order by sessid').format(id_str)

    trial_query = ('select sessid, trialtime, trialnum, data, parsed_events from beh.trials '
                   'where sessid in ({0}) order by sessid, trialnum')

    # get all session data
    cur.execute(sess_query)
    sess_rows = cur.fetchall()

    sess_data = []

    sess_start = time.perf_counter()
    for i, sess in enumerate(sess_rows):

        # fetch all trials for this session
        cur.execute(trial_query.format(sess['sessid']))
        trials = cur.fetchall()

        for trial in trials:
            # read out data stored in json
            trial['parsed_events'] = _parse_json(trial['parsed_events'])
            # remove data into its own dictionary
            trial_data = _parse_json(trial.pop('data'))
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

    cur.close()
    db.close()

    print('Retrieved {0} sessions in {1:.1f} s'.format(len(session_ids), time.perf_counter()-start))

    if len(sess_data) > 0:
        sess_data.sort_values(['sessid', 'trial'], inplace=True, ignore_index=True)

    return sess_data.infer_objects()


def get_unit_data(unit_ids):
    '''Gets all ephys data for the given unit ids'''

    if utils.is_scalar(unit_ids):
        unit_ids = [unit_ids]

    print('Retrieving {0} units...'.format(len(unit_ids)))
    start = time.perf_counter()

    db = _get_connector()
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
        db_data[i]['spike_timestamps'] = np.array(_parse_json(row['spike_timestamps']))
        db_data[i]['trial_start_timestamps'] = np.array(_parse_json(row['trial_start_timestamps']))
        db_data[i]['waveform'] = _parse_json(row['waveform'])

    # convert to data table
    unit_data = pd.DataFrame.from_dict(db_data)

    cur.close()
    db.close()

    print('Retrieved {0} units in {1:.1f} s'.format(len(unit_ids), time.perf_counter()-start))

    return unit_data.sort_values('unitid', ignore_index=True).infer_objects()


def get_fp_data(fp_ids):
    '''Gets all fiber photometry data for the given ids'''

    if utils.is_scalar(fp_ids):
        fp_ids = [fp_ids]

    print('Retrieving {0} fp recordings...'.format(len(fp_ids)))
    start = time.perf_counter()

    db = _get_connector()
    cur = db.cursor(dictionary=True, buffered=True)

    query = '''select a.id, a.subjid, a.sessid, a.trial_start_timestamps, a.time_data, a.fp_data, a.comments,
               b.region, b.AP, b.ML, b.DV, b.fiber_type from met.fp_data as a inner join met.fp_implants as b
               on a.implant_id=b.id where a.id in ({0})'''

    max_rows = 1  # number of rows to retrieve at once

    if len(fp_ids) < max_rows:
        cur.execute(query.format(','.join([str(i) for i in fp_ids])))
        db_data = cur.fetchall()
    else:
        n_iter = math.ceil(len(fp_ids)/max_rows)
        batch_start = time.perf_counter()
        for i in range(n_iter):

            # get batch of ids to load
            if i < n_iter:
                batch_ids = fp_ids[i*max_rows:(i+1)*max_rows]
            else:
                batch_ids = fp_ids[i*max_rows:]

            # load data
            cur.execute(query.format(','.join([str(i) for i in batch_ids])))
            rows = cur.fetchall()

            if i == 0:
                db_data = rows
            else:
                db_data = db_data + rows

            print('Retrieved {0}/{1} fp recordings in {2:.1f} s'.format(i*max_rows+cur.rowcount,
                  len(fp_ids), time.perf_counter()-batch_start))

    # read out data stored in json
    for i, row in enumerate(db_data):
        db_data[i]['trial_start_timestamps'] = np.array(_parse_json(row['trial_start_timestamps']))
        db_data[i]['time_data'] = _parse_json(row['time_data'])
        db_data[i]['fp_data'] = _parse_json(row['fp_data'])

        for key, signal in db_data[i]['fp_data'].items():
            db_data[i]['fp_data'][key] = np.array(signal)

    # convert to data table
    fp_data = pd.DataFrame.from_dict(db_data)
    
    # format implant information
    fp_data.rename(columns={'id': 'fpid'}, inplace=True)
    fp_data[['AP', 'ML', 'DV']] = fp_data[['AP', 'ML', 'DV']].astype(float)
    fp_data['side'] = fp_data['ML'].apply(lambda x: 'left' if x > 0 else 'right')

    cur.close()
    db.close()

    print('Retrieved {0} fp recordings in {1:.1f} s'.format(len(fp_ids), time.perf_counter()-start))

    return fp_data.sort_values('fpid', ignore_index=True).infer_objects()


# %% Get IDs

def get_unit_protocol_subj_ids(protocol):
    ''' Gets all subject ids with unit information for a particular protocol '''
    db = _get_connector()
    cur = db.cursor(buffered=True)

    cur.execute('select distinct a.subjid from beh.sessions a, met.units b where a.protocol=\'{0}\' and a.sessid=b.sessid'
                .format(protocol))
    ids = cur.fetchall()

    cur.close()
    db.close()

    # flatten list of tuples
    return sorted([i[0] for i in ids])


def get_subj_unit_ids(subj_ids):
    '''Gets all unit ids for the given subject ids. Returns a dictionary of unit ids indexed by subject id'''

    if utils.is_scalar(subj_ids):
        subj_ids = [subj_ids]

    db = _get_connector()
    cur = db.cursor(buffered=True, dictionary=True)

    cur.execute('select subjid, unitid from met.units where subjid in ({0})'
                .format(','.join([str(i) for i in subj_ids])))
    ids = cur.fetchall()

    cur.close()
    db.close()

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

    db = _get_connector()
    cur = db.cursor(buffered=True, dictionary=True)

    cur.execute('select sessid, unitid from met.units where sessid in ({0})'
                .format(','.join([str(i) for i in sess_ids])))
    ids = cur.fetchall()

    cur.close()
    db.close()

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

    db = _get_connector()
    cur = db.cursor(buffered=True, dictionary=True)

    cur.execute('select distinct sessid, subjid from met.units where subjid in ({0})'
                .format(','.join([str(i) for i in subj_ids])))
    ids = cur.fetchall()

    cur.close()
    db.close()

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

    db = _get_connector()
    cur = db.cursor(buffered=True, dictionary=True)

    cur.execute('select sessid, unitid from met.units where unitid in ({0})'
                .format(','.join([str(i) for i in unit_ids])))
    ids = cur.fetchall()

    cur.close()
    db.close()

    # group the unit ids by session
    # Note: this is much faster than repeatedly querying the database
    df = pd.DataFrame.from_dict(ids)
    # group unit ids into a sorted list by session id
    df = df.groupby('sessid').agg(list)['unitid'].apply(lambda x: sorted(x))

    return df.to_dict()


def get_fp_data_sess_ids(protocol=None, stage_num=None, subj_ids=None):
    '''Gets all session ids with fp data optionally filtering on protocol, stage number, and subject ids,
    Returns a dictionary of session ids indexed by subject id'''

    db = _get_connector()
    cur = db.cursor(buffered=True, dictionary=True)

    # first get session ids with fp data
    if subj_ids is None:
        cur.execute('select distinct subjid, sessid from met.fp_data')
    else:
        if utils.is_scalar(subj_ids):
            subj_ids = [subj_ids]

        cur.execute('select distinct subjid, sessid from met.fp_data where subjid in ({})'
                .format(','.join([str(i) for i in subj_ids])))

    ids = cur.fetchall()

    cur.close()
    db.close()

    if not protocol is None or not stage_num is None:
        sess_ids = [v['sessid'] for v in ids]
        sess_details = get_sess_protocol_stage(sess_ids)

        if not protocol is None:
            sess_details = sess_details[sess_details['protocol'] == protocol]

        if not stage_num is None:
            sess_details = sess_details[sess_details['startstage'] == stage_num]

        df = sess_details[['subjid', 'sessid']]

    else:
        # group the fp ids by session
        # Note: this is much faster than repeatedly querying the database
        df = pd.DataFrame.from_dict(ids)

    # group unit ids into a sorted list by session id
    df = df.groupby('subjid').agg(list)['sessid'].apply(lambda x: sorted(x))

    return df.to_dict()


def get_sess_fp_ids(sess_ids):
    '''Gets all fp ids for the given session ids.
    Returns a dictionary of fp ids indexed by session id'''

    if utils.is_scalar(sess_ids):
        sess_ids = [sess_ids]

    db = _get_connector()
    cur = db.cursor(buffered=True, dictionary=True)

    cur.execute('select id, sessid from met.fp_data where sessid in ({0}) and subjid in (select distinct subjid from beh.sessions where sessid in ({0}))'
                .format(','.join([str(i) for i in sess_ids])))
    ids = cur.fetchall()

    cur.close()
    db.close()

    # group the fp ids by session
    # Note: this is much faster than repeatedly querying the database
    df = pd.DataFrame.from_dict(ids)
    # group unit ids into a sorted list by session id
    df = df.groupby('sessid').agg(list)['id'].apply(lambda x: sorted(x))

    return df.to_dict()


def get_fp_sess_ids(fp_ids):
    '''Gets all session ids for the given fp ids.
    Returns a dictionary of fp ids indexed by session id'''

    if utils.is_scalar(fp_ids):
        fp_ids = [fp_ids]

    db = _get_connector()
    cur = db.cursor(buffered=True, dictionary=True)

    cur.execute('select id, sessid from met.fp_data where id in ({0})'
                .format(','.join([str(i) for i in fp_ids])))
    ids = cur.fetchall()

    cur.close()
    db.close()

    # group the fp ids by session
    # Note: this is much faster than repeatedly querying the database
    df = pd.DataFrame.from_dict(ids)
    # group fp ids into a sorted list by session id
    df = df.groupby('sessid').agg(list)['id'].apply(lambda x: sorted(x))

    return df.to_dict()


def get_fp_implant_info(subj_ids=None):
    '''Get fiber photometry implant information, optionally limited to the given subject ids.
    Returns a dictionary of implant information keyed by subject id'''

    db = _get_connector()


    if subj_ids is None:
        cur = db.cursor(buffered=True)
        cur.execute('select distinct subjid from met.fp_implants')
        subj_ids = utils.flatten(cur.fetchall())
        cur.close()
    elif utils.is_scalar(subj_ids):
        subj_ids = [subj_ids]

    cur = db.cursor(buffered=True, dictionary=True)
    cur.execute('select * from met.fp_implants where subjid in ({0})'
                .format(','.join([str(i) for i in subj_ids])))
    info = cur.fetchall()

    cur.close()
    db.close()

    # group the fp info by subject id
    # Note: this is much faster than repeatedly querying the database
    df = pd.DataFrame.from_dict(info)
    # format information and add a side column
    df[['AP', 'ML', 'DV']] = df[['AP', 'ML', 'DV']].astype(float)
    df['side'] = df['ML'].apply(lambda x: 'left' if x > 0 else 'right')
    # group fp info into a nested dictionary keyed by subject id and region
    subj_ids = np.unique(df['subjid'])

    return {subjid: df[df['subjid'] == subjid].drop_duplicates().set_index('region').to_dict('index')
                    for subjid in subj_ids}


def get_subj_sess_ids(subj_ids, stage_num=None, stage_name=None, protocol=None, date_start=None, date_end=None):
    '''Gets all session ids for the given subject ids, optionally filtering on a stage number or name, protocol,
    start date or end date. If no stage is provided, will pull only the last stage in the database.
    Returns a dictionary of session ids indexed by subject id'''

    if stage_name is not None and stage_num is not None:
        raise ValueError('Can only provide one form of stage identifier')

    if utils.is_scalar(subj_ids):
        subj_ids = [subj_ids]

    db = _get_connector()
    cur = db.cursor(buffered=True, dictionary=True)

    # get the current protocol for subjects, if not provided
    if protocol is None:
        cur.execute('select distinct protocol from met.current_settings where subjid in ({0})'
                    .format(','.join([str(i) for i in subj_ids])))
        protocol = cur.fetchall()
        protocol = list(protocol[0].values())

        if len(protocol) > 1:
            raise ValueError('Subjects are currently in different protocols. Specify a protocol or change the subject ids.')
        else:
            protocol = protocol[0]

    # get the current protocol for subjects, if not provided
    if stage_num is None:
        # if stage name is provided, convert to stage number for animals
        if stage_name is not None:
            cur.execute('''select distinct stage from met.settings where settingsname=\'{0}\' and protocol=\'{1}\'
                           and expgroupid in (select expgroupid from beh.sessions where protocol=\'{1}\' and subjid in ({2}))'''.format(
                           stage_name, protocol, ','.join([str(i) for i in subj_ids])))
            stage_num = cur.fetchall()
            stage_num = list(stage_num[0].values())

        # else get the current active stage for the protocol
        else:
            cur.execute('select distinct stage from met.current_settings where subjid in ({0}) and protocol=\'{1}\''
                        .format(','.join([str(i) for i in subj_ids]), protocol))
            stage_num = cur.fetchall()
            stage_num = list(stage_num[0].values())

        # make sure there is only one stage number
        if len(stage_num) > 1:
            raise ValueError('Subjects are currently in different stages. Specify a stage or change the subject ids.')
        else:
            stage_num = stage_num[0]

    if date_start is None:
        date_start = date(1900,1,1).isoformat()

    if date_end is None:
        date_end = date.today().isoformat()

    # get session and subject ids but filter out sessions without trials
    cur.execute('''select sessid, subjid from beh.sessions as a where
                startstage={0} and protocol=\'{1}\' and subjid in ({2}) and sessiondate >= \'{3}\' and sessiondate <= \'{4}\'
                and exists (select 1 from beh.trials as b where a.sessid=b.sessid)'''
                .format(str(stage_num), protocol, ','.join([str(i) for i in subj_ids]), date_start, date_end))
    ids = cur.fetchall()

    cur.close()
    db.close()

    # group the session ids by subject
    # Note: this is much faster than repeatedly querying the database
    df = pd.DataFrame.from_dict(ids)
    # group session ids into a sorted list by subject id
    df = df.groupby('subjid').agg(list)['sessid'].apply(lambda x: sorted(x))

    return df.to_dict()


def get_subj_sess_ids_by_date(subj_ids, date_str):
    '''Gets all session ids for the given subject ids, for the given date'''

    date_str = parser.parse(date_str).isoformat()

    db = _get_connector()
    cur = db.cursor(buffered=True, dictionary=True)

    # get session and subject ids but filter out sessions without trials
    cur.execute('''select sessid, subjid from beh.sessions as a where subjid in ({}) and
                sessiondate=\'{}\' and exists (select 1 from beh.trials as b where a.sessid=b.sessid)'''
                .format(','.join([str(i) for i in subj_ids]), date_str))
    ids = cur.fetchall()

    cur.close()
    db.close()

    # group the session ids by subject
    # Note: this is much faster than repeatedly querying the database
    df = pd.DataFrame.from_dict(ids)
    # group session ids into a sorted list by subject id
    df = df.groupby('subjid').agg(list)['sessid'].apply(lambda x: sorted(x))

    return df.to_dict()


def get_active_subj_stage(protocol=None, subj_ids=None, stage_num=None, stage_name=None):
    '''Gets all active subjects and their current protocol and stage.
    Can optionally filter results by protocol, subject ids, and stage'''

    if stage_name is not None and stage_num is not None:
        raise ValueError('Can only provide one form of stage identifier')

    db = _get_connector()

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
    # cur.execute('SELECT subjid, startstage, protocol FROM beh.sessions WHERE sessid IN (SELECT MAX(sessid) FROM beh.sessions GROUP BY subjid) AND subjid IN ({0}) ORDER BY subjid'
    #             .format(','.join([str(i) for i in active_rats])))
    cur.execute('SELECT subjid, protocol, settingsname, stage FROM met.current_settings WHERE subjid IN ({0}) ORDER BY subjid'
                .format(','.join([str(i) for i in active_rats])))
    data = cur.fetchall()

    cur.close()
    db.close()

    # format into a dataframe
    df = pd.DataFrame.from_dict(data).rename(columns={'startstage': 'stage'})

    # optionally filter based on protocol and stage
    if not stage_num is None:
        df = df[df['stage'] == stage_num]

    if not protocol is None:
        df = df[df['protocol'].str.fullmatch(protocol, case=False)]

    if not stage_name is None:
        df = df[df['settingsname'].str.fullmatch(stage_name, case=False)]

    return df


def get_protocol_subject_info(protocol, subj_ids=None, stage_num=None, stage_name=None):
    '''Gets all subjects who have data for the given protocol.
    Can optionally filter results by subject ids and stage'''

    db = _get_connector()

    # get all the data
    cur = db.cursor(buffered=True, dictionary=True)
    cur.execute('SELECT distinct a.subjid, b.protocol, b.settingsname, b.stage from met.subject_settings a '+
                'join met.settings b on a.settingsid = b.settingsid where protocol=\'{}\' ORDER BY subjid'
                .format(protocol))
    data = cur.fetchall()

    cur.close()
    db.close()

    # format into a dataframe
    df = pd.DataFrame.from_dict(data).rename(columns={'startstage': 'stage'})

    # filter out subject 0
    df = df[~df['subjid'].isin([0])]
    
    # optionally filter based on subject ids and stage
    if not subj_ids is None:
        df = df[df['subjid'].isin(subj_ids)]
    
    if not stage_num is None:
        df = df[df['stage'] == stage_num]

    if not stage_name is None:
        df = df[df['settingsname'].str.fullmatch(stage_name, case=False)]

    return df


def get_sess_protocol_stage(sess_ids):
    ''' Get the protocol name and stage number for all the given session ids'''

    db = _get_connector()
    cur = db.cursor(buffered=True, dictionary=True)

    # get session and subject ids but filter out sessions without trials
    cur.execute('''select sessid, subjid, protocol, startstage from beh.sessions as a where sessid in ({})
                and exists (select 1 from beh.trials as b where a.sessid=b.sessid)'''
                .format(','.join([str(i) for i in sess_ids])))
    data = cur.fetchall()

    cur.close()
    db.close()

    df = pd.DataFrame.from_dict(data)

    return df


# %% Add/Modify Data

def add_procedure(subj_id, description, implant_type, brain_regions):
    '''Add a procedure to the procedures table'''

    db = _get_connector()
    data = {'subjid': subj_id,
            'description': description,
            'implant_type': implant_type,
            'brain_regions_targeted': brain_regions}

    __insert(db, 'met.procedures', data)

    db.close()


def add_fp_implant(subj_id, region, fiber_type, AP, ML, DV, comments=None):
    '''Add a fiber photometry implant to the fp_implants table'''

    db = _get_connector()
    cur = db.cursor()

    cur.execute('select id from met.procedures where subjid={0}'.format(subj_id))
    procedure_id = cur.fetchone()

    if procedure_id is None:
        raise Exception('No procedures have been added for the given subject. Add a procedure for the subject before adding an implant')
    else:
        procedure_id = procedure_id[0]

    data = {'subjid': subj_id,
            'procedure_id': procedure_id,
            'region': region,
            'fiber_type': fiber_type,
            'AP': AP,
            'ML': ML,
            'DV': DV,
            'comments': comments}

    __insert(db, 'met.fp_implants', data, cur=cur)

    db.close()


def add_fp_data(subj_id, region, trial_start_ts, time_data, fp_data, sess_id=None, sess_date=None, comments=None):
    '''Add fiber photometry recording session data to the fp_data table'''

    db = _get_connector()
    cur = db.cursor()

    start = time.perf_counter()

    # get the appropriate implant
    cur.execute('select id from met.fp_implants where subjid={} and region=\'{}\''.format(subj_id, region))
    implant_id = cur.fetchone()

    if implant_id is None:
        raise Exception('No implants have been added for the given subject and region. Add an implant before adding data')
    else:
        implant_id = implant_id[0]

    # get the appropriate session
    if sess_id is not None and sess_date is not None:
        raise ValueError('Either specify the session id or the session date, not both')
    elif sess_id is None:
        # find the session based on the date
        if sess_date is None:
            sess_date = date.today().isoformat()

        cur.execute('select sessid from beh.sessions where subjid={} and sessiondate=\'{}\''.format(subj_id, sess_date))
        sess_id = cur.fetchall()

        if sess_id is None:
            raise Exception('No sessions were found for subject {} on {}. Either correct the date or pass in the session id instead'.format(subj_id, sess_date))
        elif len(sess_id) > 1:
            raise Exception('{} sessions were found for subject {} on {}. Pass in the session id instead'.format(len(sess_id), subj_id, sess_date))
        else:
            sess_id = sess_id[0]
    else:
        # make sure the given session id exists for the given subject
        cur.execute('select exists(select 1 from beh.sessions where subjid={} and sessid={})'.format(subj_id, sess_id))
        exists = bool(cur.fetchone()[0])
        if not exists:
            raise Exception('Session {} was not found for subject {}'.format(sess_id, subj_id))

    data = {'implant_id': implant_id,
            'subjid': subj_id,
            'sessid': sess_id,
            'trial_start_timestamps': _to_json(trial_start_ts),
            'time_data': _to_json(time_data),
            'fp_data': _to_json(fp_data),
            'comments': comments}

    # make sure we aren't adding duplicate sessions to the database
    cur.execute('select exists(select 1 from met.fp_data where implant_id={} and sessid={})'.format(implant_id, sess_id))
    exists = bool(cur.fetchone()[0])
    if exists:
        print('FP data for session {} and region {} was already added to the database. Will update instead'.format(sess_id, region))
        __update(db, 'met.fp_data', data, 'implant_id={} and sessid={}'.format(implant_id, sess_id), cur=cur)
    else:
        __insert(db, 'met.fp_data', data, cur=cur)

    print('Added FP data for subject {} in region {} to the database in {:.1f} s'.format(subj_id, region, time.perf_counter()-start))

    db.close()


# %% PRIVATE METHODS


def _get_connector():
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


def _parse_json(x):
    '''Private method to convert json to values'''
    tmp = json.loads(x.decode('utf-8'))
    #
    if 'vals' in tmp and 'info' in tmp:
        return tmp['vals']
    else:
        return tmp


def _to_json(x):
    '''Private method to convert values to json'''
    return json.dumps(x, cls=json_encoder).encode('utf-8')


# Extend the JSON Encoder class to serialize objects that are not base python
class json_encoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)


def __insert(db, tablename, data, cur=None, commit=True):
    '''Add a row to the given table with the given data.
       data is given as a dictionairy where the keys are the column names and the
       values are the data entries for those columns.
       Can optionally pass in a cursor and/or commit the insertion'''

    cols = data.keys()
    col_string = ', '.join(cols)
    val_string = ['%({})s'.format(c) for c in cols]
    val_string = ', '.join(val_string)
    sql = 'insert into {} ({}) values ({})'.format(tablename, col_string, val_string)

    if cur is None:
        cur = db.cursor()

    cur.execute(sql, data)

    if commit:
        db.commit()
        cur.close()
    else:
        return cur


def __update(db, tablename, data, where, cur=None, commit=True):
    '''Updates a row in the given table with the given data.
       data is given as a dictionairy where the keys are the column names and the
       values are the data entries for those columns.
       Can optionally pass in a cursor and/or commit the insertion'''

    set_string = ['{0}=%({0})s'.format(c) for c in data.keys()]
    set_string = ', '.join(set_string)

    sql = 'update {} set {} where {}'.format(tablename, set_string, where)

    if cur is None:
        cur = db.cursor()

    cur.execute(sql, data)

    if commit:
        db.commit()
        cur.close()
    else:
        return cur
