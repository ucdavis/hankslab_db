# -*- coding: utf-8 -*-
"""
Creates a GUI to handle packaging doric data

@author: tanne
"""

from pyutils import utils
from sys_neuro_tools import doric_utils as dor
from sys_neuro_tools import acq_utils as acq
from hankslab_db import db_access
import tkinter as tk
from tkinter import filedialog


def package_doric_data(subj_id, sess_id, region_dict, wavelength_dict, comments_dict=None, data_path=None,
                       target_dt=0.005, new_format=True, print_file_struct=True, print_attr=False, initial_dir=None):

    print('Packaging data for subject {}...'.format(subj_id))
    
    if data_path is None:
        if initial_dir is None:
            initial_dir = utils.get_user_home()

        win = tk.Tk()
        win.withdraw()
        win.wm_attributes('-topmost', 1)
        data_path = filedialog.askopenfilename(initialdir = initial_dir,
                                              title = 'Select a Recording File',
                                              filetypes = [('Doric files', '*.doric')])

    if print_file_struct:
        dor.h5print(data_path, print_attr=print_attr)

    dor_signal_path = '/DataAcquisition/FPConsole/Signals/Series0001/'
    ttl_name = 'ttl'

    signal_name_dict = {ttl_name: {'time': 'DigitalIO/Time', 'values': 'DigitalIO/DIO01'}}
    if new_format:
        signal_name_dict.update({'{}_{}'.format(r, w):
                                 {'time': 'LockInAOUT0{}/Time'.format(wavelength_dict[w]),
                                  'values': 'LockInAOUT0{}/AIN0{}'.format(wavelength_dict[w], region_dict[r])}
                                 for r in region_dict.keys() for w in wavelength_dict.keys()})
    else:
        signal_name_dict.update({'{}_{}'.format(r, w):
                                 {'time': 'AIN0{}xAOUT0{}-LockIn/Time'.format(region_dict[r], wavelength_dict[w]),
                                  'values': 'AIN0{}xAOUT0{}-LockIn/Values'.format(region_dict[r], wavelength_dict[w])}
                                 for r in region_dict.keys() for w in wavelength_dict.keys()})


    data = dor.get_specific_data(data_path, dor_signal_path, signal_name_dict)

    data, issues = dor.fill_missing_data(data, 'time')
    if len(issues) > 0:
        print('Issues found:\n{0}'.format('\n'.join(issues)))

    # get trial start timestamps
    trial_start_ts, trial_nums = acq.parse_trial_times(data[ttl_name]['values'], data[ttl_name]['time'])

    # check if there are any missing trial numbers
    trial_num_diffs = trial_nums[1:] - trial_nums[:-1]
    if any(trial_num_diffs > 1):
        print('Session is missing trials, will attempt to fill them in from behavior data')
        # TODO - fill in missing trial numbers and times

    signal_data = {k:v for k,v in data.items() if k != ttl_name}
    dec_time, dec_signals, dec_info = acq.decimate_data(signal_data, target_dt = target_dt)

    # no need to persist the entire timestamp array, just the elements needed to recompute
    time_data = {'start': dec_time[0], 'end': dec_time[-1], 'dt': dec_info['decimated_dt'], 'length': len(dec_time), 'dec_info': dec_info}

    if comments_dict is None:
        comments_dict = {r: '' for r in region_dict.keys()}

    for region in region_dict.keys():
        # get all signals associated with each region
        region_keys = [k for k in dec_signals.keys() if region in k]
        fp_data = {k.replace(region+'_', ''): dec_signals[k] for k in region_keys}
        db_access.add_fp_data(subj_id, region, trial_start_ts, time_data, fp_data, sess_id=sess_id, comments=comments_dict[region])
