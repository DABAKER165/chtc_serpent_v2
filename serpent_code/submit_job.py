#!/usr/bin/env python3

from argparse import ArgumentParser
# from mods.get_job_status import job_status
# from mods.record_submissions import (get_sample_list_from_record,
#                                      record_as_completed,
#                                      filter_record_by_kvp)
# from mods.chtc_submission_script import (chtc_create_and_run_submit_file)

from mods.utils import (keys_exists)

from mods.open_configuration import (open_and_sort_configurations)

import os
import subprocess
import time
from pathlib import Path
from datetime import datetime


def parse_process_arrays_args(parser: ArgumentParser):
    """
        adds the arguments to the
    """
    parser.add_argument('--compiled_config', metavar='compiled_config',
                        help='Directory of each submission configuration', required=True)
    parser.add_argument('--server_name', metavar='server_name',
                        help='server_name', required=True)
    parser.add_argument('--timeout_hours', metavar='timeout_hours',
                        help='how long to run the python', required=False, default=120)
    return


def get_process_arrays_args():
    """	Inputs arguments from bash
    Gets the arguments, checks requirements, returns a dictionary of arguments
    Return: args - Arguments as a dictionary
    """
    parser = ArgumentParser()
    parse_process_arrays_args(parser)
    return parser.parse_args()


def record_json_to_dict(record_submission_json):
    record_dict = {}
    print(record_submission_json)
    return record_dict


args = get_process_arrays_args()
# Get the compiled config complete dir
# Get the servername so it knows what parts to look at.
compiled_config_dir = args.compiled_config
local_server_name = args.server_name
# this path cannot be stored in a config as it needs the arg to know where to look
loop_timer = 0

print(compiled_config_dir)
while loop_timer < 1:
    # this path cannot be stored in a config as it needs the arg to know where to look
    # os.makedirs(chtc_serpent_submissions, exist_ok=True)
    os.makedirs(compiled_config_dir, exist_ok=True)
    config_list = os.listdir(compiled_config_dir)
    config_list = [os.path.join(compiled_config_dir, x) for x in config_list if x[-5:] == ".json"]
    print(config_list)
    # THere is three ways to stop the run: Stop the Python from looping, Delete the config file,
    # add a process complete file
    sc_list = open_and_sort_configurations(config_filepaths=config_list,
                                           compiled_config=True)
    for sc in sc_list:
        sc.mk_local_directories()

        print('All mods:', sc.order_mod)
        remote_order = [x for x in sc.order_mod if x in sc.config.keys()]
        print('Remote mods:', remote_order)
        for mod_i in remote_order:
            ####################################################################
            #  Make sure the module/server combo executes in the remote server #
            ####################################################################
            if mod_i in ['all', 'priority']:
                continue
            print('{0} -- {1}'.format(sc.get_submit_name(), mod_i))
            server_i = sc.get_executable_server_name(mod_i)
            if server_i is None:
                print('missing executable, skipping module')
                continue
            if server_i != local_server_name:
                continue
            ########################################################################
            #  Declare Module Filepaths                                            #
            #  These are hard coded because it is easir than passing files around  #
            ########################################################################
            incoming_ready, incoming_complete = sc.check_incoming_status(mod_i, server_i, calling_server=server_i)
            print('Mod: {0} ; Incoming ready: {1} ; Incoming complete: {2}'.format(mod_i,
                                                                                   incoming_ready,
                                                                                   incoming_complete))
            #################################################################################
            #  Skip the loop if the module completed based on if the completed file exists  #
            #################################################################################
            # make sure the workflow did not already complete
            if os.path.exists(sc.get_unique_path(mod_i, server_i, 'exe_complete_file_path')):
                print('{0} -- {1} has already completed!'.format(mod_i, server_i))
                continue
            # To avoid processing partially transfered files look for the transfer lock.
            # Keep trying if transfer is locked, Sleep for 10 seconds as to not bombard the server
            if os.path.exists(sc.get_unique_path(mod_i, server_i, 'transfer_lock_file_path')):
                print('Transfer is locked!')
                print('If error, remove from remote path: {0}'.format(sc.get_unique_path(mod_i,
                                                                                         server_i,
                                                                                         'transfer_lock_file_path')))
                time.sleep(10)
                continue
            # make sure the incoming is not ready
            if not incoming_ready:
                print('incoming not ready')
                continue
            ######################################################################
            #  Execute a script to aggregate files on remote server side         #
            #  These should not be labour intensive:                             #
            #  - Examples: file copying, restructuring paths, and compressing    #
            ######################################################################

            # figure out if the uploading sample files are relevant and have been uploaded
            samples_uploaded = True
            if keys_exists(sc.config, [mod_i, server_i, 'transfer_to_server'])[0]:
                if not os.path.exists(sc.get_unique_path(mod_i, server_i, 'samples_uploaded_path')):
                    samples_uploaded = False
            # figure out if the uplaoding static files are relevant and have been uploaded
            static_uploaded = True
            if keys_exists(sc.config, [mod_i, server_i, 'static_files'])[0]:
                if not os.path.exists(sc.get_unique_path(mod_i, server_i, 'static_uploaded_path')):
                    static_uploaded = False
            # unlike samples_uploaded, static files are required before a run can start
            # Subset of sample files can be uploaded and still run

            if not static_uploaded:
                print('missing static files skipping until the transfer is completed from local')
                continue
            #######################################################################################
            #  LAUNCH A LOCAL JOB from remote server                                              #
            #                                                                                     #
            #  submit_job is chtc condor specific flag.                                           #
            #  If submit_job is declared as TRUE it will submit the jobe to CHTC                  #
            #  This means all the submit files and wrapper will be dynamically created.           #
            #  The default for submit_job FALSE which is not to create a submit file and wrapper  #
            #  chtc in the server_i is for log file generation since they cause issues in chtc #
            #######################################################################################
            if not incoming_ready:
                print('incoming not ready: {0} - {1}'.format(server_i, mod_i))
                continue
            if not keys_exists(sc.config, [mod_i, server_i, 'submit_job']) or (
                    keys_exists(sc.config, [mod_i, server_i, 'submit_job'])
                    and (sc.config[mod_i][server_i]['submit_job'][0].upper() == 'F')):
                # print(mod_i, "submitjob")
                sample_list_path = sc.make_sample_list_wrapper(mod_i, server_i)
                exe_cmd = sc.make_remote_executable_command(mod=mod_i,
                                                            server=server_i,
                                                            incoming_ready=incoming_ready,
                                                            incoming_complete=incoming_complete,
                                                            sample_list_path=sample_list_path)

                print('Local sh script: {0}'.format(exe_cmd))
                # if chtc_mod_config['executable'][-3:] == '.sh':
                #     subprocess.run(['chmod +x {0}'.format(server_paths['mod_executable_path'])], shell=True)
                if server_i == 'chtc':
                    subprocess.run([exe_cmd], shell=True)
                else:
                    module_out_dir = sc.get_unique_path(mod_i, server_i, 'module_out_dir')
                    with open(os.path.join(module_out_dir, "output.log"), "a") as output:
                        subprocess.call(exe_cmd,
                                        shell=True,
                                        stdout=output,
                                        stderr=output)
                ############################################################
                #  Check for status files need creating                    #
                #  If there is no status directory:                        #
                #  Then this script is ran once and does not loop          #
                #  Then create the ready and complete files automatically  #
                ############################################################
                # probably need to be smarter about this, there are better ways to do this.
                if (not keys_exists(sc.config, [mod_i, server_i, 'arguments'])[0]) or (
                        not keys_exists(sc.config, [mod_i, server_i, 'arguments', '--status_dir'])[0]):
                    print('No Status Dir creating touches')
                    Path(sc.get_unique_path(mod_i, server_i, 'exe_ready_file_path')).touch()
                    if samples_uploaded and incoming_complete:
                        Path(sc.get_unique_path(mod_i, server_i, 'exe_complete_file_path')).touch()
                if (sample_list_path is not None) and (os.path.isfile(sample_list_path)):
                    Path(sc.get_unique_path(mod_i, server_i, 'submitted_sample_list_path')).touch()
                    with open(sc.get_unique_path(mod_i, server_i, 'submitted_sample_list_path'), "a") as submitted:
                        with open(sample_list_path, "r") as samples:
                            for line in samples:
                                line = line.strip()
                                submitted.write('\n{0}'.format(line))
                # this continue prevents from submitting a CHTC job.
                continue

            #########################################################################
            #  Execute a submission on CHTC                                         #
            #  - File aggregation before and after should be done in other modules  #
            #########################################################################

            # relies on error handling if it hasn't been submitted yet.
            print('get submitted jobs')
            submitted_sample_list, completed_sample_list = sc.get_submitted_completed_sample_lists(mod=mod_i,
                                                                                                   server=server_i)
            print('completed sample lists and submitted sample lists')
            submitted_sample_list = set(submitted_sample_list) | set(completed_sample_list)
            sc.chtc_create_and_run_submit_file(mod=mod_i,
                                               server=server_i,
                                               submitted_sample_list=submitted_sample_list)
            completed_json, held_samples = sc.completed_to_json(mod_i, server_i)
            ignore_held = True
            # This comes from the local side to create after files have uploaded.
            # This can also come from the local side at creation, because it is a DAG from another CHTC job.
            # Earlier samples uploaded and incoming complete will be set to true if they are not in the config
            if samples_uploaded and incoming_complete:
                # make sure all the smaples have completed.
                # missing due to held is just skipped for now and needs manual resubmission of some sort.
                # a simple move command and relaunch may be instore.
                submitted_sample_list, completed_sample_list = sc.get_submitted_completed_sample_lists(mod=mod_i,
                                                                                                       server=server_i)
                if len(submitted_sample_list) < 1:
                    print('Warning: no samples were submitted for processing in chtc (in a prior loop).')
                    continue
                # get flag from chtc process
                missing_samples = set(submitted_sample_list) - set(completed_sample_list)
                if len(missing_samples) < 1:
                    if (not ignore_held) and (len(held_samples) > 0):
                        print("SOME NODES WERE HELD! server: {0} -- mod: {1}".format(mod_i, server_i))
                        print(held_samples)
                    Path(sc.get_unique_path(mod_i, server_i, 'exe_complete_file_path')).touch()
    now = datetime.now()

    dt_string = now.strftime("%Y_%m_%d__%H_%M_%S")
    print('sleeping for 2 minutes - {0}'.format(dt_string))
    time.sleep(120)
