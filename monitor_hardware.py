#!/usr/bin/env python3
from argparse import ArgumentParser

from datetime import datetime

from serpent_code.mods.utils import (set_from_dict_list,
                                     read_json,
                                     rsync_files,
                                     run_ssh_cmd,
                                     get_configuration_filepaths)

from serpent_code.mods.open_configuration import (open_and_sort_configurations)


def parse_process_arrays_args(parser: ArgumentParser):
    """Parses the python script arguments from bash and makes sure files/inputs are valid"""
    parser.add_argument('--config_dir', metavar='config_dir',
                        help='Directory of each submission configuration', required=True)
    parser.add_argument('--submission_dir', metavar='submission_dir',
                        help='Directory of each submission status', required=True)
    parser.add_argument('--alert_config', metavar='alert_config',
                        help='Directory of each submission status', required=True)
    parser.add_argument('--monitoring_dir', metavar='monitoring_dir',
                        help='Directory of each submission status', required=True)


def get_process_arrays_args():
    """	Inputs arguments from bash
    Gets the arguments, checks requirements, returns a dictionary of arguments
    Return: args - Arguments as a dictionary
    """
    parser = ArgumentParser()
    parse_process_arrays_args(parser)
    args = parser.parse_args()
    return args


def import_from(module, name):
    module = __import__(module, fromlist=[name])
    return getattr(module, name)


def run_main(args):
    import pathlib
    import subprocess
    import time
    import os
    import shutil

    main_script_path = os.path.abspath(__file__)
    main_script_dir = os.path.dirname(main_script_path)
    print(main_script_dir)
    main_loop = True
    if not os.path.isdir(args.submission_dir):
        os.makedirs(args.submission_dir)
    alert_config = args.alert_config
    alert_json = read_json(alert_config)
    monitoring_dir = args.monitoring_dir
    # make sure the program directory exists
    # Only retransfer pipeline specific files (modules code) as needed
    # rsync will only transfer as needed but the connections are excessive?

    while main_loop:

        ###########################################################################################
        #  Get the configuration files that are in the argument declared configuration directory  #
        #  This will pull all files that end with json in teh default or submission directories   #
        #  It will not look in sub directories.                                                   #
        ###########################################################################################
        config_filepaths = get_configuration_filepaths(submission_config_dir=args.config_dir)

        if len(config_filepaths) < 1:
            print("Add a config file! sleeping for 2 minutes")
            time.sleep(120)
            continue
        ######################################################################################
        #  Begin looping through the submissions                                             #
        ######################################################################################
        config_list = open_and_sort_configurations(config_filepaths, main_script_dir)

        if len(config_list) < 1:
            print('No valid configs, sleep for 2 minutes. See prior messages for details')
            time.sleep(120)
            continue

        config_files_cleared = False
        for sc in config_list:
            print(sc.submit_name)
            server_mod_kvp = sc.remote_server_module_kvp()

            for server, mod_list in server_mod_kvp.items():
                remote_list = []
                for mod in mod_list:
                    remote_list.append(sc.get_un_server_path(mod, server, 'home_dir', return_dict=True))
                remote_dict_list = set_from_dict_list(remote_list)
                # remote_list = []
                # for dict_i in remote_dict_list:
                #     for key, value in dict_i.items():
                #         remote_list.append(value)
                # remote_dict_list = set_from_dict_list(remote_list)
                # print(remote_dict_list)
                # get the remaining space

                for remote_dict in remote_dict_list:
                    for drive, value_dict in remote_dict.items():
                        cmd = "df -Phk {0} | awk 'NR==2 {{print \\$4}}'".format(value_dict['path'])
                        ssh_out = run_ssh_cmd(cmd=cmd,
                                              un=value_dict['un'],
                                              server=value_dict['server'])
                        print(server, drive, value_dict['un'], value_dict['server'])
                        print(ssh_out.stderr.decode('utf-8').strip())
                        print(ssh_out.stdout.decode('utf-8').strip())
                        print(alert_json[server][drive]['disk_space'])
                        local_test_file_path = os.path.join(monitoring_dir, 'test_filezxcv.txt')
                        pathlib.Path(local_test_file_path).touch()
                        cmd = 'rm -f {0}'.format(os.path.join(value_dict['path'],
                                                              'test_filezxcv.txt'))
                        ssh_out = run_ssh_cmd(cmd=cmd,
                                              un=value_dict['un'],
                                              server=value_dict['server'])
                        print(server, drive, value_dict['un'], value_dict['server'])
                        print(ssh_out.stderr.decode('utf-8').strip())
                        print(ssh_out.stdout.decode('utf-8').strip())
                        rsync_files(source=local_test_file_path,
                                    un=value_dict['un'],
                                    server=value_dict['server'],
                                    dest=value_dict['path'])
                        checksum_result = rsync_files(source='{0}/'.format(local_test_file_path),
                                                      un=value_dict['un'],
                                                      server=value_dict['server'],
                                                      dest=value_dict['path'],
                                                      server_is_dest=True,
                                                      rsync_flag='-aP',
                                                      source_from_file_list=False,
                                                      cwd=None,
                                                      checksum_only=True)
                        checksum_result_stdout = checksum_result.stdout.decode('utf-8').strip()
                        if checksum_result_stdout == "0":
                            print('rsync success')
                        else:
                            print('rsync failed')
                            print(checksum_result.stderr.decode('utf-8').strip())
                            print(checksum_result.stdout.decode('utf-8').strip())

                # get each server homedir
                # Get free space for each including the local
                # Notify script
            # for local
        print('sleeping for 3 minutes')
        time.sleep(180)
    print('all configs tested, sleeping for 10 minutes')
    time.sleep(180)

run_main(args=get_process_arrays_args())

# import os
# import signal
# from subprocess import Popen, PIPE, TimeoutExpired
# from time import monotonic as timer
#
# start = timer()
# with Popen('sleep 30', shell=True, stdout=PIPE, preexec_fn=os.setsid) as process:
#     try:
#         output = process.communicate(timeout=1)[0]
#     except TimeoutExpired:
#         os.killpg(process.pid, signal.SIGINT) # send signal to the process group
#         output = process.communicate()[0]
