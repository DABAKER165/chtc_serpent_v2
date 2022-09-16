#!/usr/bin/env python3
from argparse import ArgumentParser

from datetime import datetime

from serpent_code.mods.utils import (rsync_files,
                                     run_ssh_cmd,
                                     get_configuration_filepaths)
from serpent_code.mods.open_configuration import (open_and_sort_configurations)
# from serpent_code.mods.serpent_config import (open_and_sort_configurations)


def parse_process_arrays_args(parser: ArgumentParser):
    """Parses the python script arguments from bash and makes sure files/inputs are valid"""
    parser.add_argument('--config_dir', metavar='config_dir',
                        help='Directory of each submission configuration', required=True)
    parser.add_argument('--submission_dir', metavar='submission_dir',
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

    # make sure the program directory exists
    # Only retransfer pipeline specific files (modules code) as needed
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
            print('{0} Module Order:'.format(sc.submit_name))
            print(sc.order_mod)
            local_compiled_config_dir = sc.local_compiled_config_dir
            if isinstance(local_compiled_config_dir, list) and len(local_compiled_config_dir) > 0:
                local_compiled_config_dir = local_compiled_config_dir[0]
            os.makedirs(local_compiled_config_dir, exist_ok=True)
            if not config_files_cleared:
                # clear config directory contents for easier management
                compile_config_files = os.listdir(local_compiled_config_dir)
                for f in compile_config_files:
                    path_i = os.path.join(local_compiled_config_dir, f)
                    if path_i.endswith(".json"):
                        os.remove(path_i)
                        continue
                    if os.path.isdir(path_i):
                        shutil.rmtree(path_i, ignore_errors=True)
                        # os.rmdir(path_i, )

            config_files_cleared = True
            #####################################################################
            #  Write the aggregated configuration files to be uploaded to CHTC  #
            #####################################################################
            print('mk_local_directories')
            sc.mk_local_directories()
            print('mk_all_remote_configs')
            sc.mk_all_remote_configs()
            print('mk_local_config')
            sc.mk_local_config()
            print('mk_all_remote_directories')
            sc.mk_all_remote_directories()
            print('wrote_configuration_files to {0}/config'.format(local_compiled_config_dir))
            print('read_configuration_files')
            # dump the configurations to local submission dir
            print('transfer_core_files')
            sc.transfer_core_files()
            print('launch_remote_services')
            sc.launch_remote_services()

            # run pre script
            print('Getting status files from chtc')
            ################################################
            #  Start looping through the pipeline modules  #
            ################################################
            # print(sc.config)
            sc.mark_as_completed()


            for mod_i in sc.order_mod:
                # skip the all as it is not an executable module
                if mod_i == 'all':
                    continue
                print('{0} -- {1}'.format(sc.submit_name, mod_i))
                # copy the sample files to staging
                mod_config = sc.config[mod_i]
                server_name = sc.get_executable_server_name(mod_i)
                if server_name is None:
                    print('missing executable, skipping module')
                    continue

                if os.path.exists(sc.get_unique_path(mod_i, 'local', 'complete_file_path')):
                    # print(sc.config[mod_i]['local']['submit_paths'])
                    # print(mod_i)
                    print(sc.get_unique_path(mod_i, 'local', 'complete_file_path'))
                    print('mod_completed: {0}'.format(mod_i))
                    continue
                ################################
                #  Give status files to server  #
                ################################
                if server_name not in ['all', 'local']:
                    sc.transfer_status_files(mod_i, server_name)
                ################################
                #  Get status files FROM server  #
                ################################
                # get all the servernames
                server_kvp = sc.remote_server_module_kvp()
                for server_i, mod_list in server_kvp.items():
                    if server_i in ['all', 'local']:
                        continue
                    for mod_k in mod_list:
                        remote_dict_i = sc.get_unique_un_server_path(mod_k, server_i, 'status_dir')
                        local_status_dir = sc.get_unique_path(mod_k, 'local', 'status_dir')
                        rsync_files('{0}/'.format(remote_dict_i['path']),
                                    un=remote_dict_i['un'],
                                    server=remote_dict_i['server'],
                                    dest=local_status_dir,
                                    server_is_dest=False)
                        break
                incoming_ready, incoming_complete = sc.check_incoming_status(mod_name=mod_i,
                                                                             server_name=server_name)

                # make a completion directory
                print('incoming_ready: {0} -- incoming_completed: {1}'.format(incoming_ready, incoming_complete))
                # if the arguments exist for remove_remote_samples_after remove_remote_results_after or
                # remove_remote_files_ after mod is completed, must be checked before anything else
                sc.remove_remote_files_from_mod(mod_i)
                #################################################################
                #  Check to see if the incoming prereq's are ready (or complete #
                #################################################################

                if not (incoming_ready or incoming_complete):
                    continue
                if os.path.isfile(sc.get_unique_path(mod_i, 'local', 'complete_file_path')) and incoming_complete:
                    continue
                #####################################################
                #  If the module is local, execute the local script #
                #####################################################
                if server_name == 'local':
                    print(mod_i, server_name)
                    # check for module completion
                    if os.path.isfile(
                            sc.get_unique_path(mod_i, 'local', 'complete_file_path')) and incoming_complete:
                        continue
                    # Check for a local executable.
                    exe_cmd = sc.make_executable_command(mod=mod_i,
                                                         incoming_ready=incoming_ready,
                                                         incoming_complete=incoming_complete)
                    # lock module
                    if 'lock_remote' in sc.config[mod_i]['local']:
                        lock_module = sc.config[mod_i]['local']['lock_remote']

                        server_name = sc.get_executable_server_name(lock_module)
                        remote_dict = sc.get_unique_un_server_path(lock_module, server_name, 'transfer_lock_file_path')
                        cmd = 'touch {0}'.format(remote_dict['path'])
                        rm_file_out = run_ssh_cmd(cmd=cmd,
                                                  un=remote_dict['un'],
                                                  server=remote_dict['server'],
                                                  cwd=None)
                        print(cmd)


                    print('Local sh script: {0}'.format(exe_cmd))
                    sc.chmod_executable(mod=mod_i)
                    subprocess.run([exe_cmd], shell=True)
                    # unlock module
                    if 'lock_remote' in sc.config[mod_i]['local']:
                        lock_module = sc.config[mod_i]['local']['lock_remote']
                        server_name = sc.get_executable_server_name(lock_module)
                        remote_dict = sc.get_unique_un_server_path(lock_module, server_name, 'transfer_lock_file_path')
                        cmd = 'rm -f {0}'.format(remote_dict['path'])
                        print(cmd)
                        rm_file_out = run_ssh_cmd(cmd=cmd,
                                                  un=remote_dict['un'],
                                                  server=remote_dict['server'],
                                                  cwd=None)
                        print(rm_file_out)

                    if 'arguments' not in mod_config['local'].keys():
                        pathlib.Path(sc.get_unique_path(mod_i, 'local', 'complete_file_path')).touch()
                        pathlib.Path(sc.get_unique_path(mod_i, 'local', 'ready_file_path')).touch()
                    else:
                        if '--status_dir' not in mod_config['local']['arguments'].keys():
                            print('No Status Dir creating touches')
                            pathlib.Path(sc.get_unique_path(mod_i, 'local', 'complete_file_path')).touch()
                            pathlib.Path(sc.get_unique_path(mod_i, 'local', 'ready_file_path')).touch()

                ###################################################
                #  If the module is a remote server, execute the server script #
                ###################################################
                else:

                    ##############################################################################
                    #  If the module needs to transfer files from CHTC                           #
                    #  - Lock the file transfer to be sure unfinished files are not transferred  #
                    #  - This is done by creating a file on CHTC with Touch                      #
                    #  - Transfer the files with rsync                                           #
                    #  - The CHTC transfer file folder need to change the name to the module.    #
                    #  - Release the lock by removing the file that created the lock             #
                    ##############################################################################
                    # first transfer any static files as needed using rsync
                    # These are shared files between runs so they should not go in a run specific directory
                    # These files could be several GB so we dont want to move TB's of data for no reason.
                    if 'static_files' in mod_config[server_name]:
                        # Allows the file input to be a list, dictionary or string that is split by ','
                        static_files_list = mod_config[server_name]['static_files']
                        if isinstance(static_files_list, str):
                            static_files_list = mod_config[server_name]['static_files'].split(',')
                        if isinstance(static_files_list, dict):
                            static_files_list = list(set(static_files_list.values()))
                        # print(static_files_list)
                        remote_dict = sc.get_unique_un_server_path(mod_i, server_name, 'static_dir')
                        for static_file in static_files_list:
                            rsync_files(static_file,
                                        un=remote_dict['un'],
                                        server=remote_dict['server'],
                                        dest=remote_dict['path'],
                                        server_is_dest=True)
                        print('Transferred static files to server as needed')
                        # NEEDS AN IMEDIATE FIX maybe?
                        # need to give a static_files transferred flag.
                        remote_dict = sc.get_unique_un_server_path(mod_i, server_name, 'static_uploaded_path')
                        cmd = 'touch {0}'.format(remote_dict['path'])

                        run_ssh_cmd(cmd=cmd,
                                    un=remote_dict['un'],
                                    server=remote_dict['server'],
                                    cwd=None)
                    # skip__this = False
                    # if skip__this:
                    if 'transfer_to_server' in mod_config[server_name]:
                        print('transfer_to_server {0}'.format(mod_i))

                        # chtc_sample_folder = os.path.join(server_paths['staging_samples_dir'], mod_i)
                        # send a lock to not start until after the rsync is complete as not to process partial files
                        remote_dict = sc.get_unique_un_server_path(mod_i, server_name, 'transfer_lock_file_path')
                        cmd = 'touch {0}'.format(remote_dict['path'])
                        rm_file_out = run_ssh_cmd(cmd=cmd,
                                                  un=remote_dict['un'],
                                                  server=remote_dict['server'],
                                                  cwd=None)
                        print(cmd)
                        print(rm_file_out)
                        # this may need to be dynamically named
                        # Needs to trail with '/' to transfer the folder contents and not the folder isself
                        transfer_from_dir = mod_config[server_name]['transfer_to_server']
                        print(transfer_from_dir)
                        if transfer_from_dir[len(transfer_from_dir) - 1] != '/':
                            if transfer_from_dir[-2:] == '/"':
                                pass
                            elif transfer_from_dir[len(transfer_from_dir) - 1] == '"':
                                transfer_from_dir = transfer_from_dir[:-1] + '/"'
                            else:
                                transfer_from_dir = transfer_from_dir + '/'
                        # else:
                        #     transfer_from_dir = transfer_from_dir + '/'

                        if transfer_from_dir[len(transfer_from_dir) - 1] == '"':
                            transfer_from_dir = transfer_from_dir[1:-1]
                        remote_dict = sc.get_unique_un_server_path(mod_i, server_name, 'module_in_dir')
                        rsync_files(transfer_from_dir,
                                    un=remote_dict['un'],
                                    server=remote_dict['server'],
                                    dest=remote_dict['path'],
                                    server_is_dest=True)
                        print('Transfered sample files to server as needed')
                        # remove lock
                        remote_dict = sc.get_unique_un_server_path(mod_i, server_name, 'transfer_lock_file_path')
                        cmd = 'rm -f {0}'.format(remote_dict['path'])
                        print(cmd)

                        rm_file_out = run_ssh_cmd(cmd=cmd,
                                                  un=remote_dict['un'],
                                                  server=remote_dict['server'],
                                                  cwd=None)
                        print(rm_file_out)
                        if incoming_complete:
                            # Create file saying the samples are uploaded.
                            remote_dict = sc.get_unique_un_server_path(mod_i, server_name,
                                                                       'samples_uploaded_path')
                            cmd = 'touch {0}'.format(remote_dict['path'])
                            run_ssh_cmd(cmd=cmd,
                                        un=remote_dict['un'],
                                        server=remote_dict['server'],
                                        cwd=None)
                            print('all Samples uploaded text file flag sent')

                    ################################
                    #  Get status files FROM CHTC  #
                    ################################
                    remote_dict = sc.get_unique_un_server_path(mod_i, server_name, 'status_dir')
                    local_status_dir = sc.get_unique_path(mod_i, 'local', 'status_dir')
                    rsync_files('{0}/'.format(remote_dict['path']),
                                un=remote_dict['un'],
                                server=remote_dict['server'],
                                dest=local_status_dir,
                                server_is_dest=False)

                    print('Getting status files from remote server')
                    ######################################
                    #  Check if it is ready or complete  #
                    ######################################

                    server_module_complete = os.path.isfile(sc.get_unique_path(mod_i,
                                                                               'local',
                                                                               'exe_complete_file_path'))
                    print(sc.get_unique_path(mod_i,
                                             'local',
                                             'exe_complete_file_path'))
                    server_download_ready = os.path.isfile(sc.get_unique_path(mod_i,
                                                                              'local',
                                                                              'exe_ready_file_path'))
                    if server_download_ready or server_module_complete:
                        # get completion trigger files from chtc
                        # if exists locally first? chtc_submit_creds['staging_local_completion_dir']

                        get_output = True
                        if 'get_output' in mod_config['local'].keys():
                            if mod_config[server_name]['get_output'].upper()[0] == 'F':
                                get_output = False
                            else:
                                get_output = True
                        if 'get_output' in mod_config[server_name].keys():
                            if mod_config[server_name]['get_output'].upper()[0] == 'F':
                                get_output = False
                            else:
                                get_output = True

                        if get_output:
                            print('Get output {0}'.format(get_output))
                            print(mod_i, sc.config[mod_i])
                            remote_dict = sc.get_unique_un_server_path(mod_i, server_name, 'module_out_dir')
                            local_path = sc.get_unique_path(mod_i, 'local', 'module_out_dir')
                            os.makedirs(local_path, exist_ok=True)
                            # check sum remote
                            rsync_files('{0}/'.format(remote_dict['path']),
                                        un=remote_dict['un'],
                                        server=remote_dict['server'],
                                        dest=local_path,
                                        server_is_dest=False)
                            print('Got output files from {0}'.format(server_name))
                            # check sum local

                        if server_download_ready or server_module_complete:
                            local_path = sc.get_unique_path(mod_i, 'local', 'ready_file_path')
                            pathlib.Path(local_path).touch()
                            if server_module_complete and incoming_complete:
                                print(mod_i, 'completed')
                                local_path = sc.get_unique_path(mod_i, 'local', 'complete_file_path')
                                pathlib.Path(local_path).touch()

                            remote_dict = sc.get_unique_un_server_path(mod_i, server_name, 'status_dir')
                            local_path = sc.get_unique_path(mod_i, 'local', 'status_dir')
                            print('{0}/'.format(remote_dict['path']))
                            print('{0}/'.format(local_path))
                            rsync_files('{0}/'.format(remote_dict['path']),
                                        un=remote_dict['un'],
                                        server=remote_dict['server'],
                                        dest=local_path,
                                        server_is_dest=False)
                            rsync_files('{0}/'.format(local_path),
                                        un=remote_dict['un'],
                                        server=remote_dict['server'],
                                        dest=remote_dict['path'],
                                        server_is_dest=True)
                            if server_module_complete and incoming_complete:
                                print('{0} is complete and the local is complete'.format(server_name))
                            else:
                                print('{0} is ready so the local is ready'.format(server_name))
        now = datetime.now()
        dt_string = now.strftime("%Y_%m_%d__%H_%M_%S")
        print('sleeping for 2 minutes - {0}'.format(dt_string))
        time.sleep(120)


run_main(args=get_process_arrays_args())
