# from serpent_local_code.read_configs import (write_configuration_file)
from .utils import (rsync_files,
                    run_ssh_cmd,
                    read_json,
                    keys_exists,
                    write_configuration_file,
                    deep_merge_dicts,
                    merge_dict_list)


class SerpentOperations:
    def __init__(self, config, ignore_mods):
        self.config = config
        self.ignore_mods = ignore_mods

    def parse_variablized_value(self, value, server, mod):
        if not isinstance(value, str):
            return value

        if not ((value[0] == '<') and (value[len(value) - 1] == '>')):
            return value

        value_trim = value[1:-1]
        values = value_trim.split(':')

        if len(values) == 1:
            try:
                if self.get_unique_path(mod, server, values[0]) is None:
                    pass
                else:
                    return self.get_unique_path(mod, server, values[0])
            except:
                pass
            try:
                return self.config[mod][server][values[0]]
            except:
                pass
            return value

        if len(values) == 2:
            try:
                if self.get_unique_path(mod, server, values[0]) is None:
                    pass
                else:
                    return self.get_unique_path(values[0], server, values[1])
            except:
                pass
            try:
                return self.config[values[0]][server][values[1]]
            except:
                pass
            return value
        if len(values) == 3:
            try:
                if self.get_unique_path(values[0], values[1], values[2]) is None:
                    pass
                else:
                    return self.get_unique_path(values[0], values[1], values[2])
            except:
                pass
            try:
                return self.config[values[0]][values[1]][values[2]]
            except:
                pass
            return value
        if len(values) == 4:
            try:
                if self.get_unique_path(mod, server, values[0]) is None:
                    pass
                else:
                    return self.get_path_by_drive(values[0], values[1], values[3], values[2])

            except:
                pass
            try:
                return self.config[values[0]][values[1]][values[2]][values[3]]
            except:
                pass
        return value

    def parse_variablized_config(self):
        def recurse_dict(d, server, mod):
            for key, value in d.items():
                if isinstance(value, dict):
                    recurse_dict(d=value,
                                 server=server,
                                 mod=mod)
                else:
                    d[key] = self.parse_variablized_value(value=value,
                                                          server=server,
                                                          mod=mod)
            return d

        for mod, mod_config in self.config.items():
            if mod in ['all', 'priority',
                       "dir_priority",
                       "dir_all",
                       "dir_setup",
                       "dir_paths"]:
                continue
            for server, local_chtc_config in mod_config.items():
                d = recurse_dict(d=local_chtc_config,
                                 server=server,
                                 mod=mod)
                self.config[mod][server] = d
        return

    def mk_local_directories(self, server='local'):
        import os
        if server == 'local':
            mod_list = self.local_module_server_kvp().keys()
        else:
            mod_list = self.remote_server_module_kvp()[server]
        for mod_i in mod_list:
            dir_key_order_list = self.config[mod_i][server]['dir_key_order_list']
            mkdir_list = [x for x in dir_key_order_list if x.endswith('dir')]
            path_list_all = []
            for path_key_i in mkdir_list:
                path_list = self.get_path(mod_i, server, path_key_i)
                path_list_all.extend(path_list)
            path_list_set = list(set(path_list_all))
            for path_i in path_list_set:
                os.makedirs(path_i, exist_ok=True)

    def edit_arguments(self,
                       arguments,
                       incoming_ready=True,
                       incoming_complete=True,
                       sample_list_path=''):
        for key, value in arguments.items():

            if not isinstance(value, str):
                continue

            if not ((value[0] == '<') and (value[len(value) - 1] == '>')):
                continue

            value_trim = value[1:-1]
            values = value_trim.split(':')

            if len(values) < 2:
                if values[0] == 'incoming_exe_ready':
                    arguments[key] = 'FALSE'
                    if incoming_ready:
                        arguments[key] = 'TRUE'
                    continue
                if values[0] == 'incoming_exe_complete':
                    arguments[key] = 'FALSE'
                    if incoming_complete:
                        arguments[key] = 'TRUE'
                    continue
                if values[0] == 'incoming_ready':
                    arguments[key] = 'FALSE'
                    if incoming_ready:
                        arguments[key] = 'TRUE'
                    continue
                if values[0] == 'incoming_complete':
                    arguments[key] = 'FALSE'
                    if incoming_complete:
                        arguments[key] = 'TRUE'
                    continue
                if values[0] == 'sample_list_path':
                    arguments[key] = sample_list_path
                    continue
                continue

        return arguments

    def make_executable_command(self,
                                mod,
                                incoming_ready=True,
                                incoming_complete=True
                                ):
        import os
        arguments = keys_exists(self.config, alt={}, keys=[mod, 'local', 'arguments'])[1]
        executable = self.config[mod]['local']['executable']
        executable_dir = self.get_unique_path(mod, 'local', 'module_code_dir')
        preexectuable = ''
        if executable[-3:] == '.py':
            preexectuable = 'python3 '
        executable_cmd = os.path.join(executable_dir, executable)
        executable_cmd = '{0}{1}'.format(preexectuable, executable_cmd)
        arguments_string = ''
        arguments = self.edit_arguments(arguments=arguments,
                                        incoming_ready=incoming_ready,
                                        incoming_complete=incoming_complete)
        for key, value in arguments.items():
            arguments_string = '{0} {1} {2}'.format(arguments_string, key, value)
        if 'docker_image' in self.config[mod]['local'].keys():
            docker_image = self.config[mod]['local']['docker_image']
            gpu_tag = ''
            if 'gpu_count' in self.config[mod]['local'].keys():
                gpu_count = self.config[mod]['local']['gpu_count']
                if gpu_count > 0:
                    gpu_tag = '--gpus all '
            docker_string = 'docker run {0}'.format(gpu_tag)
            if 'mount_list' in self.config[mod]['local'].keys():
                for mount_dir in self.config[mod]['local']['mount_list']:
                    docker_string = '{0} -v {1}:{1}'.format(docker_string, mount_dir)
            docker_string = '{0} {1} {2}{3}'.format(docker_string,
                                                    docker_image,
                                                    executable_cmd,
                                                    arguments_string)
            return docker_string

        return '{0}{1}'.format(executable_cmd, arguments_string)

    def parse_value(self, value, calling_server='local', status_dir=None):

        """
        :param value: string needing parsing for the <> notation
        :return: if the file exists (process completed) then True, if not than False
        """

        from os import path
        # there may be multiple start trigger files so split first
        path_list = value.split(',')
        for path_i in path_list:
            # if they are not surrounded by "<" and ">" then use a full path gien
            if not ((path_i[0] == '<') and (path_i[len(path_i) - 1] == '>')):
                # if it exists keep it true, if not then it must be false, as all statements must be true for true
                if path.exists(path_i):
                    continue
                else:
                    return False
            else:
                # trim off the arrows and get the path values to a file name that is dynamic and join the status dir
                path_trim = path_i[1: -1]
                path_values = path_trim.split(':')

                filename = '{0}_{1}.txt'.format(path_values[0], path_values[1])
                # if it exists keep it true, if not then it must be false, as all statements must be true for true
                path_key = 'none'
                suffix = 'none'
                if path_values[1].startswith('complete'):
                    path_key = 'complete_file_path'
                    suffix = '_completed.txt'
                elif path_values[1].startswith('ready'):
                    path_key = 'ready_file_path'
                    suffix = '_ready.txt'
                print(path_i)
                print(path_key)
                path_value = self.get_unique_path(path_values[0], calling_server, path_values[1])
                print(self.get_unique_path(path_values[0], calling_server, path_key))
                if self.get_unique_path(path_values[0], calling_server, path_key) is not None:
                    if path.exists(self.get_unique_path(path_values[0], calling_server, path_key)):
                        continue
                    else:
                        return False
                else:
                    if status_dir is None:
                        return False
                    if path.exists(path.join(status_dir, '{0}{1}'.format(path_values[0], suffix))):
                        continue
                    else:
                        return False
        return True

    def check_incoming_status(self, mod_name, server_name, calling_server='local'):
        """
        checks the incoming status of a module/server combination
        :param mod_name: module of interest
        :param server_name: server of interest
        :param calling_server: where it is called from (local or remote server)
        :return: tupple of ready_flag, complete_flag (True is complete/ready, False is not)
        """
        # the list is a set since a dictionary keys must be a set
        server_list = list(self.config[mod_name].keys())
        # if both are missing return complete (this is literally impossible)
        if server_name not in self.config[mod_name].keys():
            return True, True
        status_dir = self.get_unique_path(mod_name, calling_server, 'status_dir')

        start_trigger_exists = False
        start_trigger_string = ''
        completed_trigger_exists = False
        completed_trigger_string = ''
        # prioritize local if there is a duplicate entry
        if 'local' in server_list:
            server_list.insert(0, server_list.pop(server_list.index('local')))
        # look for start trigger
        for server_i in server_list:
            if not start_trigger_exists and ('start_trigger' in self.config[mod_name][server_i].keys()):
                start_trigger_exists = True
                start_trigger_string = self.config[mod_name][server_i]['start_trigger']

            if not completed_trigger_exists and ('input_completed_trigger' in self.config[mod_name][server_i].keys()):
                completed_trigger_exists = True
                completed_trigger_string = self.config[mod_name][server_i]['input_completed_trigger']
        # if neither exist then there is no tiggers we just continue and return tue, true
        if not start_trigger_exists and not completed_trigger_exists:
            return True, True
        # if only the start_trigger exists then set them both to the start_trigger flag
        if not completed_trigger_exists and start_trigger_exists:
            ready_flag = self.parse_value(start_trigger_string, calling_server,status_dir)
            return ready_flag, ready_flag
        # if only the input_completed_trigger exists then set them both to the input_completed_trigger flag
        if completed_trigger_exists and not start_trigger_exists:
            completed_flag = self.parse_value(completed_trigger_string, calling_server,status_dir)
            return completed_flag, completed_flag
        # if both exists then check the status of each and return each value
        if completed_trigger_exists and start_trigger_exists:
            ready_flag = self.parse_value(start_trigger_string, calling_server,status_dir)
            completed_flag = self.parse_value(completed_trigger_string, calling_server,status_dir)
            return ready_flag, completed_flag
        # just let it pass if there is typos
        return True, True

    def get_priority(self):
        """
        gets the priority of a config
        :return:
        """
        for mod in self.config:
            for key_i in self.config[mod].keys():
                if keys_exists(self.config, [mod, key_i, 'priority'])[0]:
                    priority = self.config[mod][key_i]['priority']
                    if isinstance(priority, str):
                        if priority.isnumeric():
                            self.priority = int(priority)
                            return
        self.priority = 100

    def get_unique_path(self, mod, server, path_key):
        """
        returns the unique path or first path it comes across
         :param mod: module of interest
        :param server: server of interest
        :param path_key: path_key or interest i.e.:'status_dir'
        :return: returns a path (as a string).
        """
        drive_list = self.get_drive_from_path(mod, server, path_key)
        if not isinstance(drive_list, list):
            drive_list = [drive_list]
        for drive_i in drive_list:
            if keys_exists(self.config, [mod, server, drive_i, path_key])[0]:
                return self.config[mod][server][drive_i][path_key]
        # print("request does not exist mod: {0} ; server: {1} ; path_key: {2}".format(mod, server, path_key))

        return None

    def get_path(self, mod, server, path_key):
        """
        returns a list of paths for the mod/server/pathkey combination
        :param mod: module of interest
        :param server: server of interest
        :param path_key: path_key or interest i.e.:'status_dir'
        :return: a list of paths for the mod/server/pathkey combination
        [] if it does nto exist
        """
        drive_list = self.get_drive_from_path(mod, server, path_key)
        path_list = []
        if not isinstance(drive_list, list):
            drive_list = [drive_list]
        for drive_i in drive_list:
            if keys_exists(self.config, [mod, server, drive_i, path_key])[0]:
                path_list.append(self.config[mod][server][drive_i][path_key])
        if len(path_list) < 1:
            # print("request does not exist mod: {0} ; server: {1} ; path_key: {2}".format(mod, server, path_key))
            return []
        return path_list

    def get_path_un_server_by_drive(self, mod, server, path_key, drive):
        """
        returns the dictionary of  un, server, path with a drive input
        :param mod: module of interest
        :param server: server of interest
        :param path_key: path_key of interest i.e. 'status_dir'
        :param drive: drive of interest
        :return: dictionary of  un, server, path
        """

        if keys_exists(self.config, [mod, server, drive, path_key])[0]:
            un = keys_exists(self.config, alt=None, keys=[mod, server, drive, 'un'])[1]
            server_name = keys_exists(self.config, alt=None, keys=[mod, server, drive, 'server'])[1]
            return {'un': un,
                    'server': server_name,
                    'path': self.config[mod][server][drive][path_key]}
        return {}

    def get_path_by_drive(self, mod, server, path_key, drive):
        """
        if it exists, gets the path of hte drive.
        none if it does nto exist.
        :param mod: mod of interest
        :param server:server of interest
        :param path_key:
        :param drive:path key i.e. 'status_dir'
        :return:
        """
        return keys_exists(self.config, alt=None, keys=[mod, server, drive, path_key])[1]

    def get_drive_from_path(self, mod, server, path_key):
        """
        returns a list of drives from a mod/server/path key
        :param mod: module of interest
        :param server: server of interest
        :param path_key: path key i.e. 'status_dir'
        :return: a list of drives from a mod/server/path key
        """
        if keys_exists(self.config, ['all', 'dir_setup', path_key])[0]:
            if not isinstance(self.config['all']['dir_setup'][path_key], list):
                return [self.config['all']['dir_setup'][path_key]]
            else:
                return self.config['all']['dir_setup'][path_key]
        else:
            # print("request does not exist mod: {0} ; server: {1} ; path_key: {2}".format(mod, server, path_key))
            return []

    def get_module_list(self):
        """
        :return: returns a list of all the modules.
        """
        mod_list = list(self.config.keys())
        if 'all' in mod_list:
            mod_list.remove('all')
        for i in self.ignore_mods:
            if i in mod_list:
                mod_list.remove(i)
        return mod_list

    def remote_module_server_kvp(self):
        """
        :return: a dictionary of all the remote modules (keys) and servers (values, string)
        """
        mod_list = self.get_module_list()
        module_server_kvp = {}
        for module_name in mod_list:
            server_name = self.get_executable_server_name(module_name)
            if server_name != 'local':
                module_server_kvp[module_name] = server_name
        return module_server_kvp

    def local_module_server_kvp(self):
        """
        :return: a dictionary of all the local modules (keys) and servers (values, string (always local))
        """
        mod_list = self.get_module_list()
        module_local_kvp = {}
        for module_name in mod_list:
            if 'local' in self.config[module_name].keys():
                module_local_kvp[module_name] = 'local'
        return module_local_kvp

    def remote_server_module_kvp(self):
        """
        :return: a dictionary of all the server(keys) and list of modules(values).
        """
        mod_kvp = self.remote_module_server_kvp()
        # reverse
        server_kvp = {}
        for mod_i, server_i in mod_kvp.items():
            if server_i in server_kvp.keys():
                server_kvp[server_i].append(mod_i)
            else:
                server_kvp[server_i] = [mod_i]
        return server_kvp

    def get_server_list(self):
        """
        :return: list of servers in the config
        """
        mod_kvp = self.remote_module_server_kvp()
        server_list = [y for x, y in mod_kvp.items()]
        server_list = list(set(server_list))
        return server_list

    def get_executable_server_name(self, module_name):
        """
        for a module returns the server that the executable takes place on
        :param module_name: module of interest.
        :return:
        """
        server_name_list = self.config[module_name].keys()
        server_name = None
        for server_name_i in server_name_list:
            if 'executable' in self.config[module_name][server_name_i]:
                server_name = server_name_i
                continue
        return server_name

    # def get_priority(self, module_name):
    #     server_name = self.get_executable_server_name(module_name)
    #     if 'priority' in self.config[server_name]:
    #         try:
    #             priority = float(self.config[server_name]['priority'])
    #         except (TypeError, ValueError):
    #             priority = 100
    #         return priority

    def get_config(self):
        """
        :return: self.config dictionary
        """
        return self.config

    def get_drive_list(self, mod, server):
        """
        returns a list of drives.
        drives are the locations (default is 3) the data can reside: where you submit files reside (for code) ,
        where your input files reside
        where you output files reside
        all servers must use the same structure.
        The 1 or more drives can be on the same location i.e.: all in a local folder
        i.e.: input and output can be in same mounted directory
        HTC may require different input, output, and submit directories locations
        assumes a mounted file system
        :param mod: mod of interest
        :param server: server of interest
        :return:
        """
        for mod in self.config.keys():
            for server in self.config[mod].keys():
                if keys_exists(self.config, [mod, server, 'drive_list'])[0]:
                    return self.config[mod][server]['drive_list']
        else:
            print("request does not exist mod: {0} ; server: {1}".format(mod, server))
            return []

    def get_unique_un_server_path(self, mod, server, path_key):
        """
        gets a unique un server path for a remote destination
        :param mod: mod of interest
        :param server: server of intereset
        :param path_key: pathkey of interest (i.e. status_dir
        :return: dicitionary of {"un": un, "server":" server, "path":path}
        """
        drive_list = self.get_drive_from_path(mod, server, path_key)
        if not isinstance(drive_list, list):
            drive_list = [drive_list]
        for drive_i in drive_list:
            if keys_exists(self.config, [mod, server, drive_i, path_key])[0]:
                un = keys_exists(self.config, alt=None, keys=[mod, server, drive_i, 'un'])[1]
                server_name = keys_exists(self.config, alt=None, keys=[mod, server, drive_i, 'server'])[1]
                return {'un': un,
                        'server': server_name,
                        'path': self.config[mod][server][drive_i][path_key]}

        # print("request does not exist mod: {0} ; server: {1} ; path_key: {2}".format(mod, server, path_key))

        return None

    def get_submit_name(self):
        """
        gets the submit name for the configuration.
        :return:
        """
        for mod in self.config.keys():
            for server_i in self.config[mod].keys():
                if keys_exists(self.config, [mod, server_i, 'submit_name'])[0]:
                    submit_name = keys_exists(self.config, [mod, server_i, 'submit_name'])[1]
                    return submit_name
        return 'default'

    def get_un_server_path(self, mod, server, path_key, return_dict=False):
        """
        gets the server path and un as a dictionary or list
        :param mod: mod of interest
        :param server:  server you need the path for
        :param path_key: "key" i.e. status_dir, you need the path for.
        :param return_dict: whether to merely return a list or a dictionary of keys.
        :return:
        """
        path_list = []
        path_dict={}
        drive_list = self.get_drive_from_path(mod, server, path_key)
        if not isinstance(drive_list, list):
            drive_list = [drive_list]
        for drive_i in drive_list:
            if keys_exists(self.config, [mod, server, drive_i, path_key])[0]:
                un = keys_exists(self.config, alt=None, keys=[mod, server, drive_i, 'un'])[1]
                server_name = keys_exists(self.config, alt=None, keys=[mod, server, drive_i, 'server'])[1]
                path_list.append({'un': un,
                                  'server': server_name,
                                  'path': self.config[mod][server][drive_i][path_key]})
                path_dict[drive_i] = {'un': un,
                                      'server': server_name,
                                      'path': self.config[mod][server][drive_i][path_key]}
        if return_dict:
            if len(path_list) < 1:
            # print("request does not exist mod: {0} ; server: {1} ; path_key: {2}".format(mod, server, path_key))
                return []
            return path_dict
        if len(path_list) < 1:
            # print("request does not exist mod: {0} ; server: {1} ; path_key: {2}".format(mod, server, path_key))
            return []
        return path_list


class SerpentConfig(SerpentOperations):
    def __init__(self, config_path=None,
                 local_serpent_script_dir=None,
                 **kwargs):
        """
        Takes a config-path and local_serpent_script_dir and generates parses, compiles and aggregates the configuration
        :param config_path: full path of config of interest, unique to the run
        :param local_serpent_script_dir: where the serpent_script exists. (this code).
        :param kwargs:
        """
        import os
        self.config_path = config_path
        self.config = read_json(self.config_path)
        self.local_serpent_script_dir = local_serpent_script_dir
        self.priority = 100
        self.order_mod = []
        self.valid_config = True
        if self.config is None:
            return
        # keys_exists
        self.last_modified = round(os.path.getmtime(config_path))
        self.submit_name = os.path.basename(config_path[:-5])
        # Get the pipeline_dir, and default config dir (if it exists).
        # if pipline_code_dir is missing then pipeline dir takes precedence
        # if both are missing then check te default_config_dir if it exists there then maybe pipeline_dir's are there
        pipeline_dir_exists, pipeline_dir = keys_exists(self.config, ['all', 'local', 'pipeline_dir'])
        for key_i in self.config['all']['local'].keys():
            if pipeline_dir_exists:
                break
            pipeline_dir_exists, pipeline_dir = keys_exists(self.config, ['all', 'local', key_i, 'pipeline_dir'])
        pipeline_code_dir_exists, pipeline_code_dir = keys_exists(self.config, ['all', 'local', 'pipeline_code_dir'])
        for key_i in self.config['all']['local'].keys():
            if pipeline_code_dir_exists:
                break
            pipeline_code_dir_exists, pipeline_code_dir = keys_exists(self.config,
                                                                      ['all', 'local', key_i, 'pipeline_code_dir'])
        default_config_dir_exists, default_config_dir = keys_exists(self.config,
                                                                    ['all', 'local', 'default_config_dir'])
        for key_i in self.config['all']['local'].keys():
            if default_config_dir_exists:
                break
            default_config_dir_exists, default_config_dir = keys_exists(self.config,
                                                                        ['all', 'local', key_i, 'default_config_dir'])

        if not (pipeline_dir_exists or pipeline_code_dir_exists or default_config_dir_exists):
            print('Missing  one of the following all, all:local, all:local:pipeline_dir in json')
            self.config = {}
            self.valid_config = False
            return
        if pipeline_dir_exists:
            self.local_pipeline_dir = pipeline_dir
            self.local_pipeline_code_dir = self.local_pipeline_dir
            self.local_pipeline_name = os.path.basename(self.local_pipeline_dir)
            self.local_default_config_dir = os.path.join(self.local_pipeline_dir, 'config')
        if pipeline_code_dir_exists:
            self.local_pipeline_code_dir = pipeline_code_dir
            self.local_pipeline_name = os.path.basename(self.local_pipeline_code_dir)
            self.local_default_config_dir = os.path.join(self.local_pipeline_code_dir, 'config')
        if default_config_dir_exists:
            self.local_default_config_dir = default_config_dir

        # get the list of config files
        # get list of default configuration files
        default_config_list = os.listdir(self.local_default_config_dir)
        default_config_list = [os.path.join(self.local_default_config_dir, x) for x in default_config_list if
                               x[-5:] == ".json"]

        # get list of submission config files
        # sort alphabet for systematic priority
        default_config_list.sort()
        default_config_list.insert(0, os.path.join(self.local_serpent_script_dir,
                                                   'serpent_code',
                                                   'mods',
                                                   'default_dir_setup.json'))
        # loop through the default config files
        # combine them with update.  These should be separate and be wtritten not override settings between files
        # this is done in alphabetical order otherwise, likely

        default_config_dict = {}
        for file_i in default_config_list:
            json_i = read_json(file_i)
            default_config_dict = deep_merge_dicts(default_config_dict, json_i)
        # print(self.config['minion_midnight']['chtc']['submit_paths'])
        self.config = deep_merge_dicts(default_config_dict, self.config)

        ###########################################################################################
        #  if not declared already, add the all keys to the local and chtc config for each module #
        ###########################################################################################
        self.ignore_mods = ['priority',
                            "dir_priority",
                            "dir_all",
                            "dir_setup",
                            "dir_paths"]

        for mod in self.config.keys():
            if 'all' == mod:
                continue
            if mod in self.ignore_mods:
                continue
            if 'local' not in self.config[mod]:
                self.config[mod]['local'] = {}
            for local_chtc in self.config[mod]:
                if local_chtc not in self.config['all'].keys():
                    continue
                for key in self.config['all'][local_chtc].keys():
                    if key in self.config[mod][local_chtc]:
                        continue

                    if isinstance(self.config['all'][local_chtc][key], dict):
                        self.config[mod][local_chtc][key] = self.config['all'][local_chtc][key].copy()
                    else:
                        self.config[mod][local_chtc][key] = self.config['all'][local_chtc][key]

        for mod in self.config.keys():
            if mod in ['all']:
                continue
            if mod in self.ignore_mods:
                continue
            if "mark_as_completed" not in self.config[mod]['local'].keys():
                self.config[mod]['local']['mark_as_completed'] = 'False'

        # home_dir = os.path.expanduser("~")
        pipeline_dir_exists, pipeline_dir = keys_exists(self.config, ['all', 'local', 'pipeline_dir'])
        for key_i in self.config['all']['local'].keys():
            if pipeline_dir_exists:
                break
            pipeline_dir_exists, pipeline_dir = keys_exists(self.config, ['all', 'local', key_i, 'pipeline_dir'])
        pipeline_code_dir_exists, pipeline_code_dir = keys_exists(self.config, ['all', 'local', 'pipeline_code_dir'])
        for key_i in self.config['all']['local'].keys():
            if pipeline_code_dir_exists:
                break
            pipeline_code_dir_exists, pipeline_code_dir = keys_exists(self.config,
                                                                      ['all', 'local', key_i, 'pipeline_code_dir'])
        if pipeline_dir_exists or pipeline_code_dir_exists:
            if pipeline_dir_exists:
                self.local_pipeline_dir = pipeline_dir
                self.local_pipeline_code_dir = self.local_pipeline_dir
                self.local_pipeline_name = os.path.basename(self.local_pipeline_dir)
            if pipeline_code_dir_exists:
                self.local_pipeline_code_dir = pipeline_code_dir
                self.local_pipeline_name = os.path.basename(self.local_pipeline_code_dir)
        else:
            print('Missing  one of the following all, all:local, all:local:pipeline_dir in json')
            self.config = {}
            self.valid_config = False
            return
        # print(self.config['minion_midnight']['chtc']['submit_paths']['root_dir'])

        for mod in self.config.keys():
            if mod in ['all']:
                continue
            if mod in self.ignore_mods:
                continue
            server_name = self.get_executable_server_name(module_name=mod)
            # print(mod)
            if server_name != 'local':
                #### # needs to change priority NOt here though below because it adds it ot the config
                if ('priority' in self.config.keys()) and ('priority' not in self.config[mod][server_name]):
                    self.config[mod][server_name] = self.config['priority']

                self.GetRemotePaths(pipeline_name=self.local_pipeline_name,
                                    submit_name=self.submit_name,
                                    module_name=mod,
                                    server_name=server_name)

            self.GetRemotePaths(pipeline_name=self.local_pipeline_name,
                                submit_name=self.submit_name,
                                module_name=mod,
                                server_name='local',
                                serpent_code=os.path.join(local_serpent_script_dir, 'serpent_code'))
            #### # needs to change priority  move to a Serpent Operations.
            if ('priority' in self.config.keys()) and ('priority' not in self.config[mod]['local']):
                self.config[mod]['local'] = self.config['priority']
            self.get_priority()
            self.parse_variablized_config()
            self.local_compiled_config_dir = self.get_path(mod, 'local', 'compiled_config_dir')

            super().__init__(self.config, self.ignore_mods)
        self.sort_json_keys()

    def parse_variablized(self):
        """
        I checks if the config is blank ({} and will return the parse_variablized_config().
        :return:
        """
        if self.config == {}:
            return
        self.parse_variablized_config()
        return

    def mk_all_remote_configs(self):
        """
        Creates all the remote compiled configurations.
        loops through self.mk_remote_config()
        :return:
        """
        compiled_config_path_list = []
        server_list = self.get_server_list()
        for server_name_i in server_list:
            compiled_config_path_list.append(self.mk_remote_config(server_name_i))
        return compiled_config_path_list

    def mk_all_remote_directories(self):
        """
        loops thorugh self.mk_remote_directory()
        :return:
        """
        server_list = self.get_server_list()
        for server_name_i in server_list:
            self.mk_remote_directory(server_name_i)

    def mk_remote_directory(self, server):
        """
        makes the remote directories pertenant to the server and serpent  code.
        :param server: server that you need to make the remote directories
        :return:
        """
        mod_kvp = self.remote_module_server_kvp()
        mod_list = [x for x, y in mod_kvp.items() if y == server]
        path_list_all = []
        for mod_i in mod_list:
            dir_key_order_list = self.config[mod_i][server]['dir_key_order_list']
            mkdir_list = [x for x in dir_key_order_list if x.endswith('dir')]
            for path_key_i in mkdir_list:
                path_list = self.get_un_server_path(mod_i, server, path_key_i)
                path_list_all.extend(path_list)
        path_list_set = []
        for path_i in path_list_all:
            add_path = True
            for path_alt in path_list_set:
                if path_i == path_alt:
                    add_path = False
            if add_path:
                path_list_set.append(path_i)
        for path_i in path_list_set:
            if (path_i['un'] is None) or (path_i['server'] is None):
                continue
            mk_dir = 'mkdir -p {0}'.format(path_i['path'])
            run_ssh_cmd(cmd=mk_dir,
                        un=path_i['un'],
                        server=path_i['server'],
                        cwd=None)

    def launch_remote_services(self):
        """
        launches the remote submit_job.py script on every server that is in the config
        even if it is marked as complete, the service will be launched.
        if the service is already launched  (or looks like it is running) it will not be relaunched.
        It does not check if the service is running in a "stalled" state.
        :return:
        """
        from datetime import datetime
        mod_kvp = self.remote_module_server_kvp()
        # reverse
        server_kvp = {}
        for mod_i, server_i in mod_kvp.items():
            if server_i in server_kvp.keys():
                continue
            server_kvp[server_i] = mod_i
        for server_i, mod_i in server_kvp.items():
            path_list = self.get_un_server_path(mod_i, server_i, 'compiled_config_dir')
            if len(path_list) > 0:
                path_dict = path_list[0]

                python_flags = '--compiled_config {0}'.format(path_dict['path'])
                python_flags = '{0} --server_name {1}'.format(python_flags, server_i)
                check_job_submit_script = "pgrep -u {0} -f 'python3 -u /home'".format(path_dict['un'])
                check_submit_script_out = run_ssh_cmd(cmd=check_job_submit_script,
                                                      un=path_dict['un'],
                                                      server=path_dict['server'],
                                                      cwd=None)

                # convert the out put to a string and strip the whitespaces.
                check_submit_script_err = check_submit_script_out.stderr.decode('utf-8').strip()
                check_submit_script_out = check_submit_script_out.stdout.decode('utf-8').strip()
                print('Checked if the python job submit is running in {0}: {1} , {2}'.format(server_i,
                                                                                              check_submit_script_err,
                                                                                              check_submit_script_out))
                # if the stderr is blank and the stdout starts with 'M' or not blank then the job is running
                if not ((len(check_submit_script_err) < 1) and (len(check_submit_script_out) > 0)):
                    # if True:
                    # nohup will allow the python job to continue to run even after a disconnect
                    now = datetime.now()
                    dt_string = now.strftime("%Y_%m_%d__%H_%M_%S")
                    chtc_serpent_log = 'chtc_serpent_{0}.log'.format(dt_string)
                    chtc_serpent_err = 'chtc_serpent_{0}.err'.format(dt_string)
                    path_drive_list = self.get_drive_from_path(mod_i, server_i, 'serpent_server_code_dir')
                    if len(path_drive_list) > 0:
                        path_drive = path_drive_list[0]
                        path_dict = self.get_unique_un_server_path(mod_i, server_i, 'serpent_server_main_py_path')
                        if path_dict is not None:
                            run_chtc_job_submit = 'nohup python3 -u {0} {1} > {2} 2> {3} & '.format(
                                path_dict['path'],
                                python_flags,
                                chtc_serpent_log,
                                chtc_serpent_err)
                            run_ssh_cmd(cmd=run_chtc_job_submit,
                                        un=path_dict['un'],
                                        server=path_dict['server'],
                                        cwd=None,
                                        wait_to_finish=False)
                            print('Launched job submit controller script in chtc')
                            print('To manually check if script is running and get the PID: {0}'.format(
                                run_chtc_job_submit))
                    # import time
                    # print('SLEEPING!!!!!!!!!')
                    # time.sleep(60)
        return

    def mark_as_completed(self):
        """
        marks completed modules as complete by generating completed text files.
        Can only be reversed by deletign all relevant module/ server text files in all local/remote status directories
        :return:
        """
        from pathlib import Path

        for mod_name, mod_config in self.config.items():

            if mod_name == 'all':
                continue
            if mod_name in self.ignore_mods:
                continue
            if 'mark_as_completed' in mod_config['local']:
                if mod_config['local']['mark_as_completed'][0].upper() == 'T':
                    print('mark_as_completed: {}'.format(mod_name))
                    status_path_list = ["complete_file_path",
                                        "ready_file_path",
                                        "exe_complete_file_path"
                                        "exe_ready_file_path",
                                        "samples_uploaded_path",
                                        'static_uploaded_path']
                    for path_key in status_path_list:
                        path_list = self.get_path(mod_name, 'local', path_key)
                        if len(path_list) > 0:
                            path_i = path_list[0]
                            Path(path_i).touch()
        return

    def transfer_status_files(self, mod, server):
        """
        Transfers the status files from the mod to the server
        :param mod: mod name
        :param server:  servername
        :return:
        """
        remote_dict = self.get_unique_un_server_path(mod, server, 'status_dir')
        local_path = self.get_unique_path(mod, 'local', 'status_dir')
        # print('transfer status files')
        # print(remote_dict, local_path)
        if remote_dict is None:
            return
        rsync_files('{0}/'.format(local_path),
                    un=remote_dict['un'],
                    server=remote_dict['server'],
                    dest=remote_dict['path'],
                    server_is_dest=True)

    def transfer_core_files(self):
        """
        It will transfer:
        config files to each server (filtered for only needed)
        serpent_code so it can run on remote servers (latest version)
        module_code so it run any code not in a docker image
        server name file (so the server can use it as a backup to restart
        as the server name may change per pipeline).

        :return: <NA>
        """
        import os
        # server_list = self.get_server_list()
        mod_kvp = self.remote_module_server_kvp()
        # reverse
        server_kvp = {}
        for mod_i, server_i in mod_kvp.items():
            if server_i in server_kvp.keys():
                continue
            server_kvp[server_i] = mod_i
        for server_i, mod_i in server_kvp.items():

            path_dict = self.get_unique_un_server_path(mod_i, server_i, 'compiled_config_dir')
            local_path = self.get_unique_path(mod_i, 'local', 'compiled_config_dir')
            if (local_path is not None) and (path_dict is not None):
                local_path = os.path.join(local_path, server_i)
                self.transfer_config_files(local_path, path_dict)

            path_dict = self.get_unique_un_server_path(mod_i, server_i, 'serpent_server_code_dir')
            local_path = self.get_unique_path(mod_i, 'local', 'serpent_server_code_dir')
            if (local_path is not None) and (path_dict is not None):
                self.transfer_serpent_code(local_path, path_dict)

            path_dict = self.get_unique_un_server_path(mod_i, server_i, 'module_code_dir')
            local_path = self.get_unique_path(mod_i, 'local', 'module_code_dir')
            if (local_path is not None) and (path_dict is not None):
                self.transfer_module_code(local_path, path_dict)

            path_drive_list = self.get_drive_from_path(mod_i, server_i, 'serpent_server_code_dir')
            if len(path_drive_list) > 0:
                path_drive = path_drive_list[0]
                pipeline_dict = self.get_path_un_server_by_drive(mod_i, server_i, 'pipeline_code_dir', path_drive)
                if len(pipeline_dict) > 0:
                    self.mk_server_name_file(server_i, pipeline_dict)

    def transfer_config_files(self, local_path, remote_path_dict):
        """

        :param local_path: local path of the config files dir (compiled config files).
        :param remote_path_dict: remote path dictionary as generated by self.get_unique_un_server_path()
        :return:
        """
        rsync_files('{0}/'.format(local_path),
                    un=remote_path_dict['un'],
                    server=remote_path_dict['server'],
                    dest=remote_path_dict['path'],
                    server_is_dest=True,
                    delete_flag=True)
        print('Transferred local_compiled_config_dir to chtc')

    def transfer_static_files(self, server):
        """
        managed in the main.py for now.
        :param server:
        :return:
        """
        pass

    def transfer_serpent_code(self, local_path, remote_path_dict):
        """
        transfers the
        :param local_path: local path of the serpent code directory
        :param remote_path_dict: remote path dictionary as generated by self.get_unique_un_server_path()
        :return:
        """
        rsync_files('{0}/'.format(local_path),
                    un=remote_path_dict['un'],
                    server=remote_path_dict['server'],
                    dest=remote_path_dict['path'],
                    server_is_dest=True)

    def sort_json_keys(self):
        """
        sorts the json keys of the config to an optimized order based on graph topological sort
        Does not solve for the most efficient order.  BUT:
        works well as to be able to progress to the next module with out having
        to wait through a loop once completed with the previous module

        :return:
        """
        from collections import defaultdict

        class Graph:
            def __init__(self):
                self.graph = defaultdict(list)
                # self.numberofVertices = numberofVertices

            def addEdge(self, vertex, edge):
                self.graph[vertex].append(edge)
            def topogologicalSortUtil(self, v, visited, stack):
                visited.append(v)
                for i in self.graph[v]:
                    if i not in visited:
                        self.topogologicalSortUtil(i, visited, stack)
                stack.insert(0, v)

            def topologicalSort(self):
                visited = []
                stack = []
                for k in list(self.graph):
                    if k not in visited:
                        self.topogologicalSortUtil(k, visited, stack)
                return stack



        mod_dict = {}
        mod_all =[]
        for mod in self.config.keys():
            if mod == 'all':
                continue
            if mod in self.ignore_mods:
                continue
            mod_all.append(mod)
            input_triggers = []
            # loc_chtc = list(self.config[mod].keys())
            triggers = ['start_trigger', 'input_completed_trigger']
            for loc_chtc_i in self.config[mod].keys():
                if 'start_trigger' in self.config[mod][loc_chtc_i].keys():
                    append_list = self.config[mod][loc_chtc_i]['start_trigger'].split(',')
                    input_triggers = input_triggers + append_list
                elif 'input_completed_trigger' in self.config[mod][loc_chtc_i].keys():
                    append_list = self.config[mod][loc_chtc_i]['input_completed_trigger'].split(',')
                    input_triggers = input_triggers + append_list

            input_trigger_list = [x.split(':')[0][1:] for x in input_triggers]
            input_trigger_list = list(set(input_trigger_list))
            mod_dict[mod] = input_trigger_list
        g = Graph()
        for mod_i, value in mod_dict.items():
            for value_i in value:
                g.addEdge(mod_i, value_i)
        sorted_list = g.topologicalSort()
        for mod_i in mod_all:
            if mod_i not in sorted_list:
                sorted_list.append(mod_i)
        self.order_mod = sorted_list[::-1]
        return

    def mk_server_name_file(self, server, remote_path_dict):
        """
        Transfers the modules server name to a file.  In case the code is separated and you need to manually restart
        the script form the server and need the argument value for servername
          (you dont have access to the local computer).
        :param server: server name
        :param remote_path_dict: remote path dictionary as generated by self.get_unique_un_server_path()
        :return: (nothing)
        """
        import os

        mk_server_name_file = 'echo "{0}" > {1}'.format(server,
                                                        os.path.join(remote_path_dict['path'],
                                                                     'server_name.txt'))
        run_ssh_cmd(cmd=mk_server_name_file,
                    un=remote_path_dict['un'],
                    server=remote_path_dict['server'],
                    cwd=None)

    def transfer_module_code(self, local_path, remote_path_dict):
        """
        Transfers the modules code based on the paths sent
        :param local_path: local path of the module directory
        :param remote_path_dict: remote path dictionary as generated by self.get_unique_un_server_path()
        :return:
        """
        rsync_files('{0}/'.format(local_path),
                    un=remote_path_dict['un'],
                    server=remote_path_dict['server'],
                    dest=remote_path_dict['path'],
                    server_is_dest=True)

    def mk_local_config(self):
        """
        For trouble shooting you may want to make a local configuration.
        This file will have the complete config, not just the local information.
        Also serves as a log of the seettings used.
        :return:
        """
        mod_kvp = self.remote_module_server_kvp()
        mod_list = [x for x, y in mod_kvp.items()]
        # Sprint(mod_list)
        for mod in mod_list:
            if self.get_path(mod, 'local', 'compiled_config_dir') is not None:
                local_compiled_config_dir = self.get_path(mod, 'local', 'compiled_config_dir')
                print(local_compiled_config_dir)
                svr_config_path = write_configuration_file(config_contents=self.config,
                                                           submission_dir=local_compiled_config_dir[0],
                                                           config_path=self.config_path,
                                                           config_format=None)
                break

    def mk_remote_config(self, server_name):
        """
        server name to make the file for remote configuration.
        :param server_name: the servername you want ot make a compiled config file for
        :return: the path where the configuration was generated.
        """

        import os
        server_config = {}
        svr_config_path = ''

        mod_kvp = self.remote_module_server_kvp()
        mod_list = [x for x, y in mod_kvp.items() if y == server_name]
        for mod_i in mod_list:
            server_config[mod_i] = {}
            server_config[mod_i][server_name] = self.config[mod_i][server_name].copy()
        server_config['all'] = {}
        server_config['all']['dir_setup'] = self.config['all']['dir_setup'].copy()
        server_config['all']['order_mod'] = self.order_mod
        # Sprint(mod_list)
        if len(mod_list) > 0:
            local_compiled_config_dir = self.get_path(mod_list[0], 'local', 'compiled_config_dir')
            if isinstance(local_compiled_config_dir, list):
                local_compiled_config_dir = local_compiled_config_dir[0]
            compiled_config_dir = os.path.join(local_compiled_config_dir,
                                               server_name)
            os.makedirs(compiled_config_dir,
                        exist_ok=True)
            # print(server_config)
            svr_config_path = write_configuration_file(config_contents=server_config,
                                                       submission_dir=compiled_config_dir,
                                                       config_path=self.config_path,
                                                       config_format=None)
        else:
            print('server: {0} does not have modules'.format(server_name))
        return svr_config_path

    def remove_data_files_after_flags(self, mod_name):
        result_complete_flag = False
        sample_complete_flag = False
        # make sure local/paths/status_dir and local/remove_remote_files_after exists
        # checks for the status completed for the list of prereqs before deleting.
        if keys_exists(self.config, [mod_name, 'local', 'remove_remote_files_after'])[0]:
            remove_remote_files_string = self.config[mod_name]['local']['remove_remote_files_after']
            result_complete_flag = self.parse_value(value=remove_remote_files_string)
            sample_complete_flag = result_complete_flag
            return result_complete_flag, sample_complete_flag
        if keys_exists(self.config, [mod_name, 'local', 'remove_remote_result_files_after'])[0]:
            remove_remote_files_string = self.config[mod_name]['local']['remove_remote_result_files_after']
            result_complete_flag = self.parse_value(value=remove_remote_files_string)
        if keys_exists(self.config, [mod_name, 'local', 'remove_remote_sample_files_after'])[0]:
            remove_remote_files_string = self.config[mod_name]['local']['remove_remote_sample_files_after']
            sample_complete_flag = self.parse_value(value=remove_remote_files_string)
        return sample_complete_flag, result_complete_flag

    def remove_remote_files_from_mod(self, mod_name, return_results=True):
        from pathlib import Path
        import os
        if keys_exists(self.config, [mod_name, 'local', 'return_results'])[0]:
            if isinstance(self.config[mod_name]['local']['return_results'], str):
                if self.config[mod_name]['local']['return_results'].upper().startswith('F'):
                    return_results = False

        completed_flags = self.remove_data_files_after_flags(mod_name)
        samples_complete_flag = completed_flags[0]
        results_complete_flag = completed_flags[1]
        print('completed_flags: samples, results')
        print(completed_flags)
        results_removed_path = self.get_unique_path(mod_name, 'local', 'results_removed_path')
        server_name = self.get_executable_server_name(mod_name)
        if results_complete_flag and (not os.path.isfile(results_removed_path)):
            print('REMOVEING FILES TEST')
            # raise('stop here test')
            print('checking to remove result files for: {0}'.format(mod_name))
            local_output_dir = self.get_unique_path(mod_name, 'local', 'module_out_dir')
            remote_dir = self.get_unique_un_server_path(mod_name, server_name, 'module_out_dir')
            if (local_output_dir is None) or (remote_dir is None) or (not return_results):
                return
            rsync_files(source='{0}/'.format(remote_dir['path']),
                        un=remote_dir['un'],
                        server=remote_dir['server'],
                        dest=local_output_dir,
                        server_is_dest=False,
                        control_path='$HOME/.ssh/%L-%r@%h:%p',
                        rsync_flag='-aP',
                        source_from_file_list=False,
                        cwd=None,
                        check_sum_only=False,
                        ignore_errors=False,
                        delete_flag=False)

            checksum_result = rsync_files(source='{0}/'.format(remote_dir['path']),
                                          un=remote_dir['un'],
                                          server=remote_dir['server'],
                                          dest=local_output_dir,
                                          server_is_dest=False,
                                          control_path='$HOME/.ssh/%L-%r@%h:%p',
                                          rsync_flag='-aP',
                                          source_from_file_list=False,
                                          cwd=None,
                                          check_sum_only=True)
            print('checksum_value: {0}'.format(checksum_result))
            checksum_result_stdout = checksum_result.stdout.decode('utf-8').strip()
            if checksum_result_stdout == "0":
                ssh_cmd_out = run_ssh_cmd(cmd="rm -rf {0}".format(remote_dir['path']),
                                          un=remote_dir['un'],
                                          server=remote_dir['server'],
                                          control_path='$HOME/.ssh/%L-%r@%h:%p',
                                          rsync=False)
                print("deleting {0}@{1}:{2}".format(remote_dir['un'], remote_dir['server'], remote_dir['path']))
                print("result: {0}".format(ssh_cmd_out))
                Path(results_removed_path).touch()
            else:
                print('issue transferring results, did not remove files')
        samples_removed_path = self.get_unique_path(mod_name, 'local', 'samples_removed_path')
        if samples_complete_flag and (not os.path.isfile(samples_removed_path)):
            print('checking to remove sample files for: {0}'.format(mod_name))
            remote_dir = self.get_unique_un_server_path(mod_name, server_name, 'module_in_dir')
            ssh_cmd_out = run_ssh_cmd(cmd="rm -rf {0}".format(remote_dir['path']),
                                      server=remote_dir['server'],
                                      un=remote_dir['un'],
                                      control_path='$HOME/.ssh/%L-%r@%h:%p',
                                      rsync=False)
            print('Removing files from {0}@{1}:{2}'.format(remote_dir['un'], remote_dir['server'], remote_dir['path']))
            print("result: {0}".format(ssh_cmd_out))
            Path(samples_removed_path).touch()
        return

    def GetRemotePaths(self,
                       pipeline_name=None,
                       submit_name=None,
                       module_name=None,
                       server_name=None,
                       serpent_code=None):
        from os import path
        import re
        # self.input_config = input_config
        # self.paths = {}
        # self.ms_config = self.input_config[module_name][server_name]
        #
        # self.dir_setup = self.input_config['all']['dir_setup']
        # self.dir_paths = self.input_config['all']['dir_paths']
        # ms_config = config[module_name][server_name]
        self.config[module_name][server_name]['submit_name'] = submit_name
        dir_setup = self.config['all']['dir_setup'].copy()
        dir_paths = self.config['all']['dir_paths'].copy()

        def sort_dict_list_value_pair_keys(mod_dict):
            """
            a dictionary key vaue pairs, where values (lists) that (might) contain keys
            puts them in order based on the keys to ensure keys are not called out of order
            example: path may need root_dir.  therefore root dir must be solved before submit dir as it is a prereq.
            """
            order_mod = list(mod_dict.keys())
            for mod_i in mod_dict.keys():
                if len(mod_dict[mod_i]) < 1:
                    order_mod.remove(mod_i)
                    order_mod.insert(0, mod_i)
                    continue
                higher_index = 0
                for mod_pre in mod_dict[mod_i]:
                    mod_pre_index = order_mod.index(mod_pre)
                    if mod_pre_index > higher_index:
                        higher_index = mod_pre_index
                order_mod.remove(mod_i)
                order_mod.insert(higher_index + 1, mod_i)
            return order_mod

        def parse_paths_order(dir_paths):
            """
            parses the "<>" notation to find keys they point
            """
            dir_keys = list(dir_paths.keys())
            dir_paths_order = {}
            for dir_key_i, dir_path_i in dir_paths.items():
                new_path = []
                for path_part in dir_path_i:
                    if "<" in path_part:
                        # path_part_exists = False
                        start = path_part.find("<") + len("<")
                        end = path_part.find(">")
                        substring = path_part[start:end]
                        if substring in dir_keys:
                            new_path.append(substring)
                dir_paths_order[dir_key_i] = new_path
            return dir_paths_order

        dir_paths_order = parse_paths_order(dir_paths)
        dir_key_order_list = sort_dict_list_value_pair_keys(dir_paths_order)
        drive_list = []
        for dir_key_i, drive_i in dir_setup.items():
            # add paths that must be declared to the beginning
            if dir_key_i not in dir_key_order_list:
                dir_key_order_list.insert(0, dir_key_i)
            # extract a drive list by going through the items
            if isinstance(drive_i, list):
                for x in drive_i:
                    if x not in drive_list:
                        drive_list.append(x)
            else:
                if drive_i not in drive_list:
                    drive_list.append(drive_i)
        # initialize the drives
        self.config[module_name][server_name]['drive_list'] = drive_list
        self.config[module_name][server_name]['dir_key_order_list'] = dir_key_order_list

        for drive_i in drive_list:
            if drive_i not in self.config[module_name][server_name].keys():
                self.config[module_name][server_name][drive_i] = {}

            self.config[module_name][server_name][drive_i]['pipeline_name'] = pipeline_name
            self.config[module_name][server_name][drive_i]['submit_name'] = submit_name
            self.config[module_name][server_name][drive_i]['module_name'] = module_name
            if 'executable' in self.config[module_name][server_name].keys():
                self.config[module_name][server_name][drive_i]['executable'] = self.config[module_name][server_name][
                    'executable']
        for dir_key_i in dir_key_order_list:
            dir_path_i = None
            if dir_key_i in dir_paths.keys():
                dir_path_i = dir_paths[dir_key_i]
            drive_list = dir_setup[dir_key_i]
            if not isinstance(drive_list, list):
                drive_list = [drive_list]
            for drive_i in drive_list:
                if dir_key_i in self.config[module_name][server_name][drive_i].keys():
                    continue
                if (dir_key_i == 'serpent_server_code_dir') and (server_name == 'local'):
                    self.config[module_name][server_name][drive_i][dir_key_i] = serpent_code
                    break
                if (dir_path_i is None) or (dir_key_i == 'home_dir'):
                    for drive_alt in drive_list:
                        if dir_key_i in self.config[module_name][server_name][drive_alt].keys():
                            self.config[module_name][server_name][drive_i][dir_key_i] = \
                                self.config[module_name][server_name][drive_alt][dir_key_i]
                            break
                    continue
                # else path is declared
                new_path = []
                path_part_exists = True
                for path_part in dir_path_i:
                    if "<" in path_part:
                        path_part_exists = False
                        start = path_part.find("<") + len("<")
                        end = path_part.find(">")
                        substring = path_part[start:end]
                        if substring in self.config[module_name][server_name][drive_i].keys():
                            replace_text = self.config[module_name][server_name][drive_i][substring]
                            path_part = re.sub('<.*?>', replace_text, path_part, flags=re.DOTALL)

                            path_part_exists = True
                    new_path.append(path_part)
                if path_part_exists:
                    self.config[module_name][server_name][drive_i][dir_key_i] = path.join(*new_path)

        print(module_name)

### HTC OPERATIONS


