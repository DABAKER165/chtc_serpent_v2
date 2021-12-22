#!/usr/bin/env python3


def write_configuration_file(config_contents, submission_dir, config_path, config_format=None):
    """

    Writes the dictionary object (or array of dictionaries) to a config file
    currently only supports json files
    Needed to transport a json from the local server to remote, configs.
    :param config_contents: config dictionary needed to be written
    :param submission_dir: directory the config is written to
    :param config_path: input config path (to scrape the name).
    :param config_format: optional config format (JSON) if you do not want to pull it from extension
    :return: the json path (or '' if it does not exist).
    """
    format_dict = {'JSON': 'JSON'}
    import pathlib
    import os
    import json
    if config_format is None:
        file_extension = pathlib.Path(config_path).suffix
        config_format = file_extension.upper()
    config_format = config_format.upper()

    if config_format == '.JSON':
        os.makedirs(submission_dir, exist_ok=True)
        json_path = os.path.join(submission_dir, os.path.basename(config_path))
        with open(json_path, 'w') as f:
            json.dump(config_contents, f)
        return json_path
    return ''


def deep_merge_dicts(original, incoming):
    """
    Deep merge two dictionaries. Modifies original.
    For key conflicts if both values are:
     a. dict: Recursively call deep_merge_dicts on both values.
     c. any other type: Value is overridden.
     d. conflicting types: Value is overridden.
    :param original: dictionary with priority (will overwrite identical values that are non-dict.
    :param incoming: dictionary with secondary priority
    :return: the merged dictionary.
    """
    for key in incoming:
        if key in original:
            if isinstance(original[key], dict) and isinstance(incoming[key], dict):
                deep_merge_dicts(original[key], incoming[key])

            else:
                if isinstance(incoming[key], dict):
                    original[key] = incoming[key].copy()
                else:
                    original[key] = incoming[key]
        else:
            if isinstance(incoming[key], dict):
                original[key] = incoming[key].copy()
            else:
                original[key] = incoming[key]
    return original.copy()


def merge_dict_list(dict1, dict2):
    """
    mergest a list of dicts 2 deep by the keys overwriting the first with the second
    :param dict1: dictionary with priority (will overwrite identical values that are non-dict.
    :param dict2: dictionary with secondary priority
    :return: the merged dictionary.
    """
    dict3 = {**dict1, **dict2}
    for key, value in dict3.items():
        if key in dict1 and key in dict2:
            if isinstance(value, list):
                dict3[key].append(dict1[key])
            else:
                dict3[key] = [value, dict1[key]]
    return dict3

def set_from_dict_list(inputlist):
    """
    turns a list of dicitonaries into a unique list
    :param inputlist:
    :return: returns a unique set of dictionaries as you cannot take a set of objects.
    """
    def set_from_dict(d):
        return frozenset(
            (k, set_from_dict(v) if isinstance(v, dict) else v)
            for k, v in d.items())
    seen = set()
    result = []
    for d in inputlist:
        representation = set_from_dict(d)
        if representation in seen:
            continue
        result.append(d)
        seen.add(representation)
    return result


def keys_exists(element, keys=None, alt=None):
    """
    Check if *keys (nested) exists in `element` (dict).
    :param element: the dictionary
    :param keys: list of keys you want to check if they exist in order of inner to outer
    :param alt: what to return if it does not exist
    :return: a tuple, bool to if the key exists, and the value key list (or the alt if it does not exist)
    """
    if not isinstance(element, dict):
        raise AttributeError('keys_exists() expects dict as first argument.')
    if len(keys) == 0:
        raise AttributeError('keys_exists() expects at least two arguments, one given.')
    if isinstance(keys[0], list):
        keys = keys[0]
    _element = element
    for key in keys:
        try:
            if isinstance(_element, dict):
                _element = _element[key]
            else:
                return False, alt
        except KeyError:
            return False, alt
    return True, _element


def read_json(config_path, empty={}):
    """
    Basic json read function, throws warnings and returns [] if file does not exist or is not of valid format
    :param config_path: the file path of the json file.
    :param empty: what to return if the file is empty (or does not  exist)
    (typically  pass [] for a list {} for a dictionary)
    :return: the dictionary, list of dictionaries or the empty output set.
    """
    import os
    import json
    if not os.path.exists(config_path):
        print("Warning: condor_q_held.json Does not exist")
        return empty
    try:
        with open(config_path) as f_in:
            return json.load(f_in)

    except ValueError:
        print("Warning: json is blank (none held) or bad format")
        return empty





def run_ssh_cmd(cmd,
                un,
                server,
                control_path='$HOME/.ssh/%L-%r@%h:%p',
                cwd=None,
                rsync=False,
                wait_to_finish=True):
    """
    Runs an ssh command.  The Server/ip/pw/un must be stored as a rsa_key to run automatically.
    :param cmd: what ssh command will be ran
    :param un: username of remote server
    :param server: servername / ip /domain.
    :param control_path: specific control path to reconnect to an existing session.
    :param cwd: if needed sets the current working directory. Us None if not needed.
    :param rsync:  Set to true if it is RSYNC as it needs different string
    :param wait_to_finish: Some commands must wait to finish others do not.
    Set to True if you require sequential processing - ex. mkdir then transfer a file
    Set to False if you can operate in parallel processing ex. kick off the submit_job script that loops endlessly.
    :return: subprocess.Popen output.
    """
    import subprocess
    if control_path is None:
        control_path = ''
    if control_path != '':
        # note the leading space is required
        control_path = ' -o ControlPath="{0}"'.format(control_path)
        # there should be a leading space is in the control_path  string
        # ssh -O check -o ControlPath="$HOME/.ssh/%L-%r@%h:%p" ${un}@${submit_node}
        check_ssh_connection_string = 'ssh -O check{0} {1}@{2}'.format(control_path, un, server)
        ##

        ssh_connected_out = subprocess.run([check_ssh_connection_string],
                                           shell=True,
                                           stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE)

        ssh_connected_err = ssh_connected_out.stderr.decode('utf-8').strip()
        ssh_connected = False
        # print('ssh connected reply: {0}'.format(ssh_connected_err))
        if (len(ssh_connected_err) > 0) and (ssh_connected_err[0] in ['C', 'M']):
            ssh_connected = True

        if not ssh_connected:
            ssh_make_connection_string = 'ssh -nNf -o ControlMaster=yes{0} {1}@{2}'.format(control_path, un, server)
            print(ssh_make_connection_string)
            ssh_make_connection_out = subprocess.run([ssh_make_connection_string],
                                                     shell=True,
                                                     stdout=subprocess.PIPE,
                                                     stderr=subprocess.PIPE)
            ##
            print(ssh_make_connection_out)
            print('To manually stop ssh:\n ssh -O stop{0} {1}@{2}'.format(control_path, un, server))

    if (cwd == '.') or (cwd == ''):
        cwd = None
    if rsync:
        ssh_cmd_string = 'rsync -e \'ssh{0}\' {3}'.format(control_path, un, server, cmd)
    else:
        ssh_cmd_string = 'ssh{0} {1}@{2} "{3}"'.format(control_path, un, server, cmd)

    debug_code = False
    if debug_code:
        print(ssh_cmd_string)
    if wait_to_finish:
        ssh_cmd_out = subprocess.run([ssh_cmd_string],
                                     shell=True,
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE,
                                     cwd=cwd)
    else:
        # run a process asynchronously if wait is set to false.
        ssh_cmd_out = subprocess.Popen([ssh_cmd_string], shell=True, stdout=None, stderr=None, close_fds=True)
    return ssh_cmd_out


def close_ssh(un,
              server,
              control_path='$HOME/.ssh/%L-%r@%h:%p'):

    """
    closes the ssh session if it is open at taht control_path
    :param un: username to the remote server
    :param server: server name (or ipaddress /domain)
    :param control_path: control path used for the connection
    :return: nothing.
    """
    import subprocess
    control_path = ' -o ControlPath="{0}"'.format(control_path)
    # there should be a leading space is in the control_path  string
    # ssh -O check -o ControlPath="$HOME/.ssh/%L-%r@%h:%p" ${un}@${submit_node}
    # check connection if it is still connected (or ever connected)
    check_ssh_connection_string = 'ssh -O check{0} {1}@{2}'.format(control_path, un, server)
    ssh_connected_out = subprocess.run([check_ssh_connection_string],
                                       shell=True,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE)
    ssh_connected_err = ssh_connected_out.stderr.decode('utf-8').strip()
    # if connected
    if (len(ssh_connected_err) > 0) and (ssh_connected_err[0] == 'M'):
        ssh_stop_connection_string = 'ssh -O stop{0} {1}@{2}'.format(control_path, un, server)
        ssh_stop_connection_out = subprocess.run([ssh_stop_connection_string],
                                                 shell=True,
                                                 stdout=subprocess.PIPE,
                                                 stderr=subprocess.PIPE)
    return


def get_rsync_file_string(source,
                          dest,
                          flag='-azP',
                          source_from_file_list=False,
                          cwd=None,
                          check_sum_only=False,
                          ignore_errors=False,
                          delete_flag=False):
    """
    Generates the rsync string used by the ssh command
    :param source: source directory
    :param dest: destination directory
    :param source_from_file_list: Allows you to pass a list (return delimited txt filepath) instead of a dir or filepath
    :param cwd: change the current working directory if needed for relative paths.
    :param check_sum_only: Set to true if you only want to check if the files from source/dest match by Checksum.
    (no transfer happens if set to True, just returns false or true if the files match.)
    :param ignore_errors: Ignore typical errors as the set parameter has (in case a source dir or dest does not exist).
    :param delete_flag: Delete after transfer if set to true, you are trusting the rsync to check properly.
    :return: Returns the rsync string command minus the server information
    """
    # source_from_file_list must be relative path from same parent directly.  Use cwd to choose parent directory
    # all parent directories/subdirectories listed in th source will be copied
    # if it is a input list then make a string delimited by space
    if isinstance(source, list):
        source = ' '.join(source)
    if source_from_file_list:
        source = '--files-from={0} .'.format(source)
    additional_flags = ''
    if ignore_errors:
        additional_flags = '{0} --ignore_errors'.format(additional_flags)

    if check_sum_only:
        rsync_string = '-a --dry-run --out-format="%f"{0} --checksum {1} {2} | wc -l |awk \'{{$1=$1}}1\''.format(
            additional_flags,
            source,
            dest)
        return rsync_string, cwd
    if delete_flag:
        additional_flags = '{0} --delete'.format(additional_flags)

    rsync_string = '{0}{1} {2} {3}'.format(flag, additional_flags, source, dest)
    print(rsync_string)
    return rsync_string, cwd


def rsync_files(source,
                un,
                server,
                dest,
                server_is_dest=True,
                control_path='$HOME/.ssh/%L-%r@%h:%p',
                rsync_flag='-azP',
                source_from_file_list=False,
                cwd=None,
                check_sum_only=False,
                ignore_errors=False,
                delete_flag=False):
    """
    this rsync_files is only for remote directory transfer only to local (or local to remote).
    :param source: source directory
    :param un: Remote username
    :param server: remote server or ip address or domain.
    :param dest: destination directory
    :param server_is_dest: True by default, set to false if the local directory is the destination.
    :param control_path: Allows to use the same ssh connecton (with out reopening new ones if it exists).
    :param rsync_flag: Allows to use a custom flag for file transfer (if not zipped or archive etc.)
    :param source_from_file_list: Allows you to pass a list (return delimited txt filepath) instead of a dir or filepath
    :param cwd: change the current working directory if needed for relative paths.
    :param check_sum_only: Set to true if you only want to check if the files from source/dest match by Checksum.
    (no transfer happens if set to True, just returns false or true if the files match.)
    :param ignore_errors: Ignore typical errors as the set parameter has (in case a source dir or dest does not exist).
    :param delete_flag: Delete after transfer if set to true, you are trusting the rsync to check properly.
    :return: Only returns if the check_sum_only is True, else returns an error statement.
    """
    # Add the server and un to the correct isde

    if server_is_dest:
        dest = '{0}@{1}:{2}'.format(un, server, dest)
    else:
        source = '{0}@{1}:{2}'.format(un, server, source)
    # Get the rsync string and the cwd
    rsync_string, cwd = get_rsync_file_string(source=source,
                                              dest=dest,
                                              flag=rsync_flag,
                                              source_from_file_list=source_from_file_list,
                                              cwd=cwd,
                                              check_sum_only=check_sum_only,
                                              ignore_errors=ignore_errors,
                                              delete_flag=delete_flag)
    # run the ssh cmd.
    ssh_cmd_out = run_ssh_cmd(cmd=rsync_string,
                              server=server,
                              un=un,
                              control_path=control_path,
                              rsync=True,
                              cwd=cwd)
    return ssh_cmd_out


def get_configuration_filepaths(submission_config_dir):
    """
    Gets the file paths of .json files in a directory
    """
    import os
    submission_config_list = os.listdir(submission_config_dir)
    submission_config_list = [os.path.join(submission_config_dir, x) for x in submission_config_list if
                              x[-5:] == ".json"]
    return submission_config_list
