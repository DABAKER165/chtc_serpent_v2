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


def get_ssh_control_path(control_path='$HOME/.ssh/%L-%r@%h:%p'):
    from os import environ

    if control_path is None:
        return '', False
    if control_path != '':
        control_dirs = control_path.split('/')
        for dir_i in control_dirs:
            if '$' in dir_i:
                home = environ[dir_i[1:]]
                control_path = control_path.replace(dir_i, home)
        control_path = '-o ControlPath="{0}"'.format(control_path)
    return control_path, True


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
    from subprocess import Popen, PIPE, run as subprocess_run
    control_path, use_control_path = check_reconnect_ssh_control_path(control_path=control_path,
                                                                      un=un,
                                                                      server=server)

    if (cwd == '.') or (cwd == ''):
        cwd = None
    # if rsync:
    #     ssh_cmd_string = 'rsync -e \'ssh{0}\' {3}'.format(control_path, un, server, cmd)
    # else:
    # ssh_cmd_string = 'ssh{0} {1}@{2} "{3}"'.format(control_path, un, server, cmd)
    ssh_connection_list = ['ssh',
                           control_path,
                           '{0}@{1}'.format(un, server),
                           '{0}'.format(cmd)]
    debug_code = False
    if debug_code:
        print(' '.join(ssh_connection_list))
    if wait_to_finish:
        ssh_cmd_out = subprocess_run(ssh_connection_list,
                                     shell=False,
                                     stdout=PIPE,
                                     stderr=PIPE,
                                     cwd=cwd)
    else:
        # run a process asynchronously if wait is set to false.
        ssh_cmd_out = Popen(ssh_connection_list, shell=False, stdout=None, stderr=None, close_fds=True)
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
    from subprocess import PIPE, run as subprocess_run

    control_path, use_control_path = get_ssh_control_path(control_path)
    if not use_control_path:
        return
    ssh_connected = check_ssh_connection(control_path, un, server)
    # if connected
    if ssh_connected:
        ssh_stop_connection_list = ['ssh',
                                    '-O',
                                    'stop',
                                    control_path,
                                    '{1}@{2}'.format(control_path, un, server)]
        ssh_stop_connection_out = subprocess_run(ssh_stop_connection_list,
                                                 shell=False,
                                                 stdout=PIPE,
                                                 stderr=PIPE)
    return


def check_ssh_connection(control_path, un, server):
    from subprocess import PIPE, run as subprocess_run
    check_ssh_connection_list = ['ssh',
                                 '-O',
                                 'check',
                                 control_path,
                                 '{0}@{1}'.format( un, server)]

    ssh_connected_out = subprocess_run(check_ssh_connection_list,
                                       shell=False,
                                       stdout=PIPE,
                                       stderr=PIPE)

    ssh_connected_err = ssh_connected_out.stderr.decode('utf-8').strip()
    ssh_connected = False
    # print('ssh connected reply: {0}'.format(ssh_connected_err))
    if (len(ssh_connected_err) > 0) and (ssh_connected_err[0] in ['C', 'M']):
        ssh_connected = True
        # there should be a leading space is in the control_path  string
    # ssh -O check -o ControlPath="$HOME/.ssh/%L-%r@%h:%p" ${un}@${submit_node}

    # print('ssh connected reply: {0}'.format(ssh_connected_err))
    if (len(ssh_connected_err) > 0) and (ssh_connected_err[0] in ['C', 'M']):
        ssh_connected = True
    return ssh_connected


def check_reconnect_ssh_control_path(control_path='$HOME/.ssh/%L-%r@%h:%p', un=None, server=None):
    from subprocess import PIPE, run as subprocess_run
    control_path, use_control_path = get_ssh_control_path(control_path)
    if not use_control_path:
        return '', False

    ssh_connected = check_ssh_connection(control_path, un, server)

    if not ssh_connected:
        check_ssh_connection_list = ['ssh',
                                     '-nNf',
                                     '-o ControlMaster=yes',
                                     control_path,
                                     '{0}@{1}'.format(un, server)]

        print(' '.join(check_ssh_connection_list))
        ssh_make_connection_out = subprocess_run(check_ssh_connection_list,
                                                 shell=False,
                                                 stdout=PIPE,
                                                 stderr=PIPE)
        ##
        print(ssh_make_connection_out)
        print('To manually stop ssh:\n ssh -O stop{0} {1}@{2}'.format(control_path, un, server))

    return control_path, use_control_path


def rsync_files(source=None,
                un=None,
                server=None,
                dest=None,
                server_is_dest=True,
                remote=True,
                compressed=False,
                control_path='$HOME/.ssh/%L-%r@%h:%p',
                rsync_flag='-ahP',
                source_from_file_list=False,
                cwd='/',
                checksum_only=False,
                ignore_errors=False,
                remove_source_files=False):
    """
   this rsync_files is only for remote directory transfer only to local (or local to remote).
   :param source: <string> source directory
   :param un: <string> Remote username
   :param server: <string> remote server or ip address or domain.
   :param dest: <string> destination directory
   :param server_is_dest: <bool> set to False if the local directory is the destination; True, if server is destination.
   :param remote: <bool> if sending or recieving from remote site, un server is required (not None).
   :param compressed: <bool> compress before sending to save Network but slows I/O
   :param control_path: <string>  Allows to use the same ssh connecton (with out reopening new ones if it exists).
   :param rsync_flag: <string> Allows to use a custom flag for file transfer (if not zipped or archive etc.)
   :param source_from_file_list: <string> Allows you to pass a list in a (return delimited txt filepath) instead of dir
   :param cwd: <string> change the current working directory if needed for relative paths. (source from filelist)
   :param checksum_only: <bool> Set to true if you only want to check if the files from source/dest match by Checksum.
   (no transfer happens if set to True, just returns false or true if the files match.)
   :param ignore_errors: <bool> Ignore typical errors, but not all rsync versions support it so do not use.
   :param remove_source_files: <bool> delete source files after transfer
   :return: , Returns dictionary:
    {'error': <bool>, if True if error while running rsync, false if not.
                   'checksum_passed': <bool>, True if checksum passed.
                   'checksum_failed_count': <int> of file count 0 if none failed, -1 if not running checksum only
                   Prints to terminal: Parsed Std out to the terminal (or stderr it there was an error needed)
   """
    from subprocess import Popen, PIPE

    if (source is None) or (dest is None):
        print('source or dest not declared')
        return {'error': True,
                'checksum_passed': False,
                'checksum_failed_count': 1}

    run_list = ['rsync']
    use_control_path = False
    if remote and (un is not None) and (server is not None):
        control_path, use_control_path = check_reconnect_ssh_control_path(control_path=control_path,
                                                                          un=un,
                                                                          server=server)
        if server_is_dest:
            dest = '{0}@{1}:{2}'.format(un, server, dest)
        else:
            source = '{0}@{1}:{2}'.format(un, server, source)
        # Get the rsync string and the cwd
        if use_control_path:
            run_list.append('-e')
            run_list.append('ssh {0}'.format(control_path))
    if compressed:
        run_list.append('{0}z'.format(rsync_flag))
    else:
        run_list.append(rsync_flag)

    if ignore_errors:
        run_list.append('--ignore_errors')
    if remove_source_files:
        run_list.append('--remove-source-files')
    if source_from_file_list:
        if cwd != '.':
            # files cannot have same name
            run_list.append('--no-R')
        run_list.append('--files-from={0}'.format(source))
        run_list.append(cwd)

    else:
        run_list.append(source)

    run_list.append(dest)

    result_dict = {'error': False,
                   'checksum_passed': True,
                   'checksum_failed_count': 0}
    if checksum_only:
        run_list = ['rsync']
        addl_list = ['-ro',
                     '--dry-run',
                     '--out-format="%f"',
                     '--checksum',
                     source,
                     dest]
        if use_control_path:
            run_list.append('-e')
            run_list.append('ssh {0}'.format(control_path))
        run_list = run_list + addl_list
        #     p1 = subprocess.Popen(run_list, stdout=PIPE,stderr=PIPE, bufsize=1, universal_newlines=True)
        check_error = False
        line_count = 0
        checksum_passed = True
        file_count = 0
        print(' '.join(run_list))
        with Popen(run_list, stdout=PIPE, stderr=PIPE, bufsize=1, universal_newlines=True) as p1:
            for line in iter(p1.stderr.readline, b''):
                print(line)
                if len(line) == 0:
                    break
                check_error = True
                checksum_passed = False
            for line in iter(p1.stdout.readline, b''):
                if len(line) == 0:
                    break
                print('stdout', line.strip())
                checksum_passed = False
                file_count += 1
        result_dict['checksum_passed'] = checksum_passed
        result_dict['checksum_failed_count'] = file_count
        result_dict['error'] = check_error

        print('checksum_passed: {0}'.format(checksum_passed))
        print('file_count: {0}'.format(file_count))
        print('Error: {0}'.format(check_error))
        return result_dict
    else:
        firstline = True
        items2 = []
        is_error = False
        printed_first_line = False
        print(' '.join(run_list))
        with Popen(run_list, stdout=PIPE, stderr=PIPE, bufsize=1, universal_newlines=True) as p:
            for line in iter(p.stdout.readline, b''):
                if len(line) == 0:
                    if printed_first_line:
                        print(" ".join(items))
                    break
                # print(err)
                items = line.strip().split()
                # Save over only if it has a % which is a status update on the transfer.
                if len(items) > 1 and len(items2) > 1 and items2[1].endswith('%') and items[1].endswith('%'):
                    print(" ".join(tuple(filter(None, items2))), end='\x1b\r')
                elif len(items2) > 0:
                    print(" ".join(items2))
                # Print the first line if it exists else save the line to check if it is a print over update line
                if firstline:
                    firstline = False
                    print(" ".join(items))
                else:
                    items2 = items
                    printed_first_line = True

            # Get any error messages and final lines (final lines typically blank) by.
            stout, sterr = p.communicate()
            if len(stout.strip()) > 0:
                print(stout.strip())
            # print error if it exists and save as error
            if len(sterr) > 0:
                print(sterr)
                result_dict['error'] = True
                result_dict['checksum_passed'] = False
                result_dict['checksum_failed_count'] = -1
                return result_dict
        return result_dict


def get_configuration_filepaths(submission_config_dir):
    """
    Gets the file paths of .json files in a directory
    """
    import os
    submission_config_list = os.listdir(submission_config_dir)
    submission_config_list = [os.path.join(submission_config_dir, x) for x in submission_config_list if
                              x[-5:] == ".json"]
    return submission_config_list
