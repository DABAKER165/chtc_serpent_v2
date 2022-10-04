from .serpent_config import(SerpentOperations)
from .utils import (keys_exists,
                    read_json)


class SerpentCHTCOperations(SerpentOperations):
    """
    Main Class for the Remote HTC operations.
    """
    def __init__(self, config, ignore_mods):
        self.config = config
        self.ignore_mods = ignore_mods
        super().__init__(self.config, self.ignore_mods)


    def get_sample_list_from_record(self, record_json, remove_held=False):
        appended_sample_set = set()
        for record in record_json:
            sample_set_i = set(record['sample_list'])
            appended_sample_set = appended_sample_set | sample_set_i
        return list(appended_sample_set)

    def filter_record_by_kvp(self, record_json, kvp_dict):
        filtered_json = []
        for record in record_json:
            add_record = True
            if not kvp_dict:
                add_record = False
            for key, item in kvp_dict.items():
                if record[key] != item:
                    add_record = False
                    break
            if add_record:
                filtered_json.append(record)
        return filtered_json

    def completed_to_json(self, mod, server):
        from pathlib import Path
        import os
        submitted_record_json = read_json(self.get_unique_path(mod, server, 'submitted_json_path'), empty=[])
        completed_record_json = read_json(self.get_unique_path(mod, server, 'completed_json_path'), empty=[])
        submit_name = self.get_submit_name()
        filtered_submitted_record_json = self.filter_record_by_kvp(record_json=submitted_record_json,
                                                                   kvp_dict={'submission_name': submit_name,
                                                                             'module_name': mod})
        filtered_completed_record_json = self.filter_record_by_kvp(record_json=completed_record_json,
                                                                   kvp_dict={'submission_name': submit_name,
                                                                             'module_name': mod})
        completed_iwd_list = []
        completed_json = []
        held_samples = []
        for record in filtered_completed_record_json:
            completed_iwd_list.append(record['iwd'])
        print(completed_iwd_list)
        if len(completed_iwd_list) > 0:
            Path(self.get_unique_path(mod, server, 'exe_ready_file_path')).touch()
        for submitted_record in filtered_submitted_record_json:
            iwd_i = submitted_record['iwd']
            print(iwd_i)
            if iwd_i not in completed_iwd_list:
                condor_q_held_path = self.get_unique_path(mod, server, 'condor_q_held_path')
                job_completed, held_samples, remain, total = self.job_status(condor_q_held_path=condor_q_held_path,
                                                                             attribute='Iwd',
                                                                             value=iwd_i)
                print(job_completed, held_samples, remain, total)
                if job_completed:
                    sample_sheet_path = os.path.join(iwd_i, 'sample_sheet.txt')
                    # record as completed in the json file.
                    iwd_results_dir = os.path.join(self.get_unique_path(mod, server, 'module_out_dir'),
                                                   os.path.basename(iwd_i))
                    completed_json_path = self.get_unique_path(mod,
                                                               server,
                                                               'completed_json_path')
                    completed_json = self.record_as_json(iwd=iwd_i,
                                                         held_samples=held_samples,
                                                         json_path=completed_json_path,
                                                         submission_dir=self.get_unique_path(mod,
                                                                                             server,
                                                                                             'submit_dir'),
                                                         mod_submit_dir=self.get_unique_path(mod,
                                                                                             server,
                                                                                             'module_working_dir'),
                                                         sample_dir=self.get_unique_path(mod,
                                                                                         server,
                                                                                         'module_in_dir'),
                                                         submission_name=self.get_submit_name(),
                                                         module_name=mod,
                                                         iwd_results_dir=iwd_results_dir,
                                                         sample_sheet_path=sample_sheet_path)
        return completed_json, held_samples

    def check_completion(self, q_json, iwd, attribute='TransferInput'):
        import os
        log_files = os.listdir(os.path.join(iwd, 'logs'))
        print(os.path.join(iwd, 'logs'))
        print(log_files)
        returned_log_files = [x for x in log_files if x[-7:] in ['err.txt', 'out.txt']]
        submitted_files = [x for x in log_files if x[-7:] == 'log.txt']
        log_file_count = len(returned_log_files)
        submitted_count = len(submitted_files)
        # set to zero if you want to freeze the continuation until after the held jobs run
        # resubmit must catch up then the count in the log files (some how?)
        held_list = []
        for held_i in q_json:
            if attribute in held_i:
                if 'Iwd' in held_i and held_i['Iwd'] == iwd:
                    attribute_list = held_i[attribute].split(',')
                    filepath = attribute_list[len(attribute_list) - 1]
                    held_list.append(filepath)
        if submitted_count < 1:
            print('empty')
            return False, held_list, 0, 0

        held_count = len(held_list)
        remaining_count = submitted_count - held_count - (log_file_count / 2)
        print('counts', submitted_count, held_count, log_file_count, remaining_count)
        return remaining_count == 0, held_list, remaining_count, submitted_count

    def chtc_held_filter_attribute(self, q_json, attribute='Iwd', value=None):
        # Loop through the array and pull out where cluster_id = ClusterID
        q_json_cid = []
        for held_i in q_json:
            if attribute in held_i and value == held_i[attribute]:
                q_json_cid.append(held_i)
        return q_json_cid

    def read_chtc_json(self, condor_q_held_path, attribute='Iwd', value=None):

        # read the json file
        q_json = read_json(condor_q_held_path, empty=[])
        # return the array of dictionaries with all
        # return True if there is a held  job as second tupple
        if value is None:
            return q_json, True if len(q_json) > 0 else False
        # return filtered q_json by cluster id
        q_json_cid = self.chtc_held_filter_attribute(q_json,
                                                     attribute=attribute,
                                                     value=value)
        # return True if there is a held  job as second tupple
        return q_json_cid, True if len(q_json) > 0 else False

    def get_chtc_json(self, condor_q_held_path, attribute='Iwd', value=None):
        import subprocess
        import os
        import time

        # the string that subprocess executes to form a json file
        get_json = 'condor_q -held -json -attributes ClusterID,ProcId,Iwd,TransferInput >> {0}'.format(
            condor_q_held_path)
        # remove any existing files to not have a odd append/conflict/locked file situation.
        if os.path.exists(condor_q_held_path):
            os.remove(condor_q_held_path)
        # run the process
        subprocess.run(args=[get_json], shell=True)
        # sometimes condor q errors, try again 5 times (i <5) for 3 + i seconds waiting.
        i = 0
        while not (os.path.exists(condor_q_held_path)) and (i < 5):
            time.sleep(3 + i)
            subprocess.run(args=[get_json], shell=True)
            i += 1
        # q_json,held jobs
        return self.read_chtc_json(condor_q_held_path, attribute=attribute, value=value)

    def job_status(self, condor_q_held_path='condor_q_held.json', attribute='Iwd', value=None):
        # get the chtc_json and create the file and read the file, and filter for the iwd path (submission folder).
        q_json, held_jobs_flag = self.get_chtc_json(condor_q_held_path=condor_q_held_path,
                                                    attribute=attribute,
                                                    value=value)
        return self.check_completion(q_json, iwd=value)

    def get_submitted_completed_sample_lists(self, mod, server):
        submitted_record_json = read_json(self.get_unique_path(mod, server, 'submitted_json_path'), empty=[])
        completed_record_json = read_json(self.get_unique_path(mod, server, 'completed_json_path'), empty=[])

        submit_name = self.get_submit_name()
        filtered_submitted_record_json = self.filter_record_by_kvp(record_json=submitted_record_json,
                                                                   kvp_dict={'submission_name': submit_name,
                                                                             'module_name': mod})
        filtered_completed_record_json = self.filter_record_by_kvp(record_json=completed_record_json,
                                                                   kvp_dict={'submission_name': submit_name,
                                                                             'module_name': mod})
        submitted_sample_list = self.get_sample_list_from_record(record_json=filtered_submitted_record_json)
        completed_sample_list = self.get_sample_list_from_record(record_json=filtered_completed_record_json)
        return submitted_sample_list, completed_sample_list

    def record_as_json(self,
                       iwd,
                       json_path,
                       submission_dir,
                       mod_submit_dir,
                       sample_dir,
                       submission_name,
                       module_name,
                       sample_sheet_path="",
                       iwd_results_dir="",
                       held_samples=[],
                       sample_list=None):
        import shutil
        from datetime import datetime
        import os
        import json
        if sample_list is None:
            if os.path.exists(sample_sheet_path):
                sample_list = open(sample_sheet_path).read().splitlines()
            else:
                print('Warning: missing sample list and sample_sheet path does not exist')
        # backup file
        now = datetime.now()
        dt_string = now.strftime("%Y_%m_%d__%H_%M_%S")
        completed_backup_filepath = '{0}_{1}{2}'.format(json_path[:-5], dt_string, '.json')
        completed_json = read_json(json_path, empty=[])
        add_record = {}
        add_record['iwd'] = iwd
        add_record['submission_dir'] = submission_dir
        add_record['mod_submit_dir'] = mod_submit_dir
        add_record['sample_list'] = sample_list
        add_record['held_samples'] = held_samples
        add_record['sample_dir'] = sample_dir
        add_record['submission_name'] = submission_name
        add_record['module_name'] = module_name
        add_record['iwd_results_dir'] = iwd_results_dir
        completed_json.append(add_record)
        if os.path.exists(json_path):
            shutil.copyfile(json_path, completed_backup_filepath)
            os.remove(json_path)
        with open(json_path, 'w') as f:
            json.dump(completed_json, f)
        return completed_json

    def make_chtc_wrapper_script(self,
                                 source_filepath_list,
                                 results_dir,
                                 executable,
                                 arguments_dict,
                                 submit_dir,
                                 sample_dir='fake_dest_zxs',
                                 chtc_wrapper='chtc_wrapper.sh'):
        import os
        copy_files_to_node_string_list = ['cp {0} ./'.format(x) for x in source_filepath_list]
        copy_files_to_node_string = '\n '.join(copy_files_to_node_string_list)
        arguments_string = ''
        for key, value in arguments_dict.items():
            arguments_string = '{0} {1} {2}'.format(arguments_string, key, value)
        # start with the options and bin bash statement
        script_string = '#! /bin/bash \n\
\n\
t=None\n\
r=None\n\
while getopts s:t:r: opt; do \n\
case $opt in \n\
s) s=$OPTARG;; \n\
t) t=$OPTARG;; \n\
r) r=$OPTARG;; \n\
* ) usage; \n\
exit \n\
1;; \n\
esac \n\
done \n\
\n'
        # Add the copy files string that is enter delimited
        script_string = '{0} {1} \n'.format(script_string, copy_files_to_node_string)
        # cmod _x files
        script_string = '{0} chmod +x ./{1} \n'.format(script_string, executable)
        # add the executable + the argument.  the executable will be in the same folder
        script_string = '{0} ./{1} {2} \n'.format(script_string, executable, arguments_string)

        script_string = '{0} if [[ ${{s: -7}} == ".tar.gz" ]] ; then \n\
    filename=${{s%???????}}_out.tar.gz \n\
else \n\
    filename="${{s%.*}}_out.tar.gz" \n\
fi \n\
rm -f ${{s}} \n\
rm -f ${{filename}} \n\
if [[ ${{t}} == "t" ]] ; then \n\
    find . -maxdepth 1 -type f \( ! -name "_condor_stderr" -a ! -name "_condor_stdout" -a ! -name "docker_stderror" -a ! -name "tarlist.txt" \) -print | sed "s|^\\./||"  > tarlist.txt \n\
    tar -czvf ${{filename}} -T tarlist.txt \n\
    for f in $(cat tarlist.txt) ; do \n\
        rm -f "$f" \n\
    done \n\
    rm -f tarlist.txt \n\
fi \n\
find . -maxdepth 1 -type f \( ! -name "_condor_stderr" -a ! -name "_condor_stdout" -a ! -name "docker_stderror" -a ! -name "check.md5" \) -exec md5sum "{{}}" + > check.md5 \n\
sed \'s/^.*\\( .*\\).*$/\\1/\' check.md5 > check_list.txt \n\
perl -pi -w -e "s:./:{1}/:g" check.md5 \n\
cp -f $(<check_list.txt) {1} \n\
if md5sum --status -c check.md5; then \n\
    echo "MD5 PASSED: remove sample file" \n\
    if [[ ${{r}} == "t" ]] ; then \n\
        # rm -f {2}/${{s}} \n\
        echo {2}/${{s}} \n\
        echo "removed sample" \n\
    fi \n\
else \n\
    echo "The_MD5_sum_did_not_match" \n\
fi \n\
rm -f check.md5 \n\
rm check_list.txt\n'.format(script_string, results_dir, sample_dir)

        # delete the stuff we don't need.
        script_string = '{0} find . -type f -maxdepth 1 \\( ! -name "_condor_stderr" -a ! -name "_condor_stdout" -a ! -name "docker_stderror" \\) -delete'.format(
            script_string)
        with open(os.path.join(submit_dir, chtc_wrapper), "w") as f:
            f.write(script_string)
        return

    def make_submission_file(self,
                             filepath,
                             chtc_mod_config,
                             samples_per_row,
                             sample_sheet_path):
        import os

        required_key_list = ['ram',
                             'cpus',
                             'machine_requirements',
                             'disk_space',
                             'executable']
        docker_key = 'docker_image'
        optional_key_list = ['priority_flag']
        gpu_keys_set = {'gpu_count',
                        'gpu_job_length'}

        submit_string = "# logs\n\
Error = logs/$(Cluster).$(Process).err.txt\n\
Output = logs/$(Cluster).$(Process).out.txt\n\
Log = logs/$(Cluster).$(Process).log.txt\n\
\n\
# machine specs\n\
requirements = ^MACHINE_REQUIREMENTS^ \n\
request_cpus = ^CPUS^ \n\
request_memory = ^RAM^ \n\
request_disk = ^DISK_SPACE^ \n\
^OPTIONAL_FLAGS^ \n\
\n\
# shell script to run which is a wrapper to handle file transfer to staging\n\
executable = chtc_wrapper.sh \n\
\n\
# arguments to shell script\n\
# arguments are in the chtc_wrapper.sh\n\
Arguments = -s $(s)^arg_s^\n\
\n\
# file transfer options\n\
should_transfer_files = YES \n\
when_to_transfer_output = ON_EXIT \n\
\n\
# Make sure your reference files are here, and you change the folder path for DHO_experiment\n\
transfer_input_files = ^EXECUTABLE^,chtc_wrapper.sh  \n\
# described here: https://chtc.cs.wisc.edu/multiple-jobs.shtml\n\
queue ^sample_string^ from ^SAMPLE_SHEET_NAME^"

        if samples_per_row < 2:
            submit_string = submit_string.replace('^sample_string^', 's')
        if samples_per_row == 2:
            submit_string = submit_string.replace('^sample_string^', 's, r')
            submit_string = submit_string.replace('^arg_s^', ' -r $(r)')

        if samples_per_row > 2:
            submit_string = submit_string.replace('^sample_string^', 's, r, t')
            submit_string = submit_string.replace('^arg_s^', ' -r $(r) -t ${t}')
        for key_i in required_key_list:
            placeholder_string = '^{0}^'.format(key_i.upper())
            replace_string = chtc_mod_config[key_i]
            submit_string = submit_string.replace(placeholder_string, replace_string)
        optional_string = ''
        optional_string_list = []
        for key_i in optional_key_list:
            if key_i in chtc_mod_config.keys():
                optional_string_list.append(chtc_mod_config[key_i])
        if docker_key in chtc_mod_config.keys():
            optional_string_list.append('universe = docker')
            optional_string_list.append('docker_image = {0}'.format(chtc_mod_config[docker_key]))
        else:
            optional_string_list.append('universe = vanilla')
        if gpu_keys_set.issubset(chtc_mod_config.keys()):
            optional_string_list.append('request_gpus = {0}'.format(chtc_mod_config['gpu_count']))
            optional_string_list.append('+WantGPULab = true')
            optional_string_list.append('+GPUJobLength = \"{0}\"'.format(chtc_mod_config['gpu_job_length']))

        if len(optional_string_list) > 0:
            optional_string = '\n'.join(optional_string_list)
        submit_string = submit_string.replace('^OPTIONAL_FLAGS^', optional_string)
        submit_string = submit_string.replace('^SAMPLE_SHEET_NAME^', sample_sheet_path)

        # remove if it exits
        if os.path.exists(filepath):
            os.remove(filepath)
        # print string as a file
        with open(filepath, "w") as f:
            f.write(submit_string)
        return filepath

    def chtc_create_and_run_submit_file(self,
                                        mod,
                                        server='chtc',
                                        submitted_sample_list=[]
                                        ):
        # submission_name, # dont need (get path)
        # chtc_mod_config, # dont need
        # submission_dir, # dont need
        # mod_submit_dir, # dont need
        # file needs parsing
        # mod_executable_path, # dont need
        # staging_results_dir, # dont need
        # static_file_dir, # dont need
        # sample_dir=None
        # make sure logs folder exits
        # make samplesheet based on submitted and completed
        # launch the submission file
        # Create the submission file
        # record submission in a json file with the samplesheet contents.
        import os
        from datetime import datetime
        import shutil
        import subprocess
        # Get the samples and see what is new
        sample_extension = keys_exists(self.config, alt=None, keys=[mod, server, 'sample_extension'])[1]
        sample_list = os.listdir(self.get_unique_path(mod, server, 'module_in_dir'))
        sample_list = [x for x in sample_list if x[0] != "."]
        if sample_extension is not None:
            sample_list = self.get_sample_list_from_extension(sample_list=sample_list,
                                                sample_dir=self.get_unique_path(mod, server, 'module_in_dir'),
                                                extension=sample_extension,
                                                              return_basenames=True)


        new_sample_list = list(set(sample_list) - set(submitted_sample_list))


        # Finished Get the samples and see what is new
        # If nothing is new then quit
        if len(new_sample_list) < 1:
            print("No New Samples")
            return
        samples_per_row = int(new_sample_list[0].count(",")) + 1
        # Create submission folder
        now = datetime.now()

        dt_string = now.strftime("%Y_%m_%d__%H_%M_%S")
        executable_dir = os.path.join(self.get_unique_path(mod, server, 'module_working_dir'),
                                      '{0}_{1}'.format(mod, dt_string))
        os.makedirs(executable_dir, exist_ok=True)
        executable_filename = os.path.basename(self.get_unique_path(mod, server, 'mod_executable_path'))
        shutil.copyfile(self.get_unique_path(mod, server, 'mod_executable_path'), os.path.join(executable_dir,
                                                                                               executable_filename))
        # Finished create submission folder

        # if there is something new make a sample sheet
        sample_sheet_path = os.path.join(executable_dir, 'sample_sheet.txt')
        if os.path.exists(sample_sheet_path):
            os.remove(sample_sheet_path)
        # make the sample sheet
        with open(sample_sheet_path, 'w') as f:
            f.writelines("%s\n" % x for x in new_sample_list)
        # submit file path as <mod>.sub in the copied direcotry
        submit_filepath = os.path.join(executable_dir, '{0}.sub'.format(mod))
        submit_filename = '{0}.sub'.format(mod)
        # make logs folder just in case it was forgotten.
        os.makedirs(os.path.join(executable_dir, 'logs'), exist_ok=True)
        # make submission_file
        self.make_submission_file(filepath=submit_filepath,
                                  chtc_mod_config=self.config[mod][server],
                                  samples_per_row=samples_per_row,
                                  sample_sheet_path=sample_sheet_path)
        # copy the directory for a spot for the results to go

        arguments_dict = {}
        arguments_dict = keys_exists(self.config, alt={}, keys=[mod, server, 'arguments'])[1]
        source_filepath_list = []
        # if sample_dir is not None:
        if samples_per_row > 0:
            source_filepath_list.append(os.path.join(self.get_unique_path(mod, server, 'module_in_dir'),
                                                     '${s}'))
        if samples_per_row > 1:
            source_filepath_list.append(os.path.join(self.get_unique_path(mod, server, 'module_in_dir'),
                                                     '${r}'))
        if samples_per_row > 2:
            source_filepath_list.append(os.path.join(self.get_unique_path(mod, server, 'module_in_dir'),
                                                     '${t}'))
        if keys_exists(self.config, keys=[mod, server, 'static_files'])[0]:
            # Allows the file input to be a list, dictionary or string that is split by ','
            static_files_list = self.config[mod][server]['static_files']
            if isinstance(static_files_list, str):
                static_files_list = static_files_list.split(',')
            if isinstance(static_files_list, dict):
                static_files_list = list(set(static_files_list.values()))

            for static_file in static_files_list:
                source_filepath_list.append(os.path.join(self.get_unique_path(mod, server, 'static_dir'),
                                                         os.path.basename(static_file)))
        exe_results_dir = os.path.join(self.get_unique_path(mod, server, 'module_out_dir'),
                                       os.path.basename(executable_dir))
        self.make_chtc_wrapper_script(source_filepath_list=source_filepath_list,
                                      results_dir=exe_results_dir,  # need,
                                      sample_dir=self.get_unique_path(mod, server, 'module_in_dir'),  # remove
                                      executable=executable_filename,
                                      arguments_dict=arguments_dict,
                                      submit_dir=executable_dir,
                                      chtc_wrapper='chtc_wrapper.sh')
        os.makedirs(exe_results_dir,
                    exist_ok=True)
        # shutil.copytree(executable_dir, os.path.join(staging_results_dir,
        #                                              os.path.basename(executable_dir)))
        print('Submit file path: {0}'.format(submit_filepath))
        subprocess.call(['condor_submit {0}'.format(submit_filename)],
                        shell=True,
                        cwd=executable_dir)

        self.record_as_json(iwd=executable_dir,
                            json_path=self.get_unique_path(mod, server, 'submitted_json_path'),
                            submission_dir=self.get_unique_path(mod, server, 'module_working_dir'),
                            mod_submit_dir=self.get_unique_path(mod, server, 'working_dir'),
                            sample_list=new_sample_list,
                            sample_dir=self.get_unique_path(mod, server, 'module_in_dir'),
                            submission_name=self.get_submit_name(),
                            module_name=mod)

        return
    def get_sample_list_from_extension(self, sample_list, sample_dir, extension, return_basenames=False):
        import os

        def trim_extensions(x, extension_list = []):
            for extension_i in extension_list:
                if len(extension_i) < 1:
                    continue
                if x.endswith(extension_i):
                    ext_len = -len(extension_i)
                    return x[:ext_len]
            return x

        if type(extension) == str:
            extension = [extension]
        sample_filenames = [x for x in sample_list if x.endswith(tuple(extension)) and (not x.startswith('._'))]
        sample_list = sample_filenames
        if not return_basenames:
            sample_list = [os.path.join(sample_dir, x) for x in sample_filenames]

        sample_group_list = [trim_extensions(x, extension_list=extension) for x in sample_filenames]
        sample_group_set = set(sample_group_list)
        if len(sample_group_set) != len(sample_group_list):
            sample_path_dict = {}
            # Group into a dictionary the paths by the sample
            for sample_path_i, sample_group_i in zip(sample_list, sample_group_list):
                if sample_group_i not in sample_path_dict.keys():
                    sample_path_dict[sample_group_i] = [sample_path_i]
                else:
                    temp_list = sample_path_dict[sample_group_i]
                    temp_list.append(sample_path_i)
                    sample_path_dict[sample_group_i] = temp_list
            # remake the sample list as a csv style without headers
            sample_list = []
            for sample_group_i, path_list_i in sample_path_dict.items():
                path_list_i.sort(reverse=True)
                sample_str_list = ','.join(path_list_i)
                sample_list.append(sample_str_list)

        return sample_list
    #########
    # Probably should go in a different class by leave here for now.
    def make_sample_list(self,
                         sample_dir=None,
                         execution_dir=None,
                         sample_list_path=None,
                         submitted_sample_path=None,
                         make_list='T',
                         extension=None):
        import os
        from datetime import datetime


        now = datetime.now()
        sample_list = []
        submitted_sample_list = []
        dt_string = now.strftime("%Y_%m_%d__%H_%M_%S")
        if (sample_list_path is None) and (make_list[0] == 'F'):
            sample_list_path = os.path.join(execution_dir, 'sample_list_{0}.txt'.format(dt_string))
            sample_list = ['placeholder']
            if os.path.exists(sample_list_path):
                os.remove(sample_list_path)
            with open(sample_list_path, "w") as f:
                f.writelines("%s\n" % l for l in sample_list)
            return sample_list_path

        if sample_list_path is None:
            sample_list_path = os.path.join(execution_dir, 'sample_list_{0}.txt'.format(dt_string))
        if (make_list is None) or (make_list[0].upper() != 'F'):
            sample_list = os.listdir(sample_dir)
            if extension is None:
                sample_list = [os.path.join(sample_dir, x) for x in sample_list]
            elif extension == 'directory':
                sample_list = [os.path.join(sample_dir, x) for x in sample_list if
                               os.path.isdir(os.path.join(sample_dir, x))]
            elif extension == 'parent_directory':
                sample_list = [sample_dir]
            else:
                self.get_sample_list_from_extension(sample_list=sample_list,
                                                    sample_dir=sample_dir,
                                                    extension=extension)
        else:
            if os.path.isfile(sample_list_path) and (os.stat(sample_list_path).st_size != 0):
                sample_list = []
                with open(sample_list_path, "r") as f:
                    for line in f:
                        stripped_line = line.strip()
                        if len(stripped_line) > 0:
                            sample_list.append(stripped_line)

        if (submitted_sample_path is not None) and (os.path.isfile(submitted_sample_path)) and (
                os.stat(submitted_sample_path).st_size != 0):

            with open(submitted_sample_path, "r") as f:
                for line in f:
                    stripped_line = line.strip()
                    if len(stripped_line) > 0:
                        submitted_sample_list.append(stripped_line)
        sample_list = list(set(sample_list) - set(submitted_sample_list))
        sample_list.sort()
        # df = pd.DataFrame({"SAMPLES": sample_list})
        if os.path.exists(sample_list_path):
            os.remove(sample_list_path)
        with open(sample_list_path, "w") as f:
            f.writelines("%s\n" % l for l in sample_list)
        return sample_list_path

    def make_sample_list_wrapper(self,
                                 mod,
                                 server):

        print('make_sample_list_wrapper')

        sample_list_path = keys_exists(self.config, alt=None, keys=[mod, server, 'sample_list_path'])[1]
        make_list = keys_exists(self.config, alt='T', keys=[mod, server, 'make_list'])[1]
        sample_extension = keys_exists(self.config, alt=None, keys=[mod, server, 'sample_extension'])[1]
        submitted_sample_list_path = self.get_unique_path(mod,
                                                          server,
                                                          'submitted_sample_list_path')
        sample_list_path = self.make_sample_list(sample_dir=self.get_unique_path(mod, server, 'module_in_dir'),
                                                 execution_dir=self.get_unique_path(mod, server, 'module_working_dir'),
                                                 sample_list_path=sample_list_path,
                                                 submitted_sample_path=submitted_sample_list_path,
                                                 make_list=make_list,
                                                 extension=sample_extension)
        return sample_list_path

    def make_remote_executable_command(self,
                                       mod,
                                       server,
                                       incoming_ready=True,
                                       incoming_complete=True,
                                       sample_list_path=None
                                       ):
        # use_snakemake = "T"
        # docker_image
        # gpu_count
        # mount_list
        import os
        arguments = keys_exists(self.config, alt={}, keys=[mod, server, 'arguments'])[1]
        executable_path = self.get_unique_path(mod, server, 'mod_executable_path')
        executable = os.path.basename(executable_path)
        executable_dir = os.path.dirname(executable_path)
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
        use_sm = keys_exists(self.config, [mod, server, 'use_snakemake'])[0]
        if keys_exists(self.config, [mod, server, 'use_snakemake'])[0]:
            use_sm = self.config[mod][server]['use_snakemake'][0].upper() == 'T'
        # use_sm = ('sample_dir' in local_mod_config['paths'].keys()) and use_sm
        if use_sm:
            submit_dir = self.get_unique_path(mod, server, 'module_working_dir')
            # sample_dir = self.get_unique_path(mod, server, 'module_in_dir')
            cores_string = '--cores {0} '.format(keys_exists(self.config, alt='1', keys=[mod, server, 'cores'])[1])
            cpus = keys_exists(self.config, alt='1', keys=[mod, server, 'cpus'])[0]
            sm_path = os.path.join(submit_dir,
                                   'sm_wrapper.sm')
            sm_cmd = "snakemake --snakefile {0} {1}--set-threads wrap_function={2}".format(sm_path,
                                                                                           cores_string,
                                                                                           cpus)
            sm_cmd = "{0} --config sample_list_path=\"{1}\"".format(sm_cmd,
                                                                    sample_list_path)

            self.make_snakemake_file_wrappers(submit_dir=submit_dir,
                                              cpus=cpus)
            self.make_snakemake_sh_wrapper(executable=executable_cmd,
                                           arguments_string=arguments_string,
                                           submit_dir=submit_dir)
            executable_cmd = sm_cmd
            arguments_string = ''
            # & > SomeFile.txt
        if keys_exists(self.config, [mod, server, 'docker_image'])[0]:
            docker_image = self.config[mod][server]['docker_image']

            gpu_count = keys_exists(self.config, alt='0', keys=[mod, server, 'gpu_count'])[1]
            gpu_tag = ''
            # needs to use more gpu stuff but for now only all is working
            if int(gpu_count) > 0:
                gpu_tag = '--gpus all'
            docker_string = 'docker run {0}'.format(gpu_tag)
            docker_tags = keys_exists(self.config, alt='', keys=[mod, server, 'docker_tags'])[1]
            docker_string = '{0} {1}'.format(docker_string, docker_tags)
            mount_dir_list = keys_exists(self.config, alt=[], keys=[mod, server, 'mount_list'])[1]
            for mount_dir in mount_dir_list:
                docker_string = '{0} -v {1}'.format(docker_string, mount_dir)
            docker_string = '{0} {1} {2}{3}'.format(docker_string,
                                                    docker_image,
                                                    executable_cmd,
                                                    arguments_string)
            print(docker_string)
            return docker_string
        executable_cmd = 'chmod +x {0} && {1}'.format(self.get_unique_path(mod, server, 'mod_executable_path'),
                                                      executable_cmd)
        return '{0}{1}'.format(executable_cmd, arguments_string)

    def make_snakemake_file_wrappers(self,
                                     submit_dir,
                                     cpus=1,
                                     sm_file="sm_wrapper.sm"):
        import os
        # executable_string = '{0} -s {{sample}}'.format(os.path.join(submission_dir, 'sm_wrapper.sh'))
        # sample_dir = os.path.dirname(config['sample_dir'])\n\

        # sample_list = os.listdir(sample_dir)\n\
        snakemake_string = "import os\n\
sample_list_path = config['sample_list_path']\n\
sample_list = []\n\
with open(sample_list_path, \"r\") as f:\n\
\tfor line in f:\n\
\t\tline=line.strip()\n\
\t\tsample_list.append(line)\n\
sample_list.sort()\n\
if len(sample_list) > 0:\n\
\tsample_names = [os.path.basename(x) for x in sample_list]\n\
\troot_dir1 = os.path.dirname(sample_list[0])\n\
rule all:\n\
\tinput:\n\
\t\texpand(\"{{sample}}.txt\", sample=sample_names)\n\
rule wrap_function:\n\
\tinput:\n\
\t\tsamp=root_dir1+\"/{{sample}}\"\n\
\toutput:\n\
\t\t\"{{sample}}.txt\"\n\
\tthreads:\n\
\t\t{0}\n\
\tshell:\n\
\t\t\"chmod +x {1} && \"\n\
\t\t\"{1} -s {{input.samp}} && \"\n\
\t\t\"touch {{output[0]}}\"".format(cpus, os.path.join(submit_dir, 'sm_wrapper.sh'))
        # snakemake_string = snakemake_string.replace('^executable_string^', executable_string)
        with open(os.path.join(submit_dir, sm_file), "w") as f:
            f.write(snakemake_string)
        return

    def make_snakemake_sh_wrapper(self,
                                  executable,
                                  arguments_string,
                                  submit_dir,
                                  sm_wrapper='sm_wrapper.sh'):
        import os
        script_string = '#! /bin/bash \n\
\n\
while getopts s: opt; do \n\
case $opt in \n\
s) s=$OPTARG;; \n\
* ) usage; \n\
exit \n\
1;; \n\
esac \n\
done \n\
\n'
        # cmod _x files
        script_string = '{0} chmod +x {1} \n'.format(script_string, os.path.join(submit_dir, executable))
        # add the executable + the argument.  the executable will be in the same folder
        script_string = '{0} {1} {2} \n'.format(script_string, os.path.join(submit_dir, executable), arguments_string)
        with open(os.path.join(submit_dir, sm_wrapper), "w") as f:
            f.write(script_string)
        return


class SerpentCompiledConfig(SerpentCHTCOperations):
    """
    opens the compiled config that is sent to each server
    This does not contain information from other server's keys
    This way we do not inadvertantly pass sensistive information to public servers (such as file paths, UN's and IP's).
    """
    def __init__(self, config_path):
        self.config_path = config_path
        self.priority = 100
        self.ignore_mods = ['priority',
                            "dir_priority",
                            "dir_all",
                            "dir_setup",
                            "dir_paths"]
        self.config = read_json(self.config_path)
        self.valid_config = True
        super().__init__(self.config, self.ignore_mods)
        self.get_priority()
        self.order_mod = []
        if keys_exists(self.config, ['all', 'order_mod'])[0]:
            self.order_mod = self.config['all']['order_mod']
