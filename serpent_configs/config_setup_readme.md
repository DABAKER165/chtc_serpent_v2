# this is the setup instructions for creating config files

- configurations can be broken into multiple files for ease of understanding
- They are processed in alphanumerical order (0-9 then Aa-Zz)
- The default files are processed first, then the runspecific files.
- Within a file, individual module information will overwrite the "all" category
- Subsequently processed default files will overwrite specified data of previous default files
- The runs specific file will over write specified data of previous default files
    - Dicitonaries will only over write the specified keys of the dictionary.
        - i.e. default: {"arguments": {"a":1, "b":"1"}} run specific: {arguments:{"b":"2"}} becomes
            - {"arguments": {"a":1, "b":"2"}}
        - example 2: default: {"arguments": {"a":1, "b":"1"}} run specific: {arguments:{"c":"2"}} becomes -{"arguments":
          {"a":1, "b":"1", "c":"2"}}

# Server set up and core paths:

- We need to pass (full/absolute or relative) paths to where the scripts will run
- Relative paths may not work as intended due to limitations in privileges: absolute paths should be used when possible
- These core paths is where the module will run from and save its files.
- They are set up in the config.json files and can be saved as sepearte files
- Each "module" needs its own file. Sharing the same file is possible but not recommended.
- The "all" means this will apply to all modules, but will not overwrite if it is already present

## 4 mount points allowed per server.

- Due to space privilages, read/write setups the code, input data and output data may have to reside on different
  mounted drives
    - submit_paths: { "home_dir"} is where the configuration and tracking files for the CHTC serpent will be stored.

- This set up allows for it with the following priority
    - If out_paths is not present then it will use in_paths as the out paths
    - If in_paths are not present then it will using "submit_paths"
- You can use as many servers as needed to run your pipelines each need to be specified.
- The local has one path that remote servers will not have:
    - submit_paths: {pipeline_code_dir} is where the pipeline's modules' code is stored.
- Remote servers need two additional requirements that the local will not have:
    - "submit_paths": {"un"} and  "submit_paths": {"server"} which are the usernames and server ip_address of the used
      to access the host via ssh
        - i.e. ssh <un>@<server>
    - Remote servers may have multiple connections for uploading downloading data, and thus the submit path can be
      different from the in and outpaths.

```json
{
  "all": {
    "local": {
      "submit_paths": {
        "home_dir": "/Users/username",
        "pipeline_code_dir": "/Users/username/github/module_name"
      },
      "in_paths": {
        "home_dir": "/Volumes/mount_1"
      },
      "out_paths": {
        "home_dir": "/Volumes/mount_2"
      }
    },
    "chtc": {
      "submit_paths": {
        "un": "username",
        "server": "submit.edu",
        "home_dir": "/home/username"
      },
      "in_paths": {
        "un": "username",
        "server": "transfer.edu",
        "home_dir": "/staging/groups/user_group"
      },
      "out_paths": {
        "un": "username",
        "server": "transfer.edu",
        "home_dir": "/staging/groups/user_group"
      }
    }
  }
}
```

# Controlling when Modules start to process data, and clean up data

- this is module specific and should not be included as ALL
- Modules can have descript names and numbers are not used for processing order
    - You can call a module "process_reads"

## Input Keys

- start_trigger
    - If a start trigger does not exists, then it will automatically start that module.
    - If a completed_trigger does not exist, it will run once.
        - If "completed_file_path" exists in the arguments and completed_trigger does not
            - (i.e. --completed_path": "<rsync_remote_dir:local:complete_file_path>)
            - The module will process until the module script manually creates the completed file path
            - This can allow a script to run for 24 hours or look for a time stamp on a file to show a previous process
              has completed.
    - the module will start to process files (if the files exist) once this trigger is found
    - If the files do not exist it will skip the step until the files exist
    - Files may not exist if the previous step never made valid files (which can happen in reality)
- input_completed_trigger
    - the module will stop processing files and show it has completed after this flag is detected AND it has ran one
      last time
- remove_remote_files_after
    - this after this flag is found, input and outputfiles for this module will be deleted
    - This can be useful to free up disk space as the pipeline is running

## value options

- "<module_name:ready>" : if the module specified is ready to give data. You may generate this file in a script to the
  effect: these files are complete (enough) to start operating on
- "<module_name:completed>" : if the module specified has completed, this will meet this criteria

```json
{
  "module_2": {
    "local": {
      "start_trigger": "<module_1:ready>",
      "input_completed_trigger": "<module_1:completed>",
      "remove_remote_files_after": "<module_3:completed>"
    }
  },
  "module_3": {
    "local": {
      "start_trigger": "<module_1:ready>",
      "input_completed_trigger": "<rsync_remote_dir:completed>",
      "remove_remote_files_after": "<backup_files_midnight:completed>"
    }
  }
}
```

# Chtc Specific configurations

- These are often module specific chtc criteria
-
- The chtc submit file will be generated based on this critieria
- The sample_file_list will be generated based on the files in the input folder
    - priority_flag (optional)
        - is a chtc specific flag for groups with reserved hardware (often purchased by the lab)
    - ram (required)
        - specify in GB, the amount of ram the vm should allocate to you node
    - docker_image (optional)
        - If the docker image is ommited, no docker image will be used
        - This should be the docker image in the chtc accessible registry.
    - cpus (required)
        - The number of cpus to request
    - disk_space (required)
        - The amount of diskspace requested (specify with GB)
    - machine_requirements
        - Any other additional requirements (OS version) in the nomenclature required by CHTC submit files.

```json
  {
  "module_name": {
    "chtc": {
      "docker_image": "dockerreg_chtc.edu/username/imagename:tag",
      "ram": "8GB",
      "cpus": "1",
      "priority_flag": "",
      "machine_requirements": "(Target.HasCHTCStaging == true)",
      "disk_space": "20GB"
    }
  }
}
```

## Running on hosts can use snake make to manage multiple threads of the same process

- Snakemake must be on the docker image
- 1 docker image is launched, and snakemake divides up the cores within the single docker run.
- "use_snakemake" set to "True" will run with snake make
    - if use_snakemake is omited (or set to "False") snakemake will not be used and it will not be threaded.
    - you can run docker without also running snakemake
- cores is the number of cores given to the snakemake / docker image
- cpus is the number of cores given to each threaded module.
    - Specifying 4 cores and 4 cpus will run 1 thread of 4 cpus per thread
    - Specifying 4 cores and 1 cpu will run 4 threads of 1 cpu per thread
    - Specifying 4 cores and 3 cpus will run 1 thread of 3 cpus per thread
    - Specifying 4 cores and 5 cpus will run 1 thread of 4 cpus per thread
    - Specifying 4 cores and 2 cpus will run 2 threads of 2 cpus per thread
- the mount_list is the server specific volumes to mount as as the docker run command sees as "-v /home:/home -v
  /Volumes:/Volumes"
- sample_extension is the directory where the files are or the suffix
    - "parent_directory" will pass the transfered directory as the sample
    - ".fasta" would be files that end with that extension
- sample_list_path will use the sample_list_path that was pased as the sample list (return delimited)
- if sample_extension and sample_list_path are both ommited, a list of all files in the transfered input direcory will
  be used
- "transfer_to_server" is the folder of outputted files that needs to be recieved from the local host.

```json
"module_name": {
"remote_1": {
"use_snakemake": "True",
"docker_image": "bonito:v0_4_0",
"gpu_count": "1",
"cpus": 4,
"cores": 4,
"mount_list": [
"/home:/home"
],
"sample_extension": "parent_directory",
"transfer_to_server": "<subset_fast5:local:module_out_dir>"
}
}
```

- The following will run 4 thread of 1 cpus at a time using no gpu's

```json
"module_name": {
"remote_1": {
"use_snakemake": "True",
"docker_image": "bonito:v0_4_0",
"cpus": 1,
"cores": 4,
"mount_list": [
"/home:/home"
],
"sample_extension": "parent_directory",
"transfer_to_server": "<subset_fast5:local:module_out_dir>"
}
}
```

# runnign executables

- executable: path of your executable relative to the modules_dir only .sh and .py can be ran
    - if you need to run something else (java, snakemake) wrap it in a .sh and launch the .sh
- arguments: dictionary of your arguments
  - Use the proper -- or -
  - "= signs will be added for python as required"
```json
{
  "module_name": {
    "executable": "run_all.py",
    "arguments": {
      "--ready_path": "<module_name:local:ready_file_path>",
      "--completed_path": "<module_name:local:complete_file_path>",
      "--batch_size": "50"
    }
  }
}
```

# runnign executables

- additional arguments
    - static files the local filepath(s) of non-sample specific files for the run:
        - useful if a file is shared between all of the threads (i.e. reference file)
        - a dictionary {"file1":"/var/file1.txt","file2""/var/file2.txt"}
            - if a dictionary can be used as a script arguments input:
                - <module_name:chtc:static_files:ref>
                - YOu will need to convert to the directory of where it ended up on the server
        - a list ["/var/file1.txt","/var/file2.txt"]
        - comma delimited string ("/var/file1.txt,/var/file2.txt")
        -
    - get output
        - True if ommited
        - Set to "True" to get the files from the remote to the local directory
        - Set to "False" to not get the files form the remote directory (i.e. they were intermediate files)
    - submit_job
        - Set to true to submit the jobs to CHTC noes
        - Set to false if not on chtc or if running on the chtc submit server (i.e. to aggregate files)

```json
{
  "module_name": {
    "chtc": {
      "get_output": "true",
      "transfer_to_server": "<prev_module_name:local:module_in_dir>",
      "submit_job": "True",
      "static_files": {
        "file1": "/var/file1.txt",
        "file2": "/var/file2.txt"
      },
      "arguments": {
        "--ref_path": "<module_name:chtc:static_files:file1>"
      }
    }
  }
}
```