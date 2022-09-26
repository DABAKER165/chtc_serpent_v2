# Serpent
- Remote pipeline management system.
    - Efficiently manages the transfer of data across servers
    - Minimizes manual settings of a run using defaults
    - Runs on python 3.5 or later
    - Manages the queueing of pipelines on remote and local servers (as to reduce the chance of overloading a server).
    - Automatically creates, manages and submits CHTC jobs (HTC Condor Wisconsin).
    - Pipelines can use a mixture of CHTC, Remote Servers (i.e. GPU Workstations), and local processing
    - Pipelines can perform on in realtime data as it is generated
    - Helps achieve higher utilization of resources
    - Uses secure ssh and allows to store credentials locally and securely.
    - "Pollers" can be written to automatically kicks off runs.
    - Uses snakemake (optional, if on docker image) to manage automatic multithreading across scripts
# Quick start up
## setting up ssh
- This pipeline uses ssh to establish secure connection
- Without additional configuration, manual intervention is required to establish a connection (i.e. password entry)
- The following items are strongly recommended and/or required for every username / remote host combination
  - Storing credentials with RSA keys.
    - Store the credentials for remote connections in a rsa file
  - Attempt to reconnect to an existing ssh session instead of opening a new session.
    - Configure the  use of a ControlPath, ControlMaster and ControlPersist in the .ssh/config file for relavent hosts
### Create and Store id_rsa/.ssh credentials keys
- example commands for linux/macosx shown below
```bash
# example to add rsa_id on MacOSx or Ubuntu/Debian:
# Please use a method as approved by our security teams:
# first add your ssh to a new user specific id_rsa key.    

username=your_user_name
remote_host=remote_host_ip_address

ssh-keygen -t rsa -f ~/.ssh/id_rsa_${username}
cp ~/.ssh/id_rsa_${username}.pub ~/.ssh/authorized_keys
ssh-copy-id -i ~/.ssh/id_rsa_${username}.pub ${username}@${remote_host}
# type password two times.
```
```bash
# add to .ssh/config: (i.e. bbedit or nano). Do not use text editor (adds a different new line style than expected)!!!
echo ""; \
echo "Host ${remote_host}"; \
echo "HostName ${remote_host}"; \
echo "User ${username}"; \
echo "Identityfile2 ~.ssh/${username}"
```
```bash
# test connection and approve connection
ssh ${username}@${remote_host}
# follow prompt to approve the connection and properly add to known hosts
# Repeat for the other computers of interest.
```
### Edit config file to restablish connections
- Modify per your local it requirements
- Example shown below (~/.ssh/config)
```text
Host *
	IdentitiesOnly yes
	TCPKeepAlive yes
	ServerAliveInterval 120
	IPQoS=throughput

Host host.ip.addr.XXX
HostName host.ip.addr.XXX
ControlPath ~/.ssh/%r@%h:%p
ControlMaster auto
ControlPersist yes
Identityfile2 ~/.ssh/id_rsa_username

Host host.ip.addr.XXY
HostName host.ip.addr.XXY
ControlPath ~/.ssh/%r@%h:%p
ControlMaster auto
ControlPersist yes
Identityfile2 ~/.ssh/id_rsa_username2
```
# Primary Components

- Serpent Local management service (runs locally)
- Serpent Remote management service (runs on remote server side)
- Serpent Pipeline Modules (Code being ran by the pipeline)
- Serpent pipeline Configuration files (declares run order of pipeline scripts, directory paths, server settings)

## Serpent Local management service

- Runs locally on a computer
- Management pipeline uses limited resources <100MB ram, < 5% of a cpu
- All credentials should be stored securely on the local computer for security
    - i.e. Secret files and SSH rsa_id passwords.
- Loops through all the submit.json files and automatically launches jobs based on the configuration
- Optimizes the execution order of the json files based on input files dependency between steps.
- Manages the file transfers to and from remote servers
- Currently, uses rsync and rsync checksums to transfer files.
- Gives status updates to kick off the execution of local and remote scripts in the pipelines
- Can be configured Perform scripts to aggregate data, monitor storage, back up data, and send slack notifications

## Serpent Remote Server

- Server Side management service for serpent
- Copied and launched by local management service
- The local management service will check if it is running and relaunch the script if it is not (due to a crash, reboot
  etc)
- It can execute simple file aggregation scripts locally on CHTC (optional)
- It can aggregate samples, build sample sheets, and deploy CHTC jobs
- Automatically triggers condor_q and logs for held jobs and finished jobs
- Can submit in real time with rolling data:
    - Can add samples on a rolling basis, and will process nodes with only the new data

## Serpent Submission

- This is created by the autonomous system
- Aggregates Module code, configuration files and filesystem structure and sends it to remote and local services
- this is a folder of the submission status
    - ./<SUBMIT_NAME>/status
- Stores empty files as a way of monitoring what has and hasn't been compleeted or uploaded.
- Also stores "file transfer locks" to ensure scripts wait until a file transfer is completed before processing

## Pipeline modules

- These are bash scripts (.sh) and python3 scripts (.py) that the pipeline runs.
- helper code can be stored with the modules as it will be transferred to the remote servers
- Arguments can be stored in configs and can use smart text to autofil sample names and directory paths.

## Serpent configs

- This tells the managements system:
    - Where the files are and where they need to be
    - What servers to use
    - What code to run and where.
    - What order (or when to start) the code

# Explanation of components and directory strutures.

## Pipeline modules

- Pipeline modules code and default configurations should be all contained within the same parent directory
    - i.e.: ./pipeline_dir or ./mapreadss
- Default configuration(s) must be in the relative directory (or a custom location declared in the run based config
  file):
    - ./pipeline_dir/config
- Module code must be in the relative directory (required, hardcoded):
    - ./pipline_dir/modules
    - The module code is sent to every server, regardless if it is used.
    - bash .sh and python3 .py can be launched natively from debian/MacOSX or ubuntu sy systems.
        - Any other format should be "wrapped" in a .sh or .py script
    - Assumes packages are installed

- Note about using docker:
    - The Serpent does not check if a docker image is available on the remote system nor transfers missing images.
    - Be sure to make sure the remote repository is available to the server, or the image exists directly on the server
    - Make sure you do not need "sudo" rights to access the images or any files/directories used by the pipeline.

## pipeline static_files

- Static files are files that can be used across different runs or nodes on the same system (not required).
    - Used to avoid repeatedly transferring the same file to a remote server that is used by several samples
    - They are not sample/run specific
    - Examples: Reference Genome file,
    - Specified in the configuration PipelineName:ServerName:static_files
- Static files can be stored any where and are referenced by the configuration
    - Note: static files can also be stored in docker images and called directly from the program using a full path

## pipeline secrets (secret files) optional but highly recommended

- Some pipelines need to reach services that require secret files:
    - Slack, Google Drive may need to use secret files
- These should be stored in a safe place on your computer with access rights restricted
    - Make sure secure these files with .gitignore
    - The filename of secret files should not contain any protected information.
- Never store in the module directory, config directory, chtc serpent directory, chtc_configurations directory or
  chtc_submissions directory
    - These directories and sub directories may be transferred to CHTC servers, remote servers, and backup sites.

## config_dir

- This is a directory where your run-based configurations are stored
- This is declared in your argument to launch main.py:
    - i.e. python3 main.py --config_dir ./config --submission_dir ./submission_dir
- Input user defined configurations are stored tat is not in your default configuration (run names, run specific dir's)
- These will be aggregated by serpent based on your default configuration(s)
- Defined when you launch the local chtc_serpent/chtc_main.py as the --config_dir argument
- Can be located in any mounted location accessible by your local computer
- Only the parent directory is searched for launching new pipelines, subdirectories are ignored!

## submission_dir

- This is where your submission's aggregated configuration files and status files are stored.
- Defined when you launch the local main.py:
    - i.e. python3 main.py --submission_dir ./submission_dir
- Remote servers use these files to signal the start, and completion of steps in the pipeline, as well as what to run
  and when.
- This folder is transferred to every remote server defined in your configuration.
- You can also use this directory this to store (small under ~1MB) files generated by your scripts to transfer
  information to remote servers or track files
    - i.e. results of a find command, record what files have been "batched"

## chtc_local managment code

- Contained directly in the Serpent package
- DO NOT add files to this directory (unless you are developing the base code)
- open source but should not need to be modified
    - main.py
    - ./serpent_code/
- main.py
    - Main code that runs on a local server
- ./serpent_code/
    - Mixture of code that is rain locally and remotely.
- ./serpent_code/submit_job.py
    - Ran automatically on the remote server to manage the run
- ./mods/default_dir_setup.json
    - Set up for all the directories paths, can handle up to 3 separately mounted drives (submit_paths, in_paths,
      out_paths)
    - Intelligently uses drives if only one drive is set in the configuration submit, in and out can be on same drive.
    - all paths are based on home_dir, and (default home_dir is /home/<username> if not explicitly set even if it is
      Macosx which should be /Users/<un>)
- ./serpent_code/mods/utils.py
    - where generic (not class based) code resides
- ./serpent_code/mods/seprent_config.py
    - where the configuration parser class resides
- ./serpent_code/mods/htc_operations.py
    - where the remote configuration parser class resides
- ./serpent_code/mods/open_configurations.py
    - where the open the configurations class resides that trigger the other classes.

# Configuration files

- Configuration files tells the managements system:
    - Where the files are and where they need to be
    - What servers to use
    - What code to run and where.
    - What order (or when to start) the code
- There are required settings, hardcoded default settings
- Values of settings can be variablized (discussed below) in order to:
    - reference internal/submission based filestructures
    - refer to when a pipeline module should be started or has been completed
    - pass internal structures and file paths to scripts
    - reduce mistakes

## Using Default Configuration files and how they are merged

- serpent can use multiple default configuration files stored in the /pipeline_name/config directory
- all configurations must be in json format with a .json extension in the filename
- You must use at least one default configuration file (and it can be simply: {}  ) for each pipeline
- They are merged in alphabetical order priority by file name
- It can be useful to separate chtc specific configuration from the rest to ease management and creation.
- Submission configuration files must be in config_dir as defined by your config_dir argument at launch
    - Submission configuration files are merged with the default files and override default settings regardless of
      alphabetical order filename.
    - One Submission configuration file per submission.

## Guidelines for Default Configuration and submission based configurations

- Note: at least one default configuration file must be used (even if it is blank: {})
- chtc default configuration (optional, recommended if using CHTC)
    - Non-user specific and run specific CHTC settings.
    - must be in the ./pipeline_name/config directory if not declared in the submission directory
    - cannot be in sub directory of ./pipeline_name/config
    - submission configuration can override these settings
        - Note chtc settings can be stored and generated in an configuration file
- default configuration (optional, recommended)
    - Non-user specific and run specific settings.
    - must be in the ./pipeline_name/config directory if not declared in the submission directory
    - cannot be in sub directory of ./pipeline_name/config
    - submission configuration can override these settings
- User default files (optional, recommended)
    - User specific Ip configuration and names (but not run specific)
    - All passwords should be stored in RSA id keys.
    - must be in the ./pipeline_name/config directory if not declared in the submission directory
    - submission configuration can override these settings

- submission based configuration (required)
    - must be in config_dir as defined by your config_dir argument at launch
    - cannot be in sub directory of ./chtc_serpent/chtc_serpent_configs
    - Over rides any of default settings
    - Can declare the config directory manually

### Other notes for default configurations

- multple directories are allowed at a time of default configurations
- The run based configuration can point to tha appropriate default config path
    - (i.e. one uses miseq mapper, another uses bbmap mapper)

## Configuration template:

- Do not wrap as a list start and end with straight brackets: []
- Must be in a valid .json format and have the .json extensions
    - Invalid formats can crash the serpent workflow.
- File name must end in ".json" (not case sensitive).
- Filenames are case sensitive i.e. config A.json is a different name than a.json
    - just don't mess with case sensitive as many os systems handle this differently.
- Dictionary and strings only. Some exceptions for list values.

## Configuration settings and argument

### Overall format

- First layer of dictionary key is either "all" or the pipeline_module name
    - pipeline_module_name should memic the executable (sans extension)
    - "all" settings will be applied to all modules, (unless the string/list value already exists).
- The second layer is the settings for the modules. It declares the server were the settings are relavent
- The third and fourth layers will be discussed in other sections
- prioirty lower is ran before higher in each loop (every 2 minutes)
```json
{
  "all": {
    "prioirty": "3",
    "local": {
      "submit_paths": {
        "home_dir": "/Users/username",
        "pipeline_code_dir": "/Users/username/pipeline_1"
      },
      "in_paths": {
        "home_dir": "/Volumes/ONT_RESULT419"
      },
      "out_paths": {
        "home_dir": "/Volumes/ONT_RESULT419"
      }
    },
    "chtc": {
      "submit_paths": {
        "un": "username",
        "server": "submit_server.servername.edu",
        "home_dir": "/home/username"
      },
      "in_paths": {
        "un": "username",
        "server": "transfer.servername.edu",
        "home_dir": "/staging/groups/group_id"
      }
    },
    "remote2": {
      "submit_paths": {
        "un": "username",
        "server": "123.123.12.123",
        "home_dir": "/home/username"
      },
      "in_paths": {
        "home_dir": "/home/username"
      },
      "out_paths": {
        "home_dir": "/home/username"
      }
    }
  },
  "module_0": {
    "local": {
      "executable": "module_0.py",
      "arguments": {
        "--status_dir": "<status_dir>",
        "--batch_size": "50"
      }
    }
  },
  "module_1": {
    "local": {
      "start_trigger": "<module_0:ready>",
      "input_completed_trigger": "<module_0:completed>",
      "remove_remote_files_after": "<module_2:completed>"
    },
    "chtc": {
      "submit_job": "True",
      "transfer_to_server": "<module_0:local:module_out_dir>",
      "get_output": "False",
      "executable": "module_1.sh",
      "static_files": "/Users/username/static_files/MN908947.3.tar.gz,/Users/username/static_files/voc.csv",
      "arguments": {
        "-s": "${s}",
        "-m": "150",
        "-x": "1200",
        "-c": "config.cfg"
      }
    }
  },
  "module_2": {
    "chtc": {
      "input_ready_trigger": "<module_1:ready>",
      "input_completed_trigger": "<module_1:completed>",
      "submit_job": "False",
      "get_output": "False",
      "executable": "module_2.sh",
      "make_list": "False",
      "arguments": {
        "--input_dir": "<module_1:chtc:module_out_dir>",
        "--aggregate_dir": "<module_2:chtc:module_out_dir>",
        "--module_dir": "<module_2:chtc:module_working_dir>",
        "--incoming_ready": "<incoming_exe_ready>",
        "--incoming_complete": "<incoming_exe_complete>",
        "--output_dir": "<module_3:chtc:module_in_dir>",
        "--status_dir": "<status_dir>"
      }
    }
  },
  "module_3": {
    "local": {
      "mark_as_completed": "False",
      "input_completed_trigger": "<module_1:completed>",
      "remove_remote_files_after": "<module_3:completed>"
    },
    "chtc": {
      "start_trigger": "<module_1:completed>",
      "input_completed_trigger": "<module_1:completed>",
      "submit_job": "True",
      "get_output": "True",
      "executable": "module_3.sh",
      "static_files": "/Users/username/staticfile.txt,/Users/username/staticfile2.txt",
      "arguments": {
        "-s": "${s}",
        "-c": "2",
        "-m": "150",
        "-x": "1200",
        "-n": "200",
        "-r": "staticfile.txt"
      }
    }
  }
}
```

## Run Based Configuration:
- here we add the argument "--source_dir" but none of the default is changed (unless --source_dir was in the default cofig)
- we also give the pipeline config default directory
```json
{
  "all": {
    "local": {
      "default_config_dir": "/Users/username/pipeline_1/pipeline_1_config"
    }
  },
  "module_0": {
    "local": {
      "arguments": {
        "--source_dir": "/data/devices/drive1/run_1234_samples"
      }
    }
  }
}

```

### local Settings:
- mark_as_completed: use if you want to skip this step. 
  - This can be useful for trouble shooting or choosing mapping step
  - i.e. Mark module minimap "mark_as_completed": "False" and bbmap "mark_as_completed": "True"
  - i.e. use different primers and need to choose a different module/script to run the aggregation
- This example shows you can use multiple start_trigger and input_completed_triggers to get better parallel processing!
```json
{
  "module_2": {
    "local": {
      "mark_as_completed": "False",
      "start_trigger": "<module_0:completed>,<module_1:ready>",
      "input_completed_trigger": "<module_0:completed>,<module_1:completed>",
      "executable": "module_1.sh",
      "arguments": {
        "--submit_name": "<submit_name>"
      }
    }
  }
}
```

### Remote Settings
NOTE: if the server is named "chtc" it will execute workflow as a CHTC server
Othersize it will be treated as a normal remote server/workstation.
-local:
    - remove_files_ater : remove the output and input files form the server after specified completed file is generated
- remote (or chtc)
  - start_trigger : when to start the workflow (this shows when the previous step is ready to start running samples)
    - This step is note complete until input_completed_trigger is completed.It will continue to look for new samples
  - input_completed_trigger: finish running all samples as the previous step is done.  Once complete this step is complete.
  - submit_job : CHTC specific command, set to "True" if submiting to a node instead of processing locally.
  - get_output: rsync the results back to the local drive automatically (streams in realtime)
  - executable: File that needs to be executed (must be .sh for CHTC)
  - static_files: Files taht need to be transfered to the server and not sample (or run) specific (like reference files)
  - arguments: arguments to pass to the executable (or python scrypt)
    - if using "${s}" will pass the file name of the sample (basename)
      - i.e. "--sample_name": "${s}" or "-s": "${s}"

```json

{
  "local": {
    "remove_files_after": "<module_3:completed>"
  },
  "chtc": {
    "start_trigger": "<module_1:ready>",
    "input_completed_trigger": "<module_1:completed>",
    "submit_job": "True",
    "get_output": "True",
    "executable": "module_3.sh",
    "static_files": "/file1.txt",
    "arguments": {
      "-s": "${s}"
    }
  }
}
```
### Remote CHTC specific settings
- docker_image url+name of docker image, must be on accessible
- ram: amount of ram to request use GB: i.e. "16GB"
- cpus: number of cpus to request
- priority_flag: optional info that is CHTC specific
- machine_requirements: optional machine requirements
- disk_space: how much disk space to requesnt (use GB) i.e. "40GB"
- "transfer_to_server": transfer this directory to the server input directory.
```json
{
  "module_1": {
    "chtc": {
      "docker_image": "dockerregsiet.edu/username/dockerimage:v1",
      "ram": "16GB",
      "cpus": "2",
      "priority_flag": "optional info",
      "machine_requirements": "(OpSysMajorVer =?= 7) && (Target.HasCHTCStaging == true)",
      "disk_space": "40GB",
      "transfer_to_server": "<module_0:local:module_out_dir>"      
    }
  }
}
```
### Remote docker specific settings
- docker + the docker image must be loaded on the remote system
- cpus: number of cpus to request per server
- gpu_count: if you want to request a GPU
- cores: number of cpus to request per SAMPLE (cores<= cpus)
- mount_list: list object of mounted drives as seen on the docker run -v 
  - i.e. /home:/home, /mnt/drive:/mnt/drive
- disk_space: how much disk space to requesnt (use GB) i.e. "40GB"
- sample_extension: what the extension of the file is to search for in the dir
  - leave missing or blank if no extensions (any file)
  - "parent_directory" will use directories instead of files (incase your program requires dirs)
- make_list: default is True, set to False if your sample list is dynamically created within your launched module
- sample_list_path: set make_list to False if using, and use this \n (line) delimeted file of sample paths.
```json
 {
  "module_5": {
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
}
```
### Remote snakemake sppecific settings
- Snakemake must be on the docker image and use docker.
- set use_snakemake to True. 

```json
{
  "module_5": {
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
}
```
### Required Settings
- The pipeline uses ssh (and rsync over ssh).
    - All ssh passwords should be stored in rsa_id's locally on the local computer (not on remote computers)
    - They should be configured not to prompt.
- the executable can only be declared on one server/local /chtc per module
- If you need an executable to run on multiple servers/locally, you must declare it as separate pipeline modules
### Require PATHS for local jobs :
- home_dir in all:local:submit_paths (or in_paths or out_paths)
- pipeline_code_dir in all:local:submit_paths (or in_paths or out_paths)
```json
 {
  "all": {
    "local": {
      "submit_paths": {
        "home_dir": "/Users/username",
        "pipeline_code_dir": "/Users/username/pipeline_1"
      }
    }
  }
}
```

### Required PATHS/ server data for remote jobs:
- un, server, and home_dir
```json
{
"remote_2": 
    {
    "submit_paths": {
      "un": "username",
      "server": "123.123.12.123",
      "home_dir": "/home/username"
    }
  }
}

```



### Pipeline order

- The order the pipeline configuration .json files are written has no impact on the processing order
- start_trigger and completed_trigger should be declared under the 'local' key section
- The processing order is based on start_triggers and complete_triggers, and the status of previous steps (ready or
  completed)
    - start triggers: when a subset (and possibly all) of the data from the previous pipeline step(s) are ready for
      processing and start the current pipeline step
        - The current pipeline step will continue to look for incoming data to process until the complete trigger(s) are
          generated.
    - complete triggers: when all of the data from previous pipeline step(s) are ready for processing and start the
      current pipeline step.
    - Note: this allows subsequent steps in the pipeline to know when to begin. Steps can occur in parallel, and using "
      ready" status to process data as it comes in.
- Example: data is being generated by a sequencing tool in real time and needs to be demultiplexed then consensus
  sequence
    - For this example we will ignore the intricacies steps of batching, aggregating, and backing up data.

1. Data must be transferred from the sequencing tool named "data_transfer"
2. Data must be demultiplexed into different samples named "demultiplex"
3. Each sample must be processed into a consensus sequence (in parallel) named "generate_consensus"

- Step 1 can be done "streaming" or as the data comes in.
    - Waiting until the sequencing tool finishes will increase the total process as we would wait for several GB
      transfer as opposed to getting small amounts of data transferred as it comes in.
    - We cannot use the sequencer's workstation to demultiplex as it is not powerful enough to keep up in realtime.
- Step 2 can also be done in "streaming" to split the incoming files into separate samples.
    - As soon as the some data transferred, step it can create a ready status flag for step 2 to use
        - "start_trigger": "<data_transfer:ready>"
        - note the arrows "<  >" on both sides of the string to signify a variable
        - note that the ":" is a delimiter only relavent to the variablized form.
    - Step 2 needs to know When the sequencer is complete (detected through a summary file being generated or some time
      limit)
        - "complete_trigger": "<data_transfer:completed>"
        - the distinction is that step 2 needs to know when step 1 is finished so it knows no more data is coming an can
          generate its own complete flag after running one more time
- The way the code is written Step 3 has to wait until Step 2 is complete as it needs all results from each sample
  already demultiplexed from step 2 to form the most complete consensus.
    - the start trigger and complete trigger will be the same.
    - To reduce mistakes and writing, if only one is declared, the code will automatically apply the same value to the
      other trigger
    - "complete_trigger": "<demultiplex:completed>"

- If the data was never streamed, then all steps would have to wait until the sequencer is finished, which would take
  several hours (in this case we had 4-8 hours per run * 4 simultaneous runs).
- By using start triggers and ready values, steps 1-2 just need to finish up the remaining data, then step 3 can begin
  soon after finishing in minutes.

### Using Variablized values

- Variablized values are almost always required to be used
    - reference internal/submission based filestructures
    - refer to when a pipeline module should be started or has been completed
    - pass internal structures and file paths to scripts
    - reduce mistakes
- They take on 3 forms:
    - Referencing setting(s) that are in the default/submission configurations files
    - Referencing setting(s) that are in the aggregated submission files configurations
        - These are paths that are internally declared by chtc serpent based on relative paths
        - i.e. status folder, module folder,
    - Referencing when prior modules are "ready" or "complete" for the:
        - start triggers: when a subset (and possibly all) of the data from the previous pipeline step(s) are ready for
          processing and start the current pipeline step
            - The current pipeline step will continue to look for incoming data to process until the complete trigger(s)
              are generated.
        - complete triggers: when all of the data from previous pipeline step(s) are ready for processing and start the
          current pipeline step. The pip
        - executable arguments: can use when previous pipelines steps are ready or complete directly in the code
            - Arguments references the file location and must use a "file exists" command.
            - If the referenced file exists then that step is ready or completed if not then it is not ready or
              complete.
        - Note: this allows subsequent steps in the pipeline to know when to begin. Steps can occur in parallel, and
          using "ready" status to process data as it comes in.
        

#### Referencing setting(s) in arguments that are variabalized
- complete list of options is in ./serpent_code/mods/default_dir_setup.json
- each item must be encapsulated in <>
- status_dir: status_dir path of server executing on
- submit_name: basename of submitted config  file (minus extension)
- incoming_complete: "TRUE" is all ready or "FALSE" is passed
- incoming_ready: "TRUE" if incomming is all ready or "FALSE" is passed
- "<prev_module_name:server_name:incoming_completed" : The filespath of the incoming_completed file.
- "<prev_module_name:server_name:arguments:--local_sample_dir>" grabs the arguments value of "--local_sample_dir "in server_name/module_name
```json
{
  "arguments": {
    "-a": "<module_name:server_name:sample_dir>",
    "-b": "<module_name:local:out_dir>",
    "-s": "<submit_name>",
    "-t": "<status_dir>",
    "-i": "<incoming_complete>",
    "-y": "<incoming_ready>",
    "-r": "<prev_module_name:server_name:arguments:--local_sample_dir>"
  }
}
```

- Creating a CHTC that submits jobs require:
    - executable
        - executable must be stored in "./<pipeline>/modules"
        - "<module_name": {"chtc": "executable": "executable name"
    - submit_job = True
        - "module_name": {"chtc": "submit_job": "True"
    - A sample is required of the following with argument -s and ${s}:
        - The sample will be the filename of from the sample_sheet
        - "<module_name": {"chtc": arguments": {"-s": "${s}"
    - "<module_name": {"chtc": "get_output" = "True"
        - copies the results folder back to a local location from CHTC
        - set to "False" if you do not need the results.
- Tips:
    - credential should be stored securly for access to SSH using RSA keys
    - Use "all" as module to apply configuration to every module
    - almost everything is case sensitive.
    - "module_name": {local": { "argument": {:--status_dir: will allow you to customize when a script is completed and
      ready
        - if --status_dir is delcared, your executable script MUST generate the <module_name>_completed.txt and <
          module_name>_ready.txt files or it will not progress to the next step.
            - in your code: touch ${status_dir}/<module_name>_ready.txt
            - in your code: touch ${status_dir}/<module_name>_completed.txt
            - use the "<status_dir>" to get the path of the status dir automatically
        - if --status_dir is not declared, it will run once and automatically be set to ready and completed upon
          completion
    - start_trigger and input_completed_trigger
        - can be used to submit jobs in rolling manner
        - start_trigger will run the script with available data
        - input_completed_trigger will stop relaunching the script after it runs
        - Useful if generating files in realtime that needs processing.
    - pass arguments between modules:
        - you can pass arguments between modules
        - this is useful to add as a default configuration
        - this way you only need to declare a file location once
            - "arguments": {"--arg1": "<module_name:local:arguments:--local_dir>",
            - "arguments": {"--arg2": "<module_name:chtc:arguments:--arg1>",
            - "arguments": {"--arg3": "<module_name:chtc:arguments:-a>",
        - you can also pass a directory that is not from the arugments:
            - "arguments": {"--sample_dir": "<prev_module:chtc:sample_dir>"
    - pass the status of a incoming ready or complete
        - will pass True or False to the code
        - Useful for batch processing and your final batch is smaller than the batch size
        - "arguments": {"--arg1": "<incoming_complete>"
        - "arguments": {"--arg2": "<incoming_ready>"
- a module cannot have a executable in "chtc" and "local"


# Requirements

## chtc_local

-
    - Linux OS (within last 6 years recomended) MacOSx with home brew packages installed is allowed
- rsync=3.1.3 or later
- At least 4GB RAM, though dependent on code being executed
- bin/bash
- python3
    - os
    - subprocess
    - datetime
    - sys
    - json
    - time
    - argparse
    - pathlib
    - shutil
    - socket
    - pandas (recommended but not required)
- ssh

## chtc_serverside

- rsync=3.1.3 or later
- Linux system (within last 6 years recomended) MacOSX with home brew packages installed is allowed
- At least 4GB RAM, though dependent on code being executed
- bin/bash
- python3
- python3 packages
    - os
    - subprocess
    - datetime
    - sys
    - json
    - time
    - argparse
    - pathlib
    - shutil
    - socket
    - pandas (recommended but not required)
- ssh

# Known Issue List.

- server_side crashes if json is not valid format.
    - fixed for most cases, but still can crash from bizarre errors.
- Currently, serpent does not load balance across remote servers
    - a poller service can be written to load balance at the time of creation of the configuration files
        - meaning you declare the ip adress /un differently by dynamically creating config files with a separate program
          based on resource consumptions, queued runs
    - CHTC does make an effort to load balance submitted jobs across nodes using htc condor.
  
# Under Development:
A monitoring service that runs separate to make sure the drives (remote and local) have ample space, and valid connections
- alert admins if there is an issue.

- ./monitor_hardware.py
- ./serpent_configs/alert_limits.json