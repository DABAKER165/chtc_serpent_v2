from .htc_operations import (SerpentCompiledConfig)
from .serpent_config import (SerpentConfig)


def open_and_sort_configurations(config_filepaths,
                                 main_script_dir=None,
                                 compiled_config=False):
    """
    :param compiled_config: if the configuration is already compiled (for a server)
    :param config_filepaths: list of filepaths (absolute paths or relative to current dir)
    :param main_script_dir: where the main_script resides, used in the config gerneration
    :return: sorted (by alphabet, then priority (priority tie or not declared goes to alphabetical order)
    list of configs.
    """
    import operator
    config_list = []
    for config_path_i in config_filepaths:
        if compiled_config:
            sc = SerpentCompiledConfig(config_path=config_path_i)
        else:
            sc = SerpentConfig(config_path=config_path_i,
                               local_serpent_script_dir=main_script_dir)
        if sc.valid_config:
            priority = 100
            last_modified = 10000
            if hasattr(sc, 'priority'):
                priority = sc.priority
            if hasattr(sc, 'last_modified'):
                last_modified = sc.last_modified
            config_list.append([sc, priority, last_modified])
        else:
            print('This {0} is incomplete or has an error'.format(config_path_i))
            continue
    if len(config_list) < 1:
        return []
    config_list.sort(key=operator.itemgetter(1, 2))
    config_list = [x[0] for x in config_list]
    return config_list