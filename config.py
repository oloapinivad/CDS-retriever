import yaml


def load_config(file_path):
    """
    Loading configuration YML file
    Returning configuration dict
    """

    with open(file_path, 'r') as file:
        try:
            config = yaml.safe_load(file)
            return config
        except yaml.YAMLError as exc:
            print(f"Error loading config file: {exc}")
            return None


def print_config(conf_dict):
    """
    Print the configuration options
    """
    
    print(f"\nDownloading files in {conf_dict['tmpdir']}") 
    print(f"Storing final files in {conf_dict['storedir']}")
    print(f"Downloading {conf_dict['varlist']} from {conf_dict['dataset']}")
    print(f"Data range: {conf_dict['year']['begin']}-{conf_dict['year']['end']}")
    if conf_dict['year']['update']:
        print('Updating existing datasets...')
    print(f"Vertical levels: {conf_dict['levelout']}")
    print(f"Data frequency: {conf_dict['freq']}")
    print(f"Grid selection: {conf_dict['grid']}")
    print(f"Area: {conf_dict['area']}")
    print(f"Number of parallel processes: {conf_dict['nprocs']}")
    print(f"Download {conf_dict['download_request']} chunks")
    print('Actions:')
    if conf_dict['do_retrieve']:
        print('\t - Retrieving data')
    if conf_dict['do_postproc']:
        print('\t - Postprocess data')
    if conf_dict['do_align']:
        print('\t - Set a common time axis for monthly data')
    print()

    return None





