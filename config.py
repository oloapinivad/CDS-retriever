"""Module for configuration of the parser"""

import argparse
import yaml
import re


def parser():
    """
    Command-line parser
    """

    internal_parser = argparse.ArgumentParser(description="Script for data retrieval and processing")

    internal_parser.add_argument("-c", "--config", help="Path to the YAML configuration file")
    internal_parser.add_argument("-n", "--nprocs", type=int, help="Number of parallel processes")
    internal_parser.add_argument("-u", "--update", action="store_true", help="Update existing dataset")
    internal_parser.add_argument("-v", "--variable", type=str, help="Pick a specific variable")
    internal_parser.add_argument("-l", "--levelout", type=str, help="Pick a specific levle")
    internal_parser.add_argument("--outputdir", type=str, help="Outputdir")
    internal_parser.add_argument("--tmpdir", type=str, help="Tmpdir")

    # Parse the command-line arguments
    return internal_parser.parse_args()



def load_config(file_path):
    """
    Loading configuration YML file
    Returning configuration dict
    """

    with open(file_path, 'r', encoding='utf8') as file:
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



def validate_levelout_get_unit(levelout):
    """
    Determine the appropriate unit for the given `levelout` input.

    This function checks the validity of the `levelout` parameter and returns the unit
    'hPa' if applicable. The following mutually exclusive cases are supported:
    
    1. A single string:
       - If the string matches one of the allowed exceptions ('sfc', 'plev37', 
         'plev19', 'plev8'), no unit is added (returns an empty string '').
       - If the string is a valid numerical level (e.g., '500' or '500hPa'), 
         the unit 'hPa' is added.
       - Invalid strings raise a `ValueError`.

    2. A list of strings:
       - If all elements in the list are valid numerical levels (e.g., ['500', '750hPa']), 
         the unit 'hPa' is added.
       - If any element in the list is invalid, a `ValueError` is raised.

    3. Any other input type (e.g., integers or mixed types) is considered invalid
       and raises a `ValueError`.

    Args:
        levelout (str or list of str): The pressure levels to check. It can be:
            - A single string (e.g., '500hPa', 'sfc').
            - A list of strings (e.g., ['500', '750hPa']).

    Returns:
        str: The unit 'hPa' if applicable, or an empty string '' if `levelout`
        matches one of the allowed exceptions.

    Raises:
        ValueError: If `levelout` is invalid (e.g., contains mixed or unsupported cases).
    """

    # Define allowed exception strings and valid numerical pattern
    allowed_exceptions = ['sfc', 'plev37', 'plev19', 'plev8']
    valid_level_pattern = r'^\d+(hPa)?$'  # Matches strings like '500' or '500hPa'

    # Single string
    if isinstance(levelout, str):
        if levelout in allowed_exceptions:
            return ''
        elif re.match(valid_level_pattern, levelout):
            return 'hPa'
        else:
            raise ValueError(f"Invalid levelout specification: '{levelout}'.")

    # List of strings
    if isinstance(levelout, list):
        if all(isinstance(item, str) and re.match(valid_level_pattern, item) for item in levelout):
            return 'hPa'
        else:
            raise ValueError(
                f"Invalid levelout list: {levelout}. Each element must be a valid level (e.g., '500' or '500hPa')."
            )

    # Default: Invalid type
    raise ValueError("Invalid input: levelout must be either one of 'sfc', 'plev37', 'plev19', 'plev8', or a valid string or list of strings.")



