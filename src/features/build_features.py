"""Add new features to the dataset"""
# import click
import logging
import os
import time

import pandas as pd

from dotenv import find_dotenv, load_dotenv

from ..utils.utils import in_ipynb
from .address_to_coordenates import apply_nomatin
from .combine_features import combine_features

if in_ipynb():
    from halo import HaloNotebook as Halo
else:
    from halo import Halo


# @click.option('--nomatin', type=click.BOOL, default=False,
#               help='Wheather or not call the nomatin API to convert the addresses')
def add_features(input_file, output_file, force):
    """ Runs build features scripts to turn processed data from (../processed) into
        improved data (saved in ../processed as well).

        Parameters
        ----------
        input_file: str
            Input file to be processed
        output_file: str
            Output processed file
        force: bool
            Force to process the input file
    """
    spinner = Halo(text='Building features...', spinner='dots')

    clean_data = pd.read_csv(input_file)

    # Add lat/lon columns
    if force or not os.path.exists(output_file):
        spinner.start("Adding Latitude and Longitude columns")
        transformed_data = apply_nomatin(clean_data)
        transformed_data.to_csv(output_file, index=False)
        spinner.succeed("Latitude and Longitude features added!")
    else:
        spinner.start("Loading transformed file...")
        time.sleep(2)
        transformed_data = pd.read_csv(output_file)
        spinner.stop_and_persist(text="Transformed file already exists!")
    transformed_data.to_csv(output_file, index=False)

    # Combine features
    transformed_data = combine_features(transformed_data)

    return transformed_data
