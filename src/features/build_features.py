"""Add new features to the dataset"""
# import click
import pandas as pd
import logging
from dotenv import find_dotenv, load_dotenv
from .address_to_coordenates import apply_nomatin
from .utils import in_ipynb
if in_ipynb():
    from halo import HaloNotebook as Halo
else:
    from halo import Halo


# @click.option('--nomatin', type=click.BOOL, default=False,
#               help='Wheather or not call the nomatin API to convert the addresses')
def add_features(input_file, nomatin):
    """ Runs build features scripts to turn processed data from (../processed) into
        improved data (saved in ../processed as well).
    """
    spinner = Halo(text='Building features...', spinner='dots')
    # Add lat/lon columns
    clean_data = pd.read_csv(input_file)
    if nomatin:
        spinner.start("Adding Latitude and Longitude columns")
        transformed_data = apply_nomatin(clean_data)
        transformed_data.to_csv("./data/processed/TRANSFORMED_DATA.csv", index=False)
        spinner.succeed("Latitude and Longitude features added!")
    else:
        transformed_data = pd.read_csv("./data/interim/TRANSFORMED_DATA.csv")


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    load_dotenv(find_dotenv())

    add_features()
