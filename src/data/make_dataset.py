"""Processing steps of the dataset"""
# -*- coding: utf-8 -*-
import logging
import os
import time
import pandas as pd
from dotenv import find_dotenv, load_dotenv
from .scrape_data import navigate
from .improve_and_clean import remove_duplicates_and_na, remove_outliers
from .utils import in_ipynb
if in_ipynb():
    from halo import HaloNotebook as Halo
else:
    from halo import Halo


# @click.command()
# @click.option('--scrape', type=click.BOOL, default=False,
#               help='Wheather or not scrape the data from the urls on "/reference/urls.txt"')
# @click.argument('input_file', type=click.Path())
# @click.argument('output_file', type=click.Path())
def process_dataset(input_file, output_file, scrape):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).

        Parameters
        ----------
        input_file: str
            Input file to be processed
        output_file: str
            Output processed file
        scrape: bool
            Force the scraping process
    """
    spinner = Halo(text='Making dataset...', spinner='dots')
    logger = logging.getLogger(__name__)
    logger.info('Making final dataset from raw data')
    # Scrape data
    if scrape or not os.path.exists(input_file):
        spinner.start("Scraping data")
        with open('./references/urls.txt', 'r') as f:
            urls = f.readlines()
        scraped_dfs = []
        for url in urls:
            scraped_dfs.append(navigate(url, 1, 500))
        # Save results
        raw_data = pd.concat(scraped_dfs)
        raw_data.to_csv(input_file, index=False)
        spinner.succeed("Data Scrapped!")
    else:
        spinner.succeed("Loading scraped file...")
        raw_data = pd.read_csv(input_file)
        spinner.succeed("Scraped file already exists!")

    # Remove duplicates
    spinner.start("Removing duplicates and invalid values...")
    time.sleep(1)
    interim_data = remove_duplicates_and_na(raw_data)
    interim_data.to_csv(output_file.replace("processed", "interim"), index=False)
    spinner.succeed("Done removing duplicates!")

    # Remove outliers
    spinner.start("Removing outliers and inconsistent values...")
    time.sleep(1)
    final_data = remove_outliers(interim_data)
    final_data.to_csv(output_file, index=False)
    spinner.succeed("Done removing outliers!")
    spinner.start("Cleaning processing done!")
    spinner.stop_and_persist(symbol='✔'.encode('utf-8'), text="Cleaning processing done!")

    return final_data


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    input_file = "../../data/raw/SCRAPED_DATA.csv"
    output_file = "../../data/processed/CLEAN_DATA.csv"

    data = process_dataset(input_file, output_file, False)
