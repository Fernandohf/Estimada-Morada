"""Processing steps of the dataset"""
# -*- coding: utf-8 -*-
import click
import logging
from halo import Halo
from pathlib import Path
from dotenv import find_dotenv, load_dotenv
from scrape_data import navigate
from improve_and_clean import remove_duplicates_and_na, apply_nomatin, remove_outliers
import pandas as pd


@click.command()
@click.option('--scrape', type=click.BOOL, default=False,
              help='Wheather or not scrape the data from the urls on "/reference/urls.txt"')
@click.option('--nomatin', type=click.BOOL, default=False,
              help='Wheather or not call the nomatin API to convert the addresses')
def make_dataset(scrape, nomatin):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    spinner = Halo(text='Making dataset...', spinner='dots')
    logger = logging.getLogger(__name__)
    logger.info('Making final dataset from raw data')
    # Scrape data
    if scrape:
        spinner.start("Scraping data")
        with open('../../references/urls.txt', 'r') as f:
            urls = f.read_lines()
        scraped_dfs = []
        for url in urls:
            scraped_dfs.append(navigate(url, 1, 500))
        # Save results
        raw_data = pd.concat(scraped_dfs)
        raw_data.to_csv("../../data/raw/SCRAPED_DATA.csv", index=False)
        spinner.succeed("Data Scrapped!")
    else:
        raw_data = pd.read_csv("./data/raw/SCRAPED_DATA.csv")

    # Remove duplicates
    spinner.start("Removing duplicates and invalid values")
    interim_data = remove_duplicates_and_na(raw_data)
    interim_data.to_csv("./data/interim/1_CLEAN_DATA.csv", index=False)
    spinner.succeed("First cleaning stage done!")

    # Add lat/lon columns
    if nomatin:
        spinner.start("Adding Latitude and Longitude columns")
        interim_data = apply_nomatin(interim_data)
        interim_data.to_csv("./data/interim/2_TRANSFORMED_DATA.csv", index=False)
        spinner.succeed("New features added!")
    else:
        interim_data = pd.read_csv("./data/interim/2_TRANSFORMED_DATA.csv")

    # Remove outliers
    spinner.start("Removing outliers and inconsistent values")
    final_data = remove_outliers(interim_data)
    final_data.to_csv("./data/processed/FINAL_DATA.csv", index=False)
    spinner.succeed("Final cleaning stage done!")


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    make_dataset()
