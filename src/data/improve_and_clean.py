""" Cleaning code to remove duplicates, outliers and improve the features of the dataset"""
import pandas as pd
import numpy as np
import math
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
from time import sleep


def remove_duplicates_and_na(file, na_cols=('price', 'address', 'area', 'type')):
    """
    Briefly, this function cleans the data performing the following steps:
    1 - Remove duplicates
    2 - Remove empty entries of na_cols 'price','address', 'area' and 'type'

    Parameters
    ----------
    file: str or pd.DataFrame
        Name of the .csv  file  or df object
    na_cols: iterable
        Names of the columns to drop values not available (NA)

    Returns
    -------
    df: pd.DataFrame
        Cleaned dataframe
    """

    # Load DataFrame from file
    if isinstance(file, str):
        df = pd.read_csv(file)
    else:
        df = file

    # Remove duplicates
    df.drop_duplicates(inplace=True)

    # Remove entries with missing values
    df.dropna(subset=na_cols, inplace=True)
    df.reset_index(drop=True, inplace=True)

    return df


def join_dataframes(file_static, n, save=False):
    """
    Join multiple .cvs files created previously, removing unsuccessful 'lat' and 'lon' coordenates.

    Parameters
    ----------
    file_static: str
        Begining of the name of .csv file that does not changes
    n: int
        Number of files.
    Returns
    -------
    df: pd.DataFrame
        Final combined DataFrame
    """
    # Placeholder
    df = pd.DataFrame()
    # Read all files and append them together
    for i in range(n):
        df_piece = pd.read_csv(file_static + str(i) + ".csv")
        df_piece = df_piece[(df_piece.lat != 0) & (df_piece.lon != 0)]
        df = df.append(df_piece)

    # Save result
    if save:
        df.to_csv("../../data/interim/Final_Nomatim.csv", index=False)

    return df


def remove_outliers(df, quantile=.99, margin=.15,
                    cols=('area', 'bathrooms', 'bedrooms', 'condo', 'parking_spots', 'price', 'suites')):
    """
    Remove invalid values in given dataset.

    Parameters
    ----------
    df: pd.DataFrame
        Data to be removed
    quantile: float
        Percentile to use as reference before filtering df
    margin: float
        Value to use as margin (0 < margin <= 1)
    cols: iterable
        Columns to apply the filtering

    Returns
    -------
    df: pd.DataFrame
        Filtered dataframe
    """

    # Quantiles for each column
    quantiles_max = df.quantile(quantile, axis=0)
    quantiles_min = df.quantile(1 - quantile, axis=0)

    # Add margins
    for col in cols:
        quantiles_max[col] *= 1 + margin
        quantiles_min[col] *= 1 - margin
        # Outliers for each column
        df = df[(df[col] <= quantiles_max[col]) & (df[col] >= quantiles_min[col])]

    return df
