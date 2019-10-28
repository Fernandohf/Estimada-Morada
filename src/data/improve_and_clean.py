""" Cleaning code to remove duplicates, outliers and improve the features of the dataset"""
import pandas as pd
import numpy as np
import math
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
from time import sleep


def address2coord(address):
    """
    Return the latitude and longitude from given address. Uses Free Nomatim OpenStreetMap API.
    OBS.: Super slow...zZz

    Parameters
    ----------
    address: str
        String with the address

    Returns
    -------
    lat: float
        Latitude of the address
    lon: float
        Longitude of the address
    """
    # Organize address
    address = address.replace(' ', '+')

    # Nominatim API
    api_url = "https://nominatim.openstreetmap.org/search?&format=json&q="

    # 'requests' retry features
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=Retry(20, backoff_factor=.5))
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    # Get the response
    r = session.get(api_url + address)

    # Load as json
    try:
        json_as_list = json.loads(r.content)
    except:
        print("API blocked. Message: {}".format(r.content))

    # Consider only the first result
    try:
        json_as_dict = json_as_list[0]
    # If no result is found
    except:
        json_as_dict = {'lat': 0, 'lon': 0}

    # Retrieve 'lat' and 'lon' from json dictionary
    lat = float(json_as_dict['lat'])
    lon = float(json_as_dict['lon'])

    return (lat, lon)


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


def apply_nomatin(file, na_cols=('price', 'address', 'area', 'type')):
    """
    Apply address2coord to every entry in 'address' and append results to a new column of the DataFrame:
    1 - Add 'lat' and 'lon' columns
    2 - Drop 'address' column
    OBS.: The conversion of addres in coordenates is supper slow. Took ~3 hours
    for ~8000 addresses conversions.

    Parameters
    ----------
    file: str or pd.DataFrame
        Name of the .csv  file  or DataFrame object

    Returns
    -------
    df: pd.DataFrame
        Combined dataframe with added 'lat' and 'lon' columns and removed 'address' column
    """

    # Load DataFrame from file
    if isinstance(file, str):
        df = pd.read_csv(file)
    else:
        df = file

    # Split df in n parts of maximum 1000 observations due to API limit
    n = math.ceil(len(df) / 1000)
    df_splitted = np.array_split(df, n)

    for i, df_piece in enumerate(df_splitted):
        # Get latitude and longitude
        coord_series = df_piece['address'].apply(address2coord)  # Series
        df_piece['lat'] = coord_series.apply(lambda x: x[0])     # Lat column
        df_piece['lon'] = coord_series.apply(lambda x: x[1])     # Lon column

        # Remove entries with missing values
        df_piece = df_piece.dropna(subset=['lat', 'lon'])

        # Replace missing values from bathrooms, bedrooms, condo, parking_spots, suites, condo with zero
        fill_cols = ['bathrooms', 'bedrooms', 'condo', 'parking_spots', 'suites', 'condo']
        df_piece[fill_cols] = df_piece[fill_cols].fillna(0)

        # Drop address column
        df_piece = df_piece.drop(['address'], axis=1)

        # Reset index again
        df_piece = df_piece.reset_index(drop=True)

        # Save partial df
        df_piece.to_csv("../../data/interim/Partial_nomatim_" + str(i) + ".csv", index=False)

        # Wait for next acquisition - 3 min
        sleep(180)

    # Join results dataframes
    return join_dataframes("../../data/interim/Partial_nomatim_", n)


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
