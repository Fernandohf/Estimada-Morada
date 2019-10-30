"""Specific data processing steps"""


def combine_features(dataframe):
    """
    """
    # attribute combinations
    dataframe["bedrooms_per_area"] = dataframe["bedrooms"] / dataframe["area"]

    return dataframe
