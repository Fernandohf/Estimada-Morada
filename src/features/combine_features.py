"""Specific data processing steps"""


def combine_features(dataframe):
    """
    """
    df = dataframe.copy()
    # attribute combinations
    df["bedrooms_per_area"] = df["bedrooms"] / df["area"]

    return df
