"""Visualizations for the data"""
import folium
import matplotlib.pyplot as plt
import seaborn as sns
from folium import plugins


def plot_coordinates(dataframe, save_path=None):
    """
    Dynamic plot of 'lat' and 'lon' coordinates.

    Parameters
    ----------
    dataframe: pd.DataFrame
        Source dataframe with 'lat' and 'lon' columns
    save_path: str or None
        Where the map will be saved
    """
    # Folium's Map object
    m = folium.Map(location=[dataframe.lat.mean(),
                             dataframe.lon.mean()], zoom_start=12)

    # Markers
    marker_cluster = plugins.MarkerCluster().add_to(m)

    # Iterate over df
    for i, obs in dataframe.iterrows():
        folium.Marker(location=[obs['lat'], obs['lon']]).add_to(marker_cluster)

    # Save map
    if save_path is not None:
        m.save(save_path)

    return m


def plot_correlations(dataframe):
    """
    Plot the correlations between features

    Parameter
    ---------
    dataframe : pd.DataFrame
        Dataframe with the data
    save_path : str
        Where the figure will be saved
    """
    corr_matrix = dataframe.corr()
    fig, ax = plt.subplots(figsize=(15, 10))

    # Heatmap
    sns.heatmap(corr_matrix, ax=ax, annot=True,
                fmt=".2f", cmap=plt.get_cmap('seismic'))
    return ax
