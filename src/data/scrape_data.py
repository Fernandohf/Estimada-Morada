"""Scrapes the search URL and return the results as a DataFrame"""
from bs4 import BeautifulSoup
import pandas as pd
import requests


def get_position(navi_tree, string=True):
    """
    Check if the bs4 navigation tree has a value and return its value.

    Parameters
    -----------
    navi_tree:
        Navigation tree of bs4.

    Returns
    -------
    value: str
        value in the navigation tree.
    """
    try:
        value = navi_tree.string
    except AttributeError:
        value = None
    return value


def get_and_clean_data(page):
    """
    Collects and cleans property information from 'Vila Real' website, for a given search result webpage.

    Parameters
    ---------
    page: requests.Response object
        Result webpage with the information, result from request.get(url).

    Returns
    -------
    df: pd.DataFramae
        Dataframe with the information collected.
    """
    # BeautifulSoup object
    soup = BeautifulSoup(page.text, 'html.parser')

    # Main data holder
    list_of_dicts = []

    # Verifying results per page
    results_list = soup.find_all("div", class_="js-card-selector")
    assert(len(results_list) == 36)

    # Only results tags
    for tag in results_list:

        # Dictionary to save results
        data = dict()
        final_position = tag.find("article")

        # Type of Property
        type_value = final_position.h2.a['href']
        if "apartamento" in type_value:
            data['type'] = 'apartment'
        elif "casa" in type_value:
            data['type'] = 'house'
        else:
            data['type'] = 'other'

        # Address
        data['address'] = get_position(final_position.find("span", class_="js-property-card-address"))

        # Price
        data['price'] = get_position(final_position.find("div", class_="js-property-card-prices"))

        # Condo
        data['condo'] = get_position(final_position.section.find("strong", class_="js-condo-price"))

        # Area
        area_pos = final_position.find("li", class_="property-card__detail-area")
        data['area'] = get_position(area_pos.find("span", class_="property-card__detail-value"))

        # Bedrooms
        bedrooms_pos = final_position.find("li", class_="property-card__detail-room")
        data['bedrooms'] = get_position(bedrooms_pos.find("span", class_="property-card__detail-value"))

        # Suites
        suites_pos = final_position.find("li", class_="property-card__detail-item-extra")
        if suites_pos:
            data['suites'] = get_position(suites_pos.find("span", class_="property-card__detail-value"))
        else:
            data['suites'] = None

        # Bathrooms
        bath_pos = final_position.find("li", class_="property-card__detail-bathroom")
        data['bathrooms'] = get_position(bath_pos.find("span", class_="property-card__detail-value"))

        # Parking Spots
        park_pos = final_position.find("li", class_="property-card__detail-garage")
        data['parking_spots'] = get_position(park_pos.find("span", class_="property-card__detail-value"))

        list_of_dicts.append(data)

    # Create Pandas DataFrame Objects
    df = pd.DataFrame(list_of_dicts)

    # Clean DataFrame - Convert numeric values and clean strings
    string_cols = ['address', 'type']
    price_cols = ['price', 'condo']
    numeric_cols = df.columns.drop(string_cols)

    # Remove unwanted symbols from price
    for col in price_cols:
        df[col] = df[col].str.strip()
        df[col] = df[col].replace(r"\D", '', regex=True)

    # Convert to numeric
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce', downcast='integer')

    # Clean spaces from string values
    for col in string_cols:
        df[col] = df[col].str.strip()

    return df


def navigate(main_url, page_initial=1, page_final=500):
    """
    Requests many websites alterating the page result in the url.
    OBS.: the page_initial should be in the main_url.

    Parameters
    ---------
    main_url: str
        Complete url of the search result.
    page_initial: int, optional
        Numeric value of te initial page search result. (default to 1)
    page_final: int
        Numeric value for final page to navigate

    Returns
    -------
    df: pd.DataFrane
        Dataframe which value will be saved on.
    """
    # Create empty df
    df = pd.DataFrame()
    # Spit URL in static and dynamic parts
    url_splitted = main_url.split('pagina=' + str(page_initial))
    url_static = url_splitted[0] + 'pagina='
    url_dynamic = str(page_initial) + url_splitted[1]

    # Initial position
    curr_page = page_initial

    # Go through all URLs
    while True:
        # URL being explored
        url = url_static + url_dynamic
        webpage = requests.get(url)

        # Page not found interrupter
        if (not webpage.ok):
            # raise Exception('Page could not be accessed. HTML error code: ', webpage.status_code)
            print('Page could not be accessed. HTML error code: {}'.format(webpage.status_code))
            break

        # Final page reached interrupt
        if curr_page > page_final:
            print("Navigation reached the last result page.")
            break

        # Append the gathered data
        df = df.append(get_and_clean_data(webpage))

        # Edit URL
        curr_page += 1
        url_dynamic = str(curr_page) + url_splitted[1]

    # Return df
    return df


if __name__ == '__main__':
    # get_and_clean_data('https://www.vivareal.com.br/venda/?__vt=ranking%3Avisit')
    URL = 'https://www.vivareal.com.br/venda/rio-grande-do-norte/natal/?pagina=1'

    results_df = navigate(URL, 1, 5)

    print(results_df.head())
    print(results_df.info())

    # results_df.to_csv("DataSet_Natal_test.csv", index=False)
