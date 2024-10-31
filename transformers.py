from pandas import DataFrame, to_datetime, Series, merge
from typing import Union


def calculate_percentage_of_identity_theft_online_per_year(
    data: DataFrame,
) -> DataFrame:
    """
    Calculate the percentage of identity theft incidents that occurred online per year.

    This function processes a DataFrame containing crime data, filters for identity theft
    incidents that occurred in cyberspace or on websites, and calculates the yearly percentage
    of these incidents relative to the total number of incidents.

    Args:
        data (DataFrame): A DataFrame containing crime data with columns "DATE OCC",
                          "Crm Cd Desc", and "Premis Desc".

    Returns:
        DataFrame: A DataFrame with columns "YEAR", "IDENTITY_THEFTS", "TOTAL", and "PERCENTAGE",
                   where "YEAR" is the year of the incidents, "IDENTITY_THEFTS" is the number of
                   identity theft incidents that occurred online, "TOTAL" is the total number of
                   incidents, and "PERCENTAGE" is the percentage of identity theft incidents that
                   occurred online compared to the total number of incidents.
    """
    data["YEAR"] = to_datetime(
        arg=data["DATE OCC"], format=r"%m/%d/%Y %I:%M:%S %p"  # 03/01/2020 12:00:00 AM
    ).dt.year

    identity_thefts: DataFrame = data[
        (data["Crm Cd Desc"] == "THEFT OF IDENTITY")
        & ((data["Premis Desc"] == "CYBERSPACE") | (data["Premis Desc"] == "WEBSITE"))
    ]

    yearly_identity_thefts: Union[DataFrame, Series[int]] = (
        identity_thefts.groupby(by="YEAR").size().reset_index()
    )

    yearly_data: Union[DataFrame, Series[int]] = (
        data.groupby(by="YEAR").size().reset_index()
    )

    result: DataFrame = merge(left=yearly_identity_thefts, right=yearly_data, on="YEAR")

    result.rename(columns={"0_x": "IDENTITY_THEFTS", "0_y": "TOTAL"}, inplace=True)

    result["PERCENTAGE"] = (result["IDENTITY_THEFTS"] / result["TOTAL"]) * 100

    return result
