from pandas import DataFrame, to_datetime, Series, merge, Categorical
from typing import Union
from calendar import month_name


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
    # Convert the "DATE OCC" column to a datetime object and extract the year
    data["YEAR"] = to_datetime(
        arg=data["DATE OCC"], format=r"%m/%d/%Y %I:%M:%S %p"  # 03/01/2020 12:00:00 AM
    ).dt.year

    # Filter for identity theft incidents that occurred in cyberspace or on websites
    identity_thefts: DataFrame = data[
        (
            data["Crm Cd Desc"].isin(values=["THEFT OF IDENTITY"])
            & data["Premis Desc"].isin(values=["CYBERSPACE", "WEBSITE"])
        )
    ]

    # Group the data by year and count the number of identity theft incidents
    yearly_identity_thefts: Union[DataFrame, Series[int]] = (
        identity_thefts.groupby(by="YEAR").size().reset_index()
    )

    # Group the data by year and count the total number of incidents
    yearly_data: Union[DataFrame, Series[int]] = (
        data.groupby(by="YEAR").size().reset_index()
    )

    # Merge the two DataFrames on the "YEAR" column, rename the columns,
    # and calculate the percentage difference
    result: DataFrame = merge(left=yearly_identity_thefts, right=yearly_data, on="YEAR")
    result.rename(columns={"0_x": "IDENTITY_THEFTS", "0_y": "TOTAL"}, inplace=True)
    result["PERCENTAGE"] = (result["IDENTITY_THEFTS"] / result["TOTAL"]) * 100

    return result


def calculate_burglaries_at_construction_sites_per_month(
    data: DataFrame,
) -> DataFrame:
    """
    Calculate the number of burglaries that occurred at construction sites for each month in each year.

    This function processes a DataFrame containing crime data, filters for burglaries that occurred
    at construction sites, and calculates the number of burglaries that occurred at construction sites
    for each month in each year.

    Args:
        data (DataFrame): A DataFrame containing crime data with columns "DATE OCC",
                          "Crm Cd Desc", and "Premis Desc".

    Returns:
        DataFrame: A DataFrame with columns "MONTH", "BURGLARIES", where "MONTH" is the month
                   of the incidents, and "BURGLARIES" is the number of burglaries that occurred
                   at construction sites.
    """
    # Convert the "Date" column to a datetime object
    data["Date"] = to_datetime(
        arg=data["Date"], format=r"%m/%d/%Y %I:%M:%S %p"  # 03/01/2020 12:00:00 AM
    )
    data["MONTH"] = data["Date"].dt.month_name()
    data["YEAR"] = data["Date"].dt.year

    # Filter for burglaries that occurred at construction sites
    burglaries: DataFrame = data[
        (data["Primary Type"].isin(values=["BURGLARY"]))
        & data["Location Description"].isin(values=["CONSTRUCTION SITE"])
    ]

    # Group the data by year and month and count the number of burglaries
    monthly_burglaries: DataFrame = (
        burglaries.groupby(by=["YEAR", "MONTH"]).size().reset_index()
    )

    monthly_burglaries.rename(columns={0: "BURGLARIES"}, inplace=True)

    monthly_burglaries["MONTH"] = Categorical(
        values=monthly_burglaries["MONTH"],
        categories=month_name[1:],
        ordered=True,
    )

    monthly_burglaries.sort_values(by=["YEAR", "MONTH"], inplace=True)

    return monthly_burglaries
