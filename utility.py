from __future__ import annotations
from pandas import read_csv, DataFrame, Series, concat
from typing import Callable, Union, Dict, Any, Optional, List
from os import PathLike
import seaborn

FilePath = Union[str, PathLike[str]]


def _read_csv(file_name: FilePath) -> DataFrame:
    """
    Reads a CSV file into a DataFrame.

    Args:
        file_name (FilePath): The path to the CSV file to be read.

    Returns:
        DataFrame: The DataFrame containing the data from the CSV file.
    """
    return read_csv(filepath_or_buffer=file_name)


def _apply_column_filters(data: DataFrame, columns: Dict[str, Any]) -> DataFrame:
    """
    Filters the given DataFrame based on the specified columns and values.

    Applies a mask to the DataFrame based on the filter criteria. The mask is created
    by comparing the value in each row of the DataFrame with the value specified in the
    filter criteria. The mask is then applied to the DataFrame to return a new DataFrame
    containing only the rows that match the filter criteria. If no filter criteria are
    provided, the original DataFrame is returned.

    Args:
        data (DataFrame): The input DataFrame to be filtered.
        filter (list[str]): A list of filter criteria in the format ["column_name=value", ...].

    Returns:
        DataFrame: A new DataFrame that contains only the rows that match the filter criteria.
    """
    mask = Series(data=[True] * len(data), index=data.index)
    for column, value in columns.items():
        mask &= data[column] == value
    return data[mask]


def _apply_field_filters(data: DataFrame, fields: Dict[str, Any]) -> DataFrame:
    """
    Filters the given DataFrame based on the specified fields and values.

    Applies a mask to the DataFrame based on the filter criteria. The mask is created
    by comparing the value in each column of the DataFrame with the value specified in the
    filter criteria. The mask is then applied to the DataFrame to return a new DataFrame
    containing only the columns that match the filter criteria. If no filter criteria are
    provided, the original DataFrame is returned.

    Args:
        data (DataFrame): The input DataFrame to be filtered.
        filter (list[str]): A list of filter criteria in the format ["field_name=value", ...].

    Returns:
        DataFrame: A new DataFrame that contains only the columns that match the filter criteria.
    """
    columns = list(fields.keys())
    return data[columns]


def extract(
    file_name: FilePath,
    columns: Optional[Dict[str, Any]] = None,
    fields: Optional[Dict[str, Any]] = None,
    sort_by: Optional[List[str]] = None,
) -> DataFrame:
    """
    Extracts data from a CSV file, applies optional column and field filters, and sorts the data.

    Reads the data from the specified CSV file into a DataFrame. Optionally, applies column and
    field filters to the data based on the specified filter criteria. If sort_by is specified,
    sorts the data by the specified columns. Returns the extracted, filtered, and sorted data.
    Column and field filters are specified using a key-value pair where the key is the column or
    field name and the value is the filter value. Sort columns are specified as a list of column
    names.

    Args:
        file_name (FilePath): The path to the CSV file to be read.
        columns (Optional[Dict[str, Any]], optional): A dictionary specifying columns to filter. Defaults to None.
        fields (Optional[Dict[str, Any]], optional): A dictionary specifying fields to filter. Defaults to None.
        sort_by (Optional[List[str]], optional): A list of column names to sort by. Defaults to None.

    Returns:
        DataFrame: The extracted and optionally filtered and sorted data.

    Notes:
        The column filter is applied before the field filter. If both filters are specified, the
        column filter is applied first, followed by the field filter. If no filters are specified,
        the original data is returned. The filters use logical AND to combine multiple filter criteria.
    """
    data: DataFrame = _read_csv(file_name=file_name)

    if columns:
        data = _apply_column_filters(data=data, columns=columns)

    if fields:
        data = _apply_field_filters(data=data, fields=fields)

    if sort_by:
        data = data.sort_values(by=sort_by)

    return data


def load(data: DataFrame, file_name: FilePath) -> None:
    """
    Save a DataFrame to a CSV file.

    Parameters:
    data (DataFrame): The DataFrame to be saved.
    file_name (FilePath): The path where the CSV file will be saved.

    Returns:
    None
    """
    data.to_csv(path_or_buf=file_name, index=False)


def transform(
    data: DataFrame, transformer: Callable[[DataFrame], DataFrame]
) -> DataFrame:
    """
    Transforms the given DataFrame using the specified transformer function.

    Applies the specified transformer function to the input DataFrame to perform
    the transformation. The transformer function should take a DataFrame as input
    and return a DataFrame as output. Returns the transformed DataFrame.

    Args:
        data (DataFrame): The input DataFrame to be transformed.
        transformer (Callable[[DataFrame], DataFrame]): The transformer function to apply.

    Returns:
        DataFrame: The transformed DataFrame.
    """
    return transformer(data)


def run_ELT(
    file_path: FilePath,
    output_path: FilePath,
    transformer: Callable[[DataFrame], DataFrame],
) -> None:
    """
    Extracts, transforms, and loads data using the specified transformer function.

    Reads the data from the specified CSV file into a DataFrame, applies the specified
    transformer function to the data, and saves the transformed data back to the CSV file.

    Args:
        file_path (FilePath): The path to the CSV file to be read and written.
        output_path (FilePath): The path where the transformed CSV file will be saved
        transformer (Callable[[DataFrame], DataFrame]): The transformer function to apply.

    Returns:
        None
    """
    data: DataFrame = _read_csv(file_name=file_path)
    transformed_data: DataFrame = transformer(data)
    load(data=transformed_data, file_name=output_path)


def combine_dataframes(
    dataframes: List[DataFrame], sort_by: Optional[List[str]] = None
) -> DataFrame:
    """
    Combines a list of DataFrames into a single DataFrame.

    Concatenates the list of DataFrames along the row axis to create a single
    DataFrame that contains all the rows from the input DataFrames.

    Args:
        dataframes (List[DataFrame]): A list of DataFrames to be combined.

    Returns:
        DataFrame: The combined DataFrame containing all the rows from the input DataFrames.
    """
    combined_data: DataFrame = concat(dataframes, ignore_index=True)

    if sort_by:
        combined_data = combined_data.sort_values(by=sort_by)

    return combined_data


def create_graph(data: DataFrame, x: str, y: str, output_file: FilePath) -> None:
    """
    Creates a graph from the given DataFrame and saves it to a file.

    Parameters:
    data (DataFrame): The DataFrame containing the data to be plotted.
    x (str): The column name to be plotted on the x-axis.
    y (str): The column name to be plotted on the y-axis.
    hue (str): The column name to be used for color encoding.
    output_file (FilePath): The path where the graph image will be saved.

    Returns:
    None
    """
    seaborn.set_theme(style="whitegrid")
    plot = seaborn.lineplot(data=data, x=x, y=y)
    plot.get_figure().savefig(output_file)
