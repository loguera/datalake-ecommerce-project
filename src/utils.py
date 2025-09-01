from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, upper

def drop_null(data: DataFrame, column: str) -> DataFrame:
    """_summary_

    Args:
        data (_type_): _description_
        columns (_type_): _description_
    """
    
    return data.dropna(subset=column)

def drop_duplicate(data: DataFrame, column: str) -> DataFrame:
    """_summary_

    Args:
        data (DataFrame): _description_
        columns (list): _description_

    Returns:
        DataFrame: _description_
    """
    
    return data.drop_duplicates(subset=column)

def clean_string_column(data: DataFrame, column: str) -> DataFrame:
    """_summary_

    Args:
        data (DataFrame): _description_
        column (str): _description_

    Returns:
        DataFrame: _description_
    """
    
    return data.withcolumn(column, upper(trim(col(column))))

def filter_positive(data: DataFrame, column: str) -> DataFrame:
    """_summary_

    Args:
        data (DataFrame): _description_
        column (str): _description_

    Returns:
        DataFrame: _description_
    """
    
    return data.filter(col(column) > 0)

def fill_nulls(data: DataFrame, fill_map: dict) -> DataFrame:
    """_summary_

    Args:
        data (DataFrame): _description_
        fill_map (dict): _description_

    Returns:
        DataFrame: _description_
    """
    return data.fillna(fill_map)

def clean_negatives(data: DataFrame, column: str) -> DataFrame:
    """_summary_

    Args:
        data (DataFrame): _description_
        column (str): _description_

    Returns:
        DataFrame: _description_
    """
    
    return data.filter(data[column] > 0)