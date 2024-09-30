from pyspark.sql import DataFrame
from pyspark.sql.window import WindowSpec
from pyspark.sql.functions import lag


def create_lag_columns(df: DataFrame, window_spec: WindowSpec) -> DataFrame:
    """
    Adds lag columns to a DataFrame based on a specified window.

    Parameters
    ----------
    df : DataFrame
        Input Spark DataFrame with at least the column "preco_medio_usd".
    window_spec : WindowSpec
        Window specification to use for calculating lag values.

    Returns
    -------
    DataFrame
        DataFrame with lag columns for the past 6 months.
    """
    df = df.withColumn("lag_1_mes_preco_medio_usd", lag("preco_medio_usd", 1).over(window_spec)) \
           .withColumn("lag_2_meses_preco_medio_usd", lag("preco_medio_usd", 2).over(window_spec)) \
           .withColumn("lag_3_meses_preco_medio_usd", lag("preco_medio_usd", 3).over(window_spec)) \
           .withColumn("lag_4_meses_preco_medio_usd", lag("preco_medio_usd", 4).over(window_spec)) \
           .withColumn("lag_5_meses_preco_medio_usd", lag("preco_medio_usd", 5).over(window_spec)) \
           .withColumn("lag_6_meses_preco_medio_usd", lag("preco_medio_usd", 6).over(window_spec))

    return df
