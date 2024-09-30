import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.window import Window
from jobs.utils import create_lag_columns


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .appName("TestTransform") \
        .master("local[2]") \
        .getOrCreate()


def test_create_lag_columns(spark):
    # cria um DataFrame do pyspark de exemplo
    data = [
        Row(year=2023, month=1, preco_medio_usd=50.0),
        Row(year=2023, month=2, preco_medio_usd=55.0),
        Row(year=2023, month=3, preco_medio_usd=53.0),
        Row(year=2023, month=4, preco_medio_usd=58.0),
        Row(year=2023, month=5, preco_medio_usd=60.0),
    ]

    df = spark.createDataFrame(data)
    window_spec = Window.orderBy("year", "month")

    # chama a função para criar as colunas de lag
    result_df = create_lag_columns(df, window_spec)

    # verifica se as colunas de lag foram criadas
    assert "lag_1_mes_preco_medio_usd" in result_df.columns
    assert "lag_2_meses_preco_medio_usd" in result_df.columns
    assert "lag_3_meses_preco_medio_usd" in result_df.columns

    # verifica se os valores estão corretos
    result = result_df.collect()
    assert result[1]["lag_1_mes_preco_medio_usd"] == 50.0  # para fevereiro, o lag de 1 mês deve ser o preço de janeiro
    assert result[2]["lag_2_meses_preco_medio_usd"] == 50.0  # para março, o lag de 2 meses deve ser o preço de janeiro
