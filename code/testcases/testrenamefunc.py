import pytest
from chispa import assert_df_equality
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .master("local")
         .appName("chispa")
         .config("spark.driver.host", "127.0.0.1") \
         .getOrCreate())


# def remove_non_word_characters(col):
#     return F.regexp_replace(col, "[^\\w\\s]+", "")


def rename_columns(df, col_rename):
    """
        Function to rename columns in spark dataframe
        :return: sparkdataframe
        """
    df_renamed = df.select([F.col(c).alias(col_rename.get(c, c)) for c in df.columns])

    return df_renamed


def test_rename_columns():
    df = spark.read.format("csv").option("header", "true").option('delimiter', ',').load("data/Input/dataset_one.csv")
    col_rename = {"id": "client_identifierx"}
    source_df = rename_columns(df, col_rename)
    print("ashok:" + str(source_df.columns))

    # Create Schema
    schema = StructType([
        StructField('client_identifier', StringType(), True),
        StructField('first_name', StringType(), True),
        StructField('last_name', StringType(), True),
        StructField('email', StringType(), True),
        StructField('country', StringType(), True)
    ])

    expected_df = spark.read.csv(path="data/Input/dataset_one.csv", schema=schema, header=True)
    expected_df.printSchema()
    assert_df_equality(source_df, expected_df)


test_rename_columns()
