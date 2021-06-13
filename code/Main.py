import sys
import logging
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# create logger
logging.basicConfig()
log = logging.getLogger('kommatiPara')
log.setLevel(logging.DEBUG)

# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
FUNCTION_NAME = 'kommatiPara'

pd.set_option('display.width', 1000)
pd.set_option('display.max_columns', 1000)
pd.set_option('display.expand_frame_repr', True)
pd.set_option('max_colwidth', -1)

spark = SparkSession \
    .builder \
    .master('local[*]') \
    .appName("Kommatipara") \
    .config('spark.sql.execution.arrow.pyspark.enabled', True) \
    .config("spark.driver.host", "127.0.0.1") \
    .getOrCreate()


def get_df_from_paths(path):
    """
    Function to create spark dataframe from a given path
    :return: sparkdataframe
    """
    df = spark.read.format("csv").option("header", "true").option('delimiter', ',').load(path)
    return df


def rename_columns(df, col_rename):
    """
        Function to rename columns in spark dataframe
        :return: sparkdataframe
        """
    df_renamed = df.select([col(c).alias(col_rename.get(c, c)) for c in df.columns])

    return df_renamed


def main(argv):
    """
    Function call the read source file and applies bussiness transformations and writes back the output to target location
    :param argv: source_path,output_path,input_file_name,archive_path
    :return: NA
    """
    log.info('---- START ----')
    log.info(f'function name: {FUNCTION_NAME}')
    dateset1 = argv[0]
    dateset2 = argv[1]
    # country_list = (argv[2])
    country_list = ['Netherlands', 'United Kingdom']

    log.info('Reading clients data:')
    log.info('Filtering dataset based on the country list received:')
    df1 = get_df_from_paths(dateset1).filter(col("country").isin(country_list))
    log.info(df1.show(4, False))

    log.info('Reading financial data:')
    df2 = get_df_from_paths(dateset2)
    log.info(df2.show(10, False))

    log.info(f'creating a new dataset for countries : {country_list}')
    df = df1.join(df2, on=['id'], how='inner')
    df = df.select('id', 'email', 'country', 'btc_a', 'cc_t')
    log.info(df.show(10, False))

    log.info('renaming columns for user readability')
    col_rename = {"id": "client_identifier", "btc_a": "bitcoin_address", "cc_t": "credit_card_type"}
    df = rename_columns(df, col_rename)
    log.info(f'columns after renaming  : {df.columns}')

    log.info('writing newly created data to output location ')
    df.coalesce(1).write.format("com.databricks.spark.csv").mode("overwrite").option("header", "true").save(
        r'C:\Users\ashok.kumar.goa\Downloads\Programming Exercise\KommatiParaUseCase\data\client_data')
    log.info(df.show(10, False))
    log.info('Sucessfully written data to output ')

    log.info('\n---- FINISHED ----')


if __name__ == '__main__':

    """ main function call  which requires 3 arguments inputdatasetpath1, inputdatasetpath2, countrylist"""
    # note: *** here args count is 3, the first is by default the program name.
    if len(sys.argv) != 3:
        raise Exception('Incorrect number of arguments passed')
    log.info("Argument List: " + str(sys.argv))
    log.info("Number of arguments: " + str(len(sys.argv)) + " arguments.")
    main(sys.argv[1:])
