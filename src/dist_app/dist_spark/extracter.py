from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException

# from pyspark.sql.types import StructType, StructField, StringType, DecimalType
# from pyspark.sql import types as T
# from pyspark.sql.functions import col

from config.settings import ConfigParms as sc
from metadata import dataset as ds
from utils import file_io as uff
from utils import csv_io as ufc

import os
import tempfile

import logging


def create_spark_session(warehouse_path) -> SparkSession:
    # Initialize Spark session
    spark = (
        SparkSession.builder.appName("Spark Loader in Distribution Workflow")
        .config("spark.sql.warehouse.dir", warehouse_path)
        .enableHiveSupport()
        .getOrCreate()
    )

    # Enable dynamic partition overwrite
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    return spark


def resolve_sql_query(sql_query: str, key_value_map: dict) -> str:
    resolved_sql_query = sql_query

    for key, value in key_value_map.items():
        resolved_sql_query = resolved_sql_query.replace(key, value)

    return resolved_sql_query


def run_spark_sql(spark: SparkSession, resolved_sql_query: str):
    try:
        df = spark.sql(resolved_sql_query)
    except AnalysisException as error:
        if "[TABLE_OR_VIEW_NOT_FOUND]" in str(error):
            logging.error("Spark objects referenced in the sql query are not found.")
        logging.error(error)
        raise

    df.printSchema()
    df.show(2)
    return df


def write_df_to_file(df: DataFrame, target_file_path: str, target_file_delim: str):
    try:
        if target_file_delim in ds.FileDelimiter:

            with tempfile.TemporaryDirectory() as td:
                # Write a DataFrame into a CSV file
                (
                    # It is intentional to not combine the partitions as it could lead to OOM issue.
                    # .coalesce(numPartitions=1)
                    df.write.format("csv")
                    .option("header", "true")
                    .option("sep", target_file_delim)
                    .option("encoding", "UTF-8")
                    .option("dateFormat", "yyyy-MM-dd")
                    .mode("overwrite")
                    .save(path=td)
                )
                logging.info("Temporary directory: %s", td)
                logging.info(os.listdir(td))

                if os.path.exists(td):
                    ufc.merge_csv_files(in_file_dir_path=td, out_file=target_file_path)

                    if os.path.exists(target_file_path):
                        logging.info(
                            "Data frame is written to the file %s", target_file_path
                        )
                    else:
                        raise ValueError(
                            "Unable to merge the part files in the temp directory."
                        )
                else:
                    raise ValueError(
                        "Temp file directory does not exist. Data frame write failed."
                    )
        else:
            raise ValueError(f"File delimiter {target_file_delim} is not supported.")
    except ValueError as error:
        logging.error(error)
        raise


def validate_extract(df: DataFrame, file_path: str):
    num_records_in_file = 0
    with uff.uf_open_file(file_path=file_path, open_mode="r") as f:
        num_records_in_file = sum(1 for _ in f)

    # Exclude the header record from count
    target_record_count = num_records_in_file - 1
    source_record_count = df.count()

    if source_record_count == target_record_count:
        logging.info(
            "Extract is successful. Source Record Count = %d, Target Record Count (without header) = %d",
            source_record_count,
            target_record_count,
        )
    else:
        logging.error(
            "Extract is unsuccessful. Source Record Count = %d, Target Record Count (without header) = %d",
            source_record_count,
            target_record_count,
        )

    return target_record_count


def extract_sql_to_file(
    target_file_path: str,
    target_file_delim: str,
    sql_file_path: str,
    cur_eff_date: str,
):
    spark = create_spark_session(warehouse_path=sc.hive_warehouse_path)
    key_value_map = {
        "${effective_date_yyyy-mm-dd}": f"'{cur_eff_date}'",
    }
    sql_query = uff.uf_read_file_to_str(sql_file_path)
    resolved_sql_query = resolve_sql_query(
        sql_query=sql_query, key_value_map=key_value_map
    )
    print(resolved_sql_query)
    print(target_file_path)
    df = run_spark_sql(spark=spark, resolved_sql_query=resolved_sql_query)
    write_df_to_file(
        df=df, target_file_path=target_file_path, target_file_delim=target_file_delim
    )

    target_record_count = validate_extract(
        df=df,
        file_path=target_file_path,
    )

    return target_record_count
