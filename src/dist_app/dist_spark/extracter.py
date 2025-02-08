from pyspark.sql import SparkSession, DataFrame

# from pyspark.sql.types import StructType, StructField, StringType, DecimalType
# from pyspark.sql import types as T

# from pyspark.sql.functions import col

from dist_app.settings import ConfigParms as sc
from metadata import dataset as ds
from utils import file_io as uff

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
    df = spark.sql(resolved_sql_query)
    df.printSchema()
    df.show(2)
    return df


def write_df_to_file(df: DataFrame, target_file_path: str, target_file_delim: str):
    try:
        if target_file_delim in ds.FileDelimiter:
            (
                df.coalesce()
                .write.format("csv")
                .option("header", "true")
                .option("sep", target_file_delim)
                .option("encoding", "UTF-8")
                .option("dateFormat", "yyyy-MM-dd")
                .mode("overwrite")
                .save(path=target_file_path)
            )

        else:
            raise ValueError(f"File delimiter {target_file_delim} is not supported.")
    except ValueError as error:
        logging.error(error)
        raise


def validate_extract(df: DataFrame, file_path: str):
    num_records_in_file = 0
    with uff.uf_open_file(file_path=file_path, open_mode="rb") as f:
        num_records_in_file = sum(1 for _ in f)

    target_record_count = num_records_in_file
    source_record_count = df.count()

    if source_record_count == target_record_count:
        logging.info(
            "Extract is successful. Source Record Count = %d, Target Record Count = %d",
            source_record_count,
            target_record_count,
        )
    else:
        logging.error(
            "Extract is unsuccessful. Source Record Count = %d, Target Record Count = %d",
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
    spark = create_spark_session(warehouse_path=sc.warehouse_path)
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
