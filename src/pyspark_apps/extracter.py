from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException
from metadata import dataset as ds
from utils import file_io as uff
from utils import csv_io as ufc
import os
import tempfile
import logging
import argparse
from utils import spark_io as ufs


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


def write_df_to_file(df: DataFrame, target_file_path: str, target_file_delim: str, target_temp_dir: str):
    try:
        if target_file_delim in ds.FileDelimiter:

            with tempfile.TemporaryDirectory(dir=target_temp_dir) as td:
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


def view_written_data_sample(file_path: str):
    with uff.uf_open_file(file_path=file_path, open_mode="r") as f:
        for _ in range(3):
            line = f.readline()
            if not line:
                break  # Stop if end of file is reached
            print(line, end="")


def extract_sql_to_file(
    spark: SparkSession,
    target_file_path: str,
    target_file_delim: str,
    target_temp_dir: str, 
    sql_file_path: str,
    cur_eff_date: str,
    debug: str,    
):
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
        df=df, target_file_path=target_file_path, target_file_delim=target_file_delim, target_temp_dir=target_temp_dir
    )

    if debug == "y":
        view_written_data_sample(file_path=target_file_path)

    target_record_count = validate_extract(
        df=df,
        file_path=target_file_path,
    )

    return target_record_count


def main():
    parser = argparse.ArgumentParser(description="Spark Application - Loader")
    parser.add_argument(
        "--warehouse_path",
        help="Hive or Spark warehouse path.",
        nargs=None,  # 1 value
        required=True,
    )
    parser.add_argument(
        "--spark_master_uri",
        help="Spark resource or cluster manager",
        nargs=None,  # 1 value
        required=True,
    )
    parser.add_argument(
        "--spark_history_log_dir",
        help="Spark log events directory.",
        nargs=None,  # 1 value
        required=True,
    )
    parser.add_argument(
        "--spark_local_dir",
        help="Spark local work directory.",
        nargs=None,  # 1 value
        required=True,
    )
    parser.add_argument(
        "--postgres_uri",
        help="Postgres host which is used as hive metastore.",
        nargs=None,  # 1 value
        required=True,
    )
    parser.add_argument(
        "--sql_file_path",
        help="",
        nargs=None,  # 1 value
        required=True,
    )
    parser.add_argument(
        "--target_file_path",
        help="",
        nargs=None,  # 1 value
        required=True,
    )
    parser.add_argument(
        "--target_file_delim",
        help="",
        nargs=None,  # 1 value
        required=True,
    )
    parser.add_argument(
        "--target_temp_dir",
        help="",
        nargs=None,  # 1 value
        required=True,
    )
    parser.add_argument(
        "--cur_eff_date",
        help="",
        nargs=None,  # 1 value
        required=True,
    )
    parser.add_argument(
        "--debug",
        help="Set the logging level to DEBUG",
        nargs="?",  # 0-or-1 argument values
        const="y",  # default when the argument is provided with no value
        default="n",  # default when the argument is not provided
        required=False,
    )

    # Get the arguments
    args = vars(parser.parse_args())
    warehouse_path = args["warehouse_path"]
    spark_master_uri = args["spark_master_uri"]
    spark_history_log_dir = args["spark_history_log_dir"]
    spark_local_dir = args["spark_local_dir"]
    postgres_uri = args["postgres_uri"]
    sql_file_path = args["sql_file_path"]
    target_file_path = args["target_file_path"]
    target_file_delim = args["target_file_delim"]
    target_temp_dir = args["target_temp_dir"]
    cur_eff_date = args["cur_eff_date"]
    debug = args["debug"]

    if debug == "y":
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO
    logging.info("Requested log level: %s", str(log_level))

    # Create Spark session
    spark = ufs.create_spark_session(
        warehouse_path=warehouse_path,
        spark_master_uri=spark_master_uri,
        spark_history_log_dir=spark_history_log_dir,
        spark_local_dir=spark_local_dir,
        postgres_uri=postgres_uri,
    )

    # Extract to file
    records = extract_sql_to_file(
        spark=spark,
        target_file_path=target_file_path,
        target_file_delim=target_file_delim,
        target_temp_dir=target_temp_dir, 
        sql_file_path=sql_file_path,
        cur_eff_date=cur_eff_date,
        debug=debug,
    )

    logging.info("%d records are extracted into %s.", records, target_file_path)


if __name__ == "__main__":
    main()
