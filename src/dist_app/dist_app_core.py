from metadata import dataset as ds
from metadata import data_source as dsrc
from app_calendar import eff_date as ed
from metadata import workflow as wf
from metadata import integration_task as it
from config.settings import ConfigParms as sc
from utils import spark_io as ufs
from gov import gov_tasks as dg

import logging
import os


def run_distribution_workflow(distribution_workflow_id: str, cycle_date: str) -> None:
    # Simulate getting the cycle date from API
    # Run this from the parent app
    if not cycle_date:
        cycle_date = ed.get_cur_cycle_date()

    # Simulate getting the distribution workflow metadata from API
    logging.info("Get distribution workflow metadata")
    distribution_workflow = wf.DistributionWorkflow.from_json(
        workflow_id=distribution_workflow_id
    )

    # Run pre-distribution tasks
    logging.info("Running the pre-distribution tasks.")
    pre_distribution_task_results = dg.run_management_tasks(
        tasks=distribution_workflow.pre_tasks, cycle_date=cycle_date
    )

    # Run distribution task
    logging.info(
        "Running the distribution task %s.", distribution_workflow.distribution_task_id
    )
    distribution_task_status_code, distribution_task_results = run_distribution_task(
        distribution_task_id=distribution_workflow.distribution_task_id,
        cycle_date=cycle_date,
    )

    # Run post-distribution tasks
    if not distribution_task_status_code:
        logging.info("Running the post-distribution tasks.")
        post_distribution_task_results = dg.run_management_tasks(
            tasks=distribution_workflow.post_tasks, cycle_date=cycle_date
        )

    # Data lineage is required for all workflows
    dl_results = dg.run_data_lineage_task(
        workflow_id=distribution_workflow_id, cycle_date=cycle_date
    )

    results = pre_distribution_task_results
    if not distribution_task_status_code:
        results.append(distribution_task_results)

    if not distribution_task_status_code:
        results.append(post_distribution_task_results)

    results.append(dl_results)

    logging.debug(results)
    return results


def run_distribution_task(distribution_task_id: str, cycle_date: str) -> None:
    # Simulate getting the distribution task metadata from API
    logging.info("Get distribution task metadata")
    distribution_task = it.DistributionTask.from_json(task_id=distribution_task_id)

    # Simulate getting the outbound dataset metadata from API
    logging.info("Get source dataset metadata")
    source_dataset = ds.get_dataset_from_json(
        dataset_id=distribution_task.source_dataset_id
    )

    # Simulate getting the data source metadata from API
    data_source = dsrc.get_data_source_from_json(
        data_source_id=source_dataset.data_source_id
    )

    # Prepare the sql file name
    if source_dataset.dataset_type == ds.DatasetType.SPARK_SQL_FILE:
        sql_file_path = sc.resolve_app_path(source_dataset.sql_file_path)
    else:
        raise RuntimeError("Source dataset type is not expected.")

    logging.info("Get target dataset metadata")
    target_dataset = ds.get_dataset_from_json(
        dataset_id=distribution_task.target_dataset_id
    )

    # Get current effective date
    cur_eff_date = ed.get_cur_eff_date(
        schedule_id=target_dataset.schedule_id, cycle_date=cycle_date
    )

    cur_eff_date_yyyymmdd = ed.fmt_date_str_as_yyyymmdd(cur_eff_date)

    # Prepare the target data file name
    if target_dataset.dataset_type == ds.DatasetType.LOCAL_DELIM_FILE:
        target_file_path = sc.resolve_app_path(
            target_dataset.resolve_file_path(
                date_str=cur_eff_date_yyyymmdd,
                data_source_user=data_source.data_source_user,
            )
        )
    elif target_dataset.dataset_type == ds.DatasetType.AWS_S3_DELIM_FILE:
        target_file_path = sc.resolve_app_path(
            target_dataset.resolve_file_uri(
                date_str=cur_eff_date_yyyymmdd,
                data_source_user=data_source.data_source_user,
            )
        )
    else:
        raise RuntimeError("Target dataset type is not expected.")

    debug = "n"

    # Extract data
    logging.info("Extracting data")

    spark_submit_command_str = f"""
    {os.environ['SPARK_HOME']}/bin/spark-submit \
    --master {sc.spark_master_uri} \
    --deploy-mode {sc.spark_deploy_mode} \
    --num-executors 1 \
    --executor-memory=2g \
    --executor-cores 1 \
    {sc.app_root_dir}/src/pyspark_apps/extracter.py \
    --warehouse_path={sc.spark_warehouse_path} \
    --spark_master_uri={sc.spark_master_uri} \
    --spark_history_log_dir={sc.spark_history_log_dir} \
    --spark_local_dir={sc.spark_local_dir} \
    --postgres_uri={sc.postgres_uri} \
    --sql_file_path={sql_file_path} \
    --target_file_path={target_file_path} \
    --target_file_delim={target_dataset.file_delim} \
    --target_temp_dir={sc.app_data_out_dir} \
    --cur_eff_date={cur_eff_date} \
    --debug={debug}
    """
    # ufs.spark_submit_with_callback(
    #     command=spark_submit_command_str, callback=spark_submit_callback
    # )
    status_code = ufs.spark_submit_with_wait(command=spark_submit_command_str)

    distribution_task_results = [
        {
            "distribution task id": distribution_task_id,
            "source dataset name": sql_file_path,
            "target dataset name": target_file_path,
            "source record count": 0,
            "target record count": 0,
        }
    ]
    return status_code, distribution_task_results


def run_pre_distribution_tasks(
    tasks: list[wf.ManagementTask], cycle_date: str
) -> list[dict]:
    pre_distribution_task_results = []
    pre_distribution_task_results = dg.run_management_tasks(
        tasks=tasks, cycle_date=cycle_date
    )
    return pre_distribution_task_results


def run_post_distribution_tasks(
    tasks: list[wf.ManagementTask], cycle_date: str
) -> list[dict]:
    post_distribution_task_results = []
    post_distribution_task_results = dg.run_management_tasks(
        tasks=tasks, cycle_date=cycle_date
    )
    return post_distribution_task_results
