from metadata import dataset as ds
from app_calendar import eff_date as ed
from metadata import workflow as wf
from metadata import integration_task as it

# from dist_app.settings import ConfigParms as sc
from config.settings import ConfigParms as sc
from dist_app.dist_spark import extracter as se

# Replace this with a API call in test/prod env
from dq_app import dq_app_core as dqc
from dqml_app import dqml_app_core as dqmlc
from dl_app import dl_app_core as dlc

import logging


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
    run_pre_distribution_tasks(
        tasks=distribution_workflow.pre_tasks, cycle_date=cycle_date
    )

    # Run distribution task
    logging.info(
        "Running the distribution task %s.", distribution_workflow.distribution_task_id
    )
    run_distribution_task(
        distribution_task_id=distribution_workflow.distribution_task_id,
        cycle_date=cycle_date,
    )

    # Run post-distribution tasks
    logging.info("Running the post-distribution tasks.")
    run_post_distribution_tasks(
        tasks=distribution_workflow.post_tasks, cycle_date=cycle_date
    )

    # Data lineage is required for all workflows
    run_data_lineage_task(workflow_id=distribution_workflow_id, cycle_date=cycle_date)


def run_distribution_task(distribution_task_id: str, cycle_date: str) -> None:
    # Simulate getting the distribution task metadata from API
    logging.info("Get distribution task metadata")
    distribution_task = it.DistributionTask.from_json(task_id=distribution_task_id)

    # Simulate getting the outbound dataset metadata from API
    logging.info("Get source dataset metadata")
    src_dataset = []
    if (
        distribution_task.distribution_pattern.source_type
        == ds.DatasetType.SPARK_SQL_FILE
    ):
        src_dataset = ds.SparkSqlFileDataset.from_json(
            dataset_id=distribution_task.source_dataset_id
        )

    # Prepare the sql file name
    sql_file_path = sc.resolve_app_path(src_dataset.sql_file_path)

    logging.info("Get target dataset metadata")
    tgt_dataset = []
    if (
        distribution_task.distribution_pattern.target_type
        == ds.DatasetType.LOCAL_DELIM_FILE
    ):
        tgt_dataset = ds.LocalDelimFileDataset.from_json(
            dataset_id=distribution_task.target_dataset_id
        )

    # Get current effective date
    cur_eff_date = ed.get_cur_eff_date(
        schedule_id=tgt_dataset.schedule_id, cycle_date=cycle_date
    )

    cur_eff_date_yyyymmdd = ed.fmt_date_str_as_yyyymmdd(cur_eff_date)

    # Prepare the target data file name
    target_file_path = sc.resolve_app_path(
        tgt_dataset.resolve_file_path(cur_eff_date_yyyymmdd)
    )

    # Extract data
    logging.info("Extracting data")
    records = se.extract_sql_to_file(
        target_file_path=target_file_path,
        target_file_delim=tgt_dataset.file_delim,
        sql_file_path=sql_file_path,
        cur_eff_date=cur_eff_date,
    )

    logging.info("%d records are written to %s.", records, target_file_path)


def run_data_quality_task(required_parameters: dict, cycle_date: str) -> None:
    dataset_id = required_parameters["dataset_id"]
    logging.info("Start applying data quality rules on the dataset %s", dataset_id)
    dq_check_results = dqc.apply_dq_rules(dataset_id=dataset_id, cycle_date=cycle_date)

    logging.info(
        "Finished applying data quality rules on the dataset %s",
        dataset_id,
    )

    logging.info("Data quality check results for dataset %s", dataset_id)
    logging.info(dq_check_results)


def run_data_quality_ml_task(required_parameters: dict, cycle_date: str) -> None:
    dataset_id = required_parameters["dataset_id"]
    logging.info("Started detecting anomalies in the dataset %s", dataset_id)
    column_scores = dqmlc.detect_anomalies(dataset_id=dataset_id, cycle_date=cycle_date)

    logging.info("Column/Feature scores for dataset %s", dataset_id)
    logging.info(column_scores)

    logging.info("Finished detecting anomalies in the dataset %s", dataset_id)


def run_data_lineage_task(workflow_id: str, cycle_date: str) -> None:
    logging.info(
        "Start capturing data lineage relationships forn the workflow %s", workflow_id
    )
    dl_relationships = dlc.capture_relationships(
        workflow_id=workflow_id, cycle_date=cycle_date
    )

    logging.info(
        "Finished capturing data lineage relationships for the workflow %s",
        workflow_id
    )

    logging.info("Data lineage relationships for the workflow %s", workflow_id)
    logging.info(dl_relationships)


def run_pre_distribution_tasks(tasks: list[wf.ManagementTask], cycle_date: str) -> None:
    if not tasks:
        logging.info("There are no pre distribution tasks to run.")
        logging.info("Cycle date is %s.", cycle_date)


def run_post_distribution_tasks(
    tasks: list[wf.ManagementTask], cycle_date: str
) -> None:
    for task in tasks:
        if task.name == "data quality":
            run_data_quality_task(
                required_parameters=task.required_parameters, cycle_date=cycle_date
            )
        elif task.name == "data quality ml":
            run_data_quality_ml_task(
                required_parameters=task.required_parameters, cycle_date=cycle_date
            )
