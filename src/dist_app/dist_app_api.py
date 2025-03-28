import os
import logging
from dotenv import load_dotenv
from config.settings import ConfigParms as sc
from dist_app import dist_app_core as ddc
from utils import logger as ufl

from fastapi import FastAPI
import uvicorn

app = FastAPI()


@app.get("/")
async def root():
    """
    Default route

    Args:
        none

    Returns:
        A default message.
    """

    return {"message": "Data Ingestion App"}


@app.get("/run-distribution-workflow/")
async def run_distribution_workflow(
    distribution_workflow_id: str, cycle_date: str = ""
):
    """
    Runs the distribution workflow.

    Args:
        dataset_id: Id of the dataset.
        cycle_date: Cycle date

    Returns:
        Results from the data reconciliation validations.
    """

    logging.info("Running the distribution workflow %s", distribution_workflow_id)
    ddc.run_distribution_workflow(
        distribution_workflow_id=distribution_workflow_id, cycle_date=cycle_date
    )
    logging.info(
        "Finished running the distribution workflow %s", distribution_workflow_id
    )

    return {"return_code": 0}


def main():
    # Load the environment variables from .env file
    load_dotenv()

    # Fail if env variable is not set
    sc.load_config()

    script_name = os.path.splitext(os.path.basename(__file__))[0]
    ufl.config_logger(log_file_path_name=f"{sc.app_log_dir}/{script_name}.log")
    logging.info("Configs are set")
    logging.info(os.environ)
    logging.info(sc.config)
    logging.info(vars(sc))

    logging.info("Starting the API service")

    uvicorn.run(
        app,
        port=8080,
        host="0.0.0.0",
        log_config=f"{sc.app_config_dir}/api_log.ini",
    )

    logging.info("Stopping the API service")


if __name__ == "__main__":
    main()
