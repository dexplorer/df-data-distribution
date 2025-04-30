# df-data-distribution

This application runs a data distribution workflow to extract data from spark tables and write to a delimited file. 

It runs the configured pre-distribution and post-distribution (data quality checks) tasks as part of the workflow.

Also, it runs the data lineage task to capture the relationships. This is not optional.

Application can be invoked using CLI or REST API end points. This allows the app to be integrated into a larger data ingestion / distribution framework.

## Sub Modules
The data distribution service leverages the following services to perform the tasks in the data distribution pipeline.

[Metadata Management Service](https://github.com/dexplorer/df-metadata)

[Application Calendar Service](https://github.com/dexplorer/df-app-calendar)

[Data Quality Service](https://github.com/dexplorer/df-data-quality)

[Data Quality ML Service](https://github.com/dexplorer/df-data-quality-ml)

[Data Profiling Service](https://github.com/dexplorer/df-data-profile)

[Data Lineage Service](https://github.com/dexplorer/df-data-lineage)

## Data Flow

![Data Distribution Pipeline](docs/df-data-distribution.png?raw=true "Data Distribution Pipeline")

### Define the Environment Variables

Update one of the following .env files which is appropriate for the application hosting pattern.

```
.env.on_prem_vm_native
.env.aws_ec2_native
.env.aws_ec2_container
.env.aws_ecs_container
```

### Install

- **Install via Makefile and pip**:
  ```
    make install-dev
  ```

### Start the Spark Standalone Cluster

In df-spark project,
```sh
APP_INFRA_USER_NAME="ec2-user" APP_INFRA_USER_GROUP_NAME="ec2-user" docker-compose up --build
```

### Start the Data Management Services

In df-data-governance project,
```sh
docker-compose --project-name=df-spark up
```

### Start the Data Management Services Individually

##### Data Lineage Service
  ```sh
    docker run \
    --mount=type=bind,src=/nas,dst=/nas \
    -p 9091:9090 \
    --rm -it df-data-lineage:latest \
    dl-app-api --app_host_pattern "aws_ec2_container"
  ```

##### Data Profile Service
  ```sh
    docker run \
    --mount=type=bind,src=/nas,dst=/nas \
    -p 9092:9090 \
    --rm -it df-data-profile:latest \
    dp-app-api --app_host_pattern "aws_ec2_container"
  ```

##### Data Quality Service
  ```sh
    docker run \
    --mount=type=bind,src=/nas,dst=/nas \
    -p 9093:9090 \
    --rm -it df-data-quality:latest \
    dq-app-api --app_host_pattern "aws_ec2_container"
  ```

##### Data Quality ML Service
  ```sh
    docker run \
    --mount=type=bind,src=/nas,dst=/nas \
    -p 9094:9090 \
    --rm -it df-data-quality-ml:latest \
    dqml-app-api --app_host_pattern "aws_ec2_container"
  ```

##### Data Reconciliation Service
  ```sh
    docker run \
    --mount=type=bind,src=/nas,dst=/nas \
    -p 9095:9090 \
    --rm -it df-data-recon:latest \
    dr-app-api --app_host_pattern "aws_ec2_container"
  ```

### Usage Examples

- **Run a distribution workflow via CLI**:
  ```sh
    dist-app-cli --app_host_pattern "aws_ec2_native" run-distribution-workflow --distribution_workflow_id "workflow_11"
  ```

- **Run a distribution workflow via CLI with cycle date override**:
  ```sh
    dist-app-cli --app_host_pattern "aws_ec2_native" run-distribution-workflow --distribution_workflow_id "workflow_11" --cycle_date "2024-12-26"
  ```

- **Run a Distribution Workflow via API**:
  ##### Start the API server
  ```sh
    dist-app-api --app_host_pattern "aws_ec2_native"
  ```
  ##### Invoke the API endpoint
  ```sh
    https://<host name with port number>/run-distribution-workflow/?distribution_workflow_id=<value>
    https://<host name with port number>/run-distribution-workflow/?distribution_workflow_id=<value>&cycle_date=<value>

    /run-distribution-workflow/?distribution_workflow_id=workflow_11
    /run-distribution-workflow/?distribution_workflow_id=workflow_11&cycle_date=2024-12-26
  ```
  ##### Invoke the API from Swagger Docs interface
  ```sh
    https://<host name with port number>/docs
  ```

### Sample Input

  ##### Dataset (ext_asset_value_agg.sql)
```
select 
ta.effective_date 
, ta.asset_type 
, ta.asset_name 
, sum(cast(tac.asset_value as decimal(25, 2))) as asset_value_agg 
from dl_asset_mgmt.tasset ta 

left join dl_asset_mgmt.tacct_pos tac 
on ta.effective_date = tac.effective_date 
and ta.asset_id = tac.asset_id 

where ta.effective_date = ${effective_date_yyyy-mm-dd}

group by ta.effective_date, ta.asset_type, ta.asset_name 
order by ta.effective_date, ta.asset_type, ta.asset_name
;

```

### API Data (simulated)
These are metadata that would be captured via the Metadata Management UI and stored in a database.

  ##### Datasets 
```
{
  "datasets": [
    {
      "dataset_id": "dataset_4",
      "dataset_type": "spark sql file",
      "schedule_id": "schedule_2",
      "data_source_id": "data_source_4",
      "sql_file_path": "APP_SQL_SCRIPT_DIR/ext_asset_value_agg.sql"
    },
    {
      "dataset_id": "dataset_14",
      "dataset_type": "local delim file",
      "file_delim": "|",
      "file_path": "APP_DATA_OUT_DIR/asset_value_agg_yyyymmdd.dat",
      "schedule_id": "schedule_2",
      "data_source_id": "data_source_4",
      "recon_file_delim": null,
      "recon_file_path": null
    } 
  ]
}

```

  ##### Distribution Workflows 
```
{
    "workflows": [
      {
        "workflow_id": "workflow_11",
        "workflow_type": "distribution", 
        "distribution_task_id": "integration_task_11",
        "pre_tasks": [
        ],
        "post_tasks": [
          {
            "name": "data quality",
            "required_parameters": {
              "dataset_id": "dataset_14"
            }
          },
          {
            "name": "data quality ml",
            "required_parameters": {
              "dataset_id": "dataset_14"
            }
          }
        ]
      }
    ]
  }

```

  ##### Distribution Tasks 
```
{
    "integration_tasks": [
      {
        "task_id": "integration_task_11",
        "task_type": "distribution",
        "source_dataset_id": "dataset_4",
        "target_dataset_id": "dataset_14",
        "distribution_pattern": {
            "extracter": "spark",
            "source_type": "spark sql file", 
            "target_type": "local delim file" 
        } 
      }      
    ]
  }
  
```

### Sample Output 

  ##### Dataset (asset_value_agg_20241226.dat)
```
effective_date|asset_type|asset_name|asset_value_agg
2024-12-26|equity|HCL Tech|-65000.00
2024-12-26|mutual fund|Tata Digital Fund|-5000.00

```

  ##### Validation 
```
Extract is successful. Source Record Count = 2, Target Record Count (without header) = 2

```
