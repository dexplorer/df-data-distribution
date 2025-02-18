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

### Install

- **Install via Makefile and pip**:
  ```
    make install
  ```

### Usage Examples

- **Run a distribution workflow via CLI**:
  ```sh
    dist-app-cli run-distribution-workflow --distribution_workflow_id "11" --env "dev"
  ```

- **Run a distribution workflow via CLI with cycle date override**:
  ```sh
    dist-app-cli run-distribution-workflow --distribution_workflow_id "11" --env "dev" --cycle_date "2024-12-24"
  ```

- **Run a distribution workflow via API**:
  ##### Start the API server
  ```sh
    dist-app-api --env "dev"
  ```
  ##### Invoke the API endpoint
  ```sh
    https://<host name with port number>/run-distribution-workflow/?distribution_workflow_id=<value>
    https://<host name with port number>/run-distribution-workflow/?distribution_workflow_id=<value>&cycle_date=<value>

    /run-distribution-workflow/?distribution_workflow_id=11
    /run-distribution-workflow/?distribution_workflow_id=11&cycle_date=2024-12-26
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
        "dataset_id": "4",
        "dataset_type": "spark sql file",
        "catalog_ind": false,
        "schedule_id": "2",
        "sql_file_path": "APP_SQL_SCRIPT_DIR/ext_asset_value_agg.sql"
      },
      {
        "dataset_id": "14",
        "dataset_type": "local delim file",
        "catalog_ind": true,
        "file_delim": "|",
        "file_path": "APP_DATA_OUT_DIR/asset_value_agg_yyyymmdd.dat",
        "schedule_id": "2",
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
        "workflow_id": "11",
        "workflow_type": "distribution", 
        "distribution_task_id": "11",
        "pre_tasks": [
        ],
        "post_tasks": [
          {
            "name": "data quality",
            "required_parameters": {
              "dataset_id": "14"
            }
          },
          {
            "name": "data quality ml",
            "required_parameters": {
              "dataset_id": "14"
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
        "task_id": "11",
        "task_type": "distribution",
        "source_dataset_id": "4",
        "target_dataset_id": "14",
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
