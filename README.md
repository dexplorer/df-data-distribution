# df-data-distribution

This application runs a data distribution workflow to extract data from spark tables and write to a delimited file. 

It runs the configured pre-distribution and post-distribution (data quality checks) tasks as part of the workflow.

Application can be invoked using CLI or REST API end points. This allows the app to be integrated into a larger data ingestion / distribution framework.

### Install

- **Install via Makefile and pip**:
  ```
    make install
  ```

### Usage Examples

- **Run a distribution workflow via CLI**:
  ```sh
    dist-app-cli run-distribution-workflow --distribution_workflow_id "1" --env "dev"
  ```

- **Run a distribution workflow via CLI with cycle date override**:
  ```sh
    dist-app-cli run-distribution-workflow --distribution_workflow_id "1" --env "dev" --cycle_date "2024-12-24"
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

    /run-distribution-workflow/?distribution_workflow_id=3
    /run-distribution-workflow/?distribution_workflow_id=1&cycle_date=2024-12-26
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
, sum(tac.asset_value) as asset_value_agg 
from dl_asset_mgmt.tassets ta 

left join dl_asset_mgmt.taccount_pos tac 
on ta.effective_date = tac.effective_date 
and ta.asset_id = tac.asset_id 

where ta.effective_date = ${effective_date_yyyy-mm-dd}

group by ta.effective_date, ta.asset_type, tac.asset_name 
order by ta.effective_date, ta.asset_type, tac.asset_name
;

```

### API Data (simulated)
These are metadata that would be captured via the Metadata Management UI and stored in a database.

  ##### datasets 
```
{
    "datasets": [
      {
        "dataset_id": "4",
        "dataset_kind": "spark sql file",
        "catalog_ind": false,
        "schedule_id": "2", 
        "dq_rule_ids": null, 
        "model_parameters": null, 
        "sql_file_path": "APP_ROOT_DIR/sql/ext_asset_value_agg.sql"
      }, 
      {
        "dataset_id": "14",
        "dataset_kind": "local delim file",
        "catalog_ind": true,
        "file_delim": "|",
        "file_path": "APP_ROOT_DIR/data/asset_value_agg_yyyymmdd.dat", 
        "schedule_id": "2", 
        "dq_rule_ids": null, 
        "model_parameters": null, 
        "recon_file_delim": null, 
        "recon_file_path": null 
      } 
    ]
  }

```

  ##### Distribution Workflows 
```
{
    "distribution_workflows": [
      {
        "workflow_id": "1",
        "workflow_kind": "distribution", 
        "distribution_task_id": "1",
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
    "distribution_tasks": [
      {
        "distribution_task_id": "1",
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

```

  ##### Validation 
```
Extract is successful. Source Record Count = 2, Target Record Count = 2

```
