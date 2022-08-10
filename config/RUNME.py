# Databricks notebook source
# MAGIC %md This notebook sets up the companion cluster(s) to run the solution accelerator with. It also creates the Workflow to create a Workflow DAG and illustrate the order of execution. Feel free to interactively run notebooks with the cluster or to run the Workflow to see how this solution accelerator executes. Happy exploring!
# MAGIC 
# MAGIC The pipelines, workflows and clusters created in this script are user-specific, so you can alter the workflow and cluster via UI without affecting other users. Running this script again after modification resets them.
# MAGIC 
# MAGIC **Note**: If the job execution fails, please confirm that you have set up other environment dependencies as specified in the accelerator notebooks. Accelerators sometimes require the user to set up additional cloud infra or data access, for instance. 

# COMMAND ----------

# DBTITLE 0,Install util packages
# MAGIC %pip install git+https://github.com/databricks-academy/dbacademy-rest git+https://github.com/databricks-academy/dbacademy-gems

# COMMAND ----------

from dbacademy.dbrest import DBAcademyRestClient
from dbacademy.dbgems import get_username, get_cloud, get_notebook_dir
import hashlib
import json
import re

# COMMAND ----------

# DBTITLE 1,Key cluster configs
spark_version = "10.4.x-cpu-ml-scala2.12"
num_workers = 2
node_type_dict = {"AWS": "i3.xlarge", "MSA": "Standard_D3_v2", "GCP": "n1-highmem-4"}

# COMMAND ----------

# DBTITLE 1,Companion job and cluster(s) definition
def get_job_param_json(env, solacc_path, job_name, node_type_id, spark_version, spark):
    
    # This job is not environment specific, so `env` is not used
    num_workers = 2
    
    # 1st DLT
    pipeline_name1 = f"SOLACC_rtb_lite_{current_user_sql}"
    dlt_definition_dict1 = {
          "clusters": [
              {
                  "label": "default",
                  "autoscale": {
                      "min_workers": 1,
                      "max_workers": 3
                  }
              }
          ],
          "development": True,
          "continuous": False,
          "edition": "advanced",
          "libraries": [
              {
                  "notebook": {
                      "path": f"{solacc_path}/01_dlt_real_time_bidding"
                  }
              }
          ],
          "name": pipeline_name1,
          "storage": f"{config_database_path}rtb_lite/dlt_{current_user_sql}",
          "target": f"SOLACC_rtb_lite_{current_user_sql}",
          "allow_duplicate_names": "true"
      }
    pipeline_id1 = create_or_update_pipeline_by_name(client, dlt_config_table, pipeline_name1, dlt_definition_dict1, spark)
    
    
    return {
        "timeout_seconds": 7200,
        "name": job_name,
        "max_concurrent_runs": 1,
        "tags": {
            "usage": "solacc_automation",
            "group": "CME_solacc_automation"
        },
        "tasks": [
            {
                "pipeline_task": {
                    "pipeline_id": pipeline_id1
                },
                "task_key": "rtb_lite_01",
                "description": ""
            },
          {
                "job_cluster_key": "rtb_lite_cluster",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": f"{solacc_path}/02_real_time_bidding_ml"
                },
                "task_key": "rtb_lite_02",
                "description": "",
                "depends_on": [
                    {
                        "task_key": "rtb_lite_01"
                    }
                ]
            }
        ],
        "job_clusters": [
            {
                "job_cluster_key": "rtb_lite_cluster",
                "new_cluster": {
                    "spark_version": spark_version,
                "spark_conf": {
                    "spark.databricks.delta.formatCheck.enabled": "false"
                    },
                    "num_workers": num_workers,
                    "node_type_id": node_type_id,
                    "custom_tags": {
            "usage": "solacc_automation",
            "group": "CME_solacc_automation"
        }
                }
            }
        ]
    }


# COMMAND ----------

# DBTITLE 1,Automated configs
current_cloud = get_cloud()
current_user = get_username()
client = DBAcademyRestClient()
current_user_sql = re.sub('\W', '_', current_user.split("@")[0])
solacc_path = get_notebook_dir()[:-7]
hash_code = hashlib.sha256(solacc_path.encode()).hexdigest()
solacc_name = solacc_path.split("/")[-1]
node_type_id = node_type_dict[current_cloud]
job_name = f"[SOLACC] {current_user_sql} {solacc_name} | {hash_code}"
config_database = f"databricks_solacc_{current_user_sql}"
dlt_config_table = f"{config_database}.dlt"
config_database_path = f"/databricks_solacc/{current_user}/"
pipeline_name = f"SOLACC_POS_DLT_{current_user_sql}"
env = "dev"

# COMMAND ----------

# DBTITLE 1,Initialize special config table for DLT-based accelerators
spark.sql(f"CREATE DATABASE IF NOT EXISTS {config_database} LOCATION '{config_database_path}'")
spark.sql(f"CREATE TABLE IF NOT EXISTS {dlt_config_table} (path STRING, pipeline_id STRING, solacc STRING)")

# COMMAND ----------

# DBTITLE 1,Util functions
# Note these functions assume that names for solacc jobs/cluster/pipelines are unique, which is guaranteed if solacc jobs/cluster/pipelines are created from this script only

def create_or_update_pipeline_by_name(client, dlt_config_table, pipeline_name, dlt_definition_dict, spark):
    """Look up a companion pipeline by name and edit with the given param and return pipeline id; create a new pipeline if a pipeline with that name does not exist"""
    dlt_id_pdf = spark.table(dlt_config_table).filter(f"solacc = '{pipeline_name}'").toPandas()
    assert len(dlt_id_pdf) <= 1, f"two pipelines with the same name {pipeline_name} exist in the {dlt_config_table} table; please manually inspect the table to make sure pipelines names are unique"
    pipeline_id = dlt_id_pdf['pipeline_id'][0] if len(dlt_id_pdf) > 0 else None
    if pipeline_id:
        dlt_definition_dict['id'] = pipeline_id
        print(f"Found dlt {pipeline_name} at '{pipeline_id}'; updating it with latest config if there is any change")
        client.execute_put_json(f"{client.endpoint}/api/2.0/pipelines/{pipeline_id}", dlt_definition_dict)
    else:
        response = DBAcademyRestClient().pipelines().create_from_dict(dlt_definition_dict)
        pipeline_id = response["pipeline_id"]
        # log pipeline id to the cicd dlt table: we use this delta table to store pipeline id information because looking up pipeline id via API can sometimes bring back a lot of data into memory and cause OOM error; this table is user-specific
        # Reusing the DLT pipeline allows for DLT run history to accumulate over time rather than to be wiped out after each deployment. DLT has some UI components that only show up after the pipeline is executed at least twice. 
        spark.createDataFrame([{"solacc": pipeline_name, "pipeline_id": pipeline_id}]).write.mode("append").option("mergeSchema", "True").saveAsTable(dlt_config_table)
        
    return pipeline_id
  
def create_or_update_job_by_name(client, params):
  """Look up the companion job by name and resets it with the given param and return job id; create a new job if a job with that name does not exist"""
  jobs = client.jobs().list()
  jobs_matched = list(filter(lambda job: job["settings"]["name"] == params["name"], jobs)) 
  assert len(jobs_matched) <= 1, f"""Two jobs with the same name {params["name"]} exist; please manually inspect them to make sure solacc job names are unique"""
  job_id = jobs_matched[0]["job_id"] if len(jobs_matched)  == 1 else None
  if job_id: 
    reset_params = {"job_id": job_id,
                   "new_settings": params}
    json_response = client.execute_post_json(f"{client.endpoint}/api/2.1/jobs/reset", reset_params) # returns {} if status is 200
    assert json_response == {}, "Job reset returned non-200 status"
    print(f"""Reset the {params["name"]} job with job_id {job_id} to original definition""")
  else:
    json_response = client.execute_post_json(f"{client.endpoint}/api/2.1/jobs/create", params)
    job_id = json_response["job_id"]
    print(f"""Created {params["name"]} job with job_id {job_id}""")
  
  return 

def create_or_update_cluster_by_name(client, params):
    """Look up a companion cluster by name and edit with the given param and return cluster id; create a new cluster if a cluster with that name does not exist"""
    clusters = client.execute_get_json(f"{client.endpoint}/api/2.0/clusters/list")["clusters"]
    clusters_matched = list(filter(lambda cluster: params["cluster_name"] == cluster["cluster_name"], clusters)) # cluster names are guaranteed unique by the Databricks platform
    cluster_id = clusters_matched[0]["cluster_id"] if len(clusters_matched) == 1 else None
    if cluster_id: 
      params["cluster_id"] = cluster_id
      json_response = client.execute_post_json(f"{client.endpoint}/api/2.0/clusters/edit", params) # returns {} if status is 200
      assert json_response == {}, "Job reset returned non-200 status"
      print(f"""Reset the {params["cluster_name"]} cluster with cluster_id {cluster_id} to original definition""")
    else:
      json_response = client.execute_post_json(f"{client.endpoint}/api/2.0/clusters/create", params)
      cluster_id = json_response["cluster_id"]
      print(f"""Created {params["cluster_name"]} cluster with cluster_id {cluster_id}""")
    return 
  
def convert_job_cluster_to_cluster(job_cluster_params):
  params = job_cluster_params["new_cluster"]
  params["cluster_name"] = f"""{current_user_sql}_{job_cluster_params["job_cluster_key"]}"""
  params["autotermination_minutes"] = 45
  return params

# COMMAND ----------

# DBTITLE 1,Create companion job and cluster(s)
job_params = get_job_param_json(env, solacc_path, job_name, node_type_id, spark_version, spark)
create_or_update_job_by_name(client, job_params)
for job_cluster_params in job_params["job_clusters"]:
  create_or_update_cluster_by_name(client, convert_job_cluster_to_cluster(job_cluster_params))
