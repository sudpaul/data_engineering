# Databricks notebook source
# MAGIC %md
# MAGIC ##### Gold - Truncate Load Jinja2 template

# COMMAND ----------

def gld_truncate():
  return r"""
-- *----------------------------------------------*
-- STEP 3.1: Truncate Gold Table
-- *----------------------------------------------*
TRUNCATE TABLE {{template_params['main_database']}}.{{schema_dict['object_name']}};;

-- *----------------------------------------------*
-- STEP 3.2: Rename staging table as load table
-- *----------------------------------------------*
DROP TABLE IF EXISTS {{template_params['work_database']}}.{{template_params['worker_table']}}_load;;

ALTER TABLE {{template_params['work_database']}}.{{template_params['worker_table']}}_stg
RENAME TO {{template_params['work_database']}}.{{template_params['worker_table']}}_load;;

ANALYZE TABLE {{template_params['work_database']}}.{{template_params['worker_table']}}_load COMPUTE STATISTICS;;
"""