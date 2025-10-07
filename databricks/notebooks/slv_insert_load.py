# Databricks notebook source
# MAGIC %md
# MAGIC ##### Silver - Insert Load Jinja2 template

# COMMAND ----------

def slv_insert():
  return r"""
-- *----------------------------------------------*
-- STEP 4.1: Identify staging records to insert and silver records to update
-- *----------------------------------------------*
DROP TABLE IF EXISTS {{template_params['work_database']}}.slv_{{template_params['sourceName']|lower}}_{{schema_dict['File']['ObjectName']}}_load;;
CREATE TABLE {{template_params['work_database']}}.slv_{{template_params['sourceName']|lower}}_{{schema_dict['File']['ObjectName']}}_load
	AS
	SELECT *
	FROM {{template_params['work_database']}}.slv_{{template_params['sourceName']|lower}}_{{schema_dict['File']['ObjectName']}}_stg;;

ANALYZE TABLE {{template_params['work_database']}}.slv_{{template_params['sourceName']|lower}}_{{schema_dict['File']['ObjectName']}}_load COMPUTE STATISTICS;;

-- *----------------------------------------------*
-- STEP 4.2: Identify staging records which matches PK_Hash and Row_check_sum
-- *----------------------------------------------*
DELETE FROM {{template_params['work_database']}}.slv_{{template_params['sourceName']|lower}}_{{schema_dict['File']['ObjectName']}}_load Table1 WHERE EXISTS ( SELECT 1 FROM {{template_params['main_database']}}.{{schema_dict['File']['ObjectName']}} Table2 WHERE Table1.Pk_Hash = Table2.Pk_Hash AND Table1.Row_Hash = Table2.Row_Hash );;
"""
