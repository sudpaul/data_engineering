# Databricks notebook source
# MAGIC %md
# MAGIC ##### Gold - Reference Load Start Jinja2 template

# COMMAND ----------


def gld_reference_start():
  return r"""
  -- *----------------------------------------------*
  -- STEP 1: Create/Update Src_App and Rfrnc_Type tables
  -- *----------------------------------------------*
  {%- for table in ('Src_App','Rfrnc_Type') -%}
  {##}
  CREATE OR REPLACE TABLE {{template_params['work_database']}}.gld_{{schema_dict['schema_name']|lower}}_{{table|lower}}_{{template_params['pipeline_run_id']}}_stg
  USING DELTA AS
  SELECT
    secure_hash(array(`{{table}}_Nm`{% if table == 'Rfrnc_Type' %},`{{table}}_Cd`{% endif %}),'|') AS `Pk_Hash`
    ,secure_hash(array(`{{table}}_Shrt_Dsc`,`{{table}}_Long_Dsc`),'|') AS `Row_Hash`
    ,CAST('1900-01-01 00:00:00' AS TIMESTAMP) AS `Effective_Dttm` 
    ,CAST('9999-12-31 00:00:00' AS TIMESTAMP) AS `Expiry_Dttm`
    ,{%- if table == 'Rfrnc_Type' -%} "Common" {%- else -%} `Src_App_Nm` {%- endif %} AS `Source_App_Name` 
    ,'I' AS `Record_Type`
    ,CAST('{{template_params['process_start_time_stamp']}}' AS TIMESTAMP) AS `Record_Insert_Dttm`
    ,CAST('' AS TIMESTAMP) AS `Record_Update_Dttm`
    ,'{{template_params['pipeline_run_id']}}' AS `Process_Instance_Id` 
    ,CAST(NULL as STRING) AS `Update_Process_Instance_Id` 
    ,CAST(1 AS BOOLEAN) AS `Is_Current`
    {%- if table == 'Rfrnc_Type' -%}
    ,`{{table}}_Cd` as `{{table}}_Cd`
    {%- endif -%}
    ,`{{table}}_Nm` as `{{table}}_Nm`
    ,`{{table}}_Shrt_Dsc` as `{{table}}_Shrt_Dsc`
    ,`{{table}}_Long_Dsc` AS `{{table}}_Long_Dsc`
    ,CAST(date_format(timestamp '{{template_params['process_start_time_stamp']}}','yyyyMM') as INT) as Year_Month
   FROM (
      SELECT `Src_App_Nm`
      {% if table == 'Rfrnc_Type' %} ,`{{table}}_Cd` {% endif %}
      ,`{{table}}_Nm`
      ,MIN({{table}}_Shrt_Dsc) as `{{table}}_Shrt_Dsc` 
      ,{%- if template_params['cols_exist'][table|lower+'_long_dsc'] -%}MIN(COALESCE(CAST(`{{table}}_Long_Dsc` AS STRING), "")){%- else -%}CAST(NULL AS STRING){%- endif %} as `{{table}}_Long_Dsc`
      FROM {{template_params['work_database']}}.{{template_params['worker_table']}}_mrg
      GROUP BY `Src_App_Nm`,{% if table == 'Rfrnc_Type' %}  `{{table}}_Cd`, {% endif %} `{{table}}_Nm`
  );;
  
  INSERT INTO {{template_params['main_database']}}.{{table|lower}}
  SELECT *
  FROM {{template_params['work_database']}}.gld_{{schema_dict['schema_name']|lower}}_{{table|lower}}_{{template_params['pipeline_run_id']}}_stg stg
  WHERE NOT EXISTS (SELECT 1 FROM {{template_params['main_database']}}.{{table|lower}} tb
      WHERE tb.{{table}}_Nm = stg.{{table}}_Nm {%- if table == 'Rfrnc_Type' %} AND tb.{{table}}_Cd = stg.{{table}}_Cd {%- endif %});;
  {%- endfor -%}
 
  -- *----------------------------------------------*
  -- STEP 2: Create Rfrnc_Cd staging table
  -- *----------------------------------------------*
  
  CREATE OR REPLACE TABLE {{template_params['work_database']}}.{{template_params['worker_table']}}_stg
  USING DELTA AS
    SELECT
    secure_hash(array(
        COALESCE(CAST(`Src_App_Nm` as STRING),"")
        ,COALESCE(CAST(`Rfrnc_Type_Cd` as STRING),"")
        ,COALESCE(CAST(`Rfrnc_Type_Nm` as STRING),"")
        ,COALESCE(CAST(`Rfrnc_Cd_Key` as STRING),"")
        ),'|') AS `Pk_Hash`
    ,secure_hash(array(
        {%- if template_params['cols_exist']['stndrd_rfrnc_cd_vl'] -%}COALESCE(CAST(`Stndrd_Rfrnc_Cd_Vl` AS STRING), ""){%- else -%}CAST(NULL AS STRING){%- endif -%}
		{%- for n in range(1,template_params['num_vals_ent_ref']+1) -%}
          {%- if n <= template_params['num_vals_mrg'] -%}
            ,CAST(`Rfrnc_Cd_Vl_Prmtr_{{n}}` as STRING)
          {%- else -%}
            ,CAST(NULL as STRING)
          {%- endif -%}
          {%- endfor -%}),'|') AS `Row_Hash`
    ,`Effective_Dttm` AS `Effective_Dttm` 
	,CAST('9999-12-31 00:00:00' AS TIMESTAMP) AS `Expiry_Dttm`
    ,CAST(`Src_App_Nm` as STRING) AS `Source_App_Name`
    ,'I' AS `Record_Type`
    ,CAST('{{template_params['process_start_time_stamp']}}' AS TIMESTAMP) AS `Record_Insert_Dttm`
    ,CAST('' AS TIMESTAMP) AS `Record_Update_Dttm`
    ,'{{template_params['pipeline_run_id']}}' AS `Process_Instance_Id` 
    ,CAST(NULL as STRING) AS `Update_Process_Instance_Id` 
    ,CAST(1 AS BOOLEAN) AS `Is_Current`
    ,secure_hash(array(`Src_App_Nm`),'|') AS `Src_App_Adt_Prmry_Key_Hash`
    ,secure_hash(array(`Rfrnc_Type_Nm`,`Rfrnc_Type_Cd`),'|') AS `Rfrnc_Type_Adt_Prmry_Key_Hash`
    ,CAST(`Rfrnc_Cd_Key` as STRING) as `Rfrnc_Cd_Key`
    {##}{%- for n in range(1,template_params['num_vals_ent_ref']+1) -%}
    {%- if n <= template_params['num_vals_mrg'] -%}
    ,CAST(`Rfrnc_Cd_Vl_Prmtr_{{n}}` as STRING) AS `Rfrnc_Cd_Vl_Prmtr_{{n}}`
    {%- else -%}
    ,CAST(NULL as STRING) AS `Rfrnc_Cd_Vl_Prmtr_{{n}}`
    {%- endif -%}
    {% endfor -%}
    ,{%- if template_params['cols_exist']['stndrd_rfrnc_cd_vl'] -%}COALESCE(CAST(`Stndrd_Rfrnc_Cd_Vl` AS STRING), ""){%- else -%}CAST(NULL AS STRING){%- endif %} as `Stndrd_Rfrnc_Cd_Vl`
    ,CAST(date_format(timestamp '{{template_params['process_start_time_stamp']}}','yyyyMM') as INT) as Year_Month
  FROM
    {{template_params['work_database']}}.{{template_params['worker_table']}}_mrg
  
 
"""
