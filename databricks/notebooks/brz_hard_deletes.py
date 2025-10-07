# Databricks notebook source
def brz_hard_deletes():
    return r"""
    
-- *----------------------------------------------*
-- STEP 1: Create Staging table for source PKs
-- *----------------------------------------------*
DROP TABLE IF EXISTS {{template_params['work_database']}}.brz_{{template_params['source_name']|lower}}_{{schema_dict['File']['ObjectName']}}_stg_src_pks;;
CREATE TABLE {{template_params['work_database']}}.brz_{{template_params['source_name']|lower}}_{{schema_dict['File']['ObjectName']}}_stg_src_pks
USING {{schema_dict['File']['FileExtension']}}
OPTIONS (multiline{% if schema_dict['File']['ContainsMultilineData'] %} 'true'{% else %} 'false'{% endif %}, badRecordsPath '{{template_params['quarantine_path']}}')
LOCATION '{{template_params['incoming_path']}}_pk';;
         

-- *----------------------------------------------*
-- STEP 2: Create Staging table for Silver Layer PKs
-- *----------------------------------------------*
DROP TABLE IF EXISTS {{template_params['work_database']}}.brz_{{template_params['source_name']|lower}}_{{schema_dict['File']['ObjectName']}}_stg_slv_pks;;
CREATE TABLE {{template_params['work_database']}}.brz_{{template_params['source_name']|lower}}_{{schema_dict['File']['ObjectName']}}_stg_slv_pks
    {% set count = namespace(value=0) %}
    SELECT DISTINCT
    
        {%- for col in schema_dict['SourceColumns'] -%}
		{##}
		{%- if col['IsKeyColumn'] -%}
            {% if count.value > 0 %}
            ,`{{col['ColumnName']}}` as `{{col['ColumnName']}}`
            
        
            {%- else -%}
            `{{col['ColumnName']}}` as `{{col['ColumnName']}}`
            {% set count.value = count.value + 1 %}
            
            {%- endif -%}
		{%- endif -%}  
        {##}
        
		{%- endfor -%}
FROM {{template_params['main_database_slv']}}.{{schema_dict['File']['ObjectName']}}
WHERE PK_HASH NOT IN (SELECT DISTINCT PK_HASH from {{template_params['main_database_slv']}}.{{schema_dict['File']['ObjectName']}} WHERE Record_Type = 'D')
		 ;;
         
-- *----------------------------------------------*
-- STEP 3: Tag PKs that exist in Silver but not in source as 'D' (soft-delete)
-- *----------------------------------------------* 

DROP TABLE IF EXISTS {{template_params['work_database']}}.brz_{{template_params['source_name']|lower}}_{{schema_dict['File']['ObjectName']}}_stg_deleted_pks;;
CREATE TABLE {{template_params['work_database']}}.brz_{{template_params['source_name']|lower}}_{{schema_dict['File']['ObjectName']}}_stg_deleted_pks
    SELECT *, 'D' as `operation`
    FROM {{template_params['work_database']}}.brz_{{template_params['source_name']|lower}}_{{schema_dict['File']['ObjectName']}}_stg_slv_pks
    EXCEPT 
    SELECT *, 'D' as `operation` 
    FROM {{template_params['work_database']}}.brz_{{template_params['source_name']|lower}}_{{schema_dict['File']['ObjectName']}}_stg_src_pks
    ;;
    
-- *----------------------------------------------*
-- STEP 4: Create final wip table with all columns for the Pks tagged delete
-- *----------------------------------------------*     
DROP TABLE IF EXISTS {{template_params['work_database']}}.brz_{{template_params['source_name']|lower}}_{{schema_dict['File']['ObjectName']}}_stg_deletes;;
CREATE TABLE {{template_params['work_database']}}.brz_{{template_params['source_name']|lower}}_{{schema_dict['File']['ObjectName']}}_stg_deletes
    SELECT
	{% for col in schema_dict['SourceColumns'] %}
	  {% if loop.index > 1 %},{%- endif -%}
      {%- if col['IsAttributePII'] == True and col['EncryptionType'] == 'FPE' -%}
      CAST(NULL AS STRING) as `{{col['ColumnName']}}`
      ,CAST(NULL AS STRING) as `{{col['ColumnName']}}_Cpy`
      {%- elif col['IsKeyColumn'] -%}
       CAST(`{{col['ColumnName']}}` AS {{col['DataType']}})  as `{{col['ColumnName']}}`
      {%- elif col['IsAttributePII'] == True and col['EncryptionType'] == 'DET' -%}
      encrypt_scala_det_binary(CAST({% if col['DataType'] == 'DATE' %}to_date(NULL,'{{col['Format']['InputFormatString']}}'){% else %}NULL{% endif %} AS STRING)) as `{{col['ColumnName']}}`
      {%- elif col['IsAttributePII'] == True and col['EncryptionType'] == 'NDET' -%}
      encrypt_scala_ndet_binary(CAST({% if col['DataType'] == 'DATE' %}to_date(NULL,'{{col['Format']['InputFormatString']}}'){% else %}NULL{% endif %} AS STRING)) as `{{col['ColumnName']}}`
      {%- elif col['DataType'] == 'TIMESTAMP' and col['HistoryStitchColumn'] == True -%}
      CAST('{{template_params['process_start_time_stamp']}}' AS TIMESTAMP) as `{{col['ColumnName']}}`
      {%- elif col['DataType'] == 'TIMESTAMP' -%}
      CAST(NULL AS STRING) as `{{col['ColumnName']}}`
      {%- elif col['DataType'] == 'DATE' -%}
      to_date(NULL,'{{col['Format']['InputFormatString']}}') as `{{col['ColumnName']}}`
      {%- elif col['ColumnName'] == 'operation'-%}
      `operation` as `{{col['ColumnName']}}`
      {%- else -%}
      CAST(NULL AS {{col['DataType']}}) as `{{col['ColumnName']}}`
      {%- endif -%}
	{% endfor %}
      ,CAST(from_unixtime(to_unix_timestamp(regexp_extract(reverse(split(input_file_name(),'/'))[0],'{{schema_dict['File']['TimeStampFilenameRegexPattern']}}', 1),'{{schema_dict['File']['TimeStampFilenamePattern']}}'),'yyyy-MM-dd HH:mm:ss') AS DATE) AS `Source_Date`
      ,CAST(from_unixtime(to_unix_timestamp(regexp_extract(reverse(split(input_file_name(),'/'))[0],'{{schema_dict['File']['TimeStampFilenameRegexPattern']}}', 1),'{{schema_dict['File']['TimeStampFilenamePattern']}}'),'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) AS `Source_Timestamp`
      ,CAST('{{template_params['process_start_time_stamp']}}' AS TIMESTAMP) as `Process_Start_TimeStamp`
      ,reverse(split(input_file_name(),'/'))[0] AS `Source_File_Name`
      ,CAST(date_format(timestamp '{{template_params['process_start_time_stamp']}}','yyyyMM') as INT) as `Year_Month` 
    FROM {{template_params['work_database']}}.brz_{{template_params['source_name']|lower}}_{{schema_dict['File']['ObjectName']}}_stg_deleted_pks;;
-- *----------------------------------------------*
-- STEP 5: Merge the records with normal load staging table (Upserts)
-- *----------------------------------------------*  

INSERT INTO {{template_params['work_database']}}.brz_{{template_params['source_name']|lower}}_{{schema_dict['File']['ObjectName']}}_stg
SELECT * FROM {{template_params['work_database']}}.brz_{{template_params['source_name']|lower}}_{{schema_dict['File']['ObjectName']}}_stg_deletes;;

"""
