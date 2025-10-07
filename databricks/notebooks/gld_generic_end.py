# Databricks notebook source
# MAGIC %md
# MAGIC ##### Gold - Generic Load Jinja2 template

# COMMAND ----------

def gld_generic_end():
  return r"""
-- *----------------------------------------------*
-- STEP 4: Insert all records identified in step 4 into the gold table
-- *----------------------------------------------*
INSERT INTO {{template_params['main_database']}}.{{schema_dict['object_name']}}
PARTITION (`Year_Month`)
SELECT  `Pk_Hash`
      ,`Row_Hash`
      ,`Effective_Dttm`
      ,`Expiry_Dttm`
      ,`Source_App_Name`
      ,`Record_Type`
      ,`Record_Insert_Dttm`
      ,`Record_Update_Dttm`
      ,`Process_Instance_Id`
      ,`Update_Process_Instance_Id`
      ,`Is_Current`
      {##}{%- for col in schema_dict['target_columns'] -%}
      {%- if  not(col['ignore']) or col['is_primary_key_col'] -%} 
      ,`{{col['column_name']}}`
      {% endif %}
      {%- endfor -%}
      {%- for ik_name in template_params['integration_key_cols_per_name'] -%}
      ,`{{ik_name}}`
      {% endfor %}
      ,`Year_Month`
FROM {{template_params['work_database']}}.{{template_params['worker_table']}}_load;;

ANALYZE TABLE {{template_params['main_database']}}.{{schema_dict['object_name']}} COMPUTE STATISTICS;;

-- *----------------------------------------------*
-- STEP 5: Create dynamic view for decrypting PII data for priviliged users
-- *----------------------------------------------*
DROP VIEW IF EXISTS {{template_params['main_database']}}_piiView.{{schema_dict['object_name']}};;

CREATE VIEW {{template_params['main_database']}}_piiView.{{schema_dict['object_name']}}
as SELECT `Pk_Hash` 
	 ,`Row_Hash` 
	 ,`Effective_Dttm`
	 ,`Expiry_Dttm` 
	 ,`Source_App_Name` 
	 ,`Record_Type` 
	 ,`Record_Insert_Dttm` 
	 ,`Record_Update_Dttm` 
	 ,`Process_Instance_Id` 
	 ,`Update_Process_Instance_Id` 
	 ,`Is_Current`
     {##}{%- for col in schema_dict['target_columns'] -%}
     {%- if  not(col['ignore']) or col['is_primary_key_col'] -%}
     {%- if  col['adb_encryption_type'] == 'DET' -%}
     ,`{{col['column_name']}}`
	 ,case when is_member("role-{{template_params['business_unit_name_code']|lower}}-classified") then cast(pii_{{template_params['business_unit_name_code']|lower}}_decrypt_aes_det(`{{col['column_name']}}`) as `{{col['adb_data_type']}}`)
    else cast(NULL as `{{col['adb_data_type']}}`) end as `{{col['column_name']}}_pii`
     {##}
     {%- elif  col['adb_encryption_type'] == 'NDET' -%}
      ,`{{col['column_name']}}`
	 ,case when is_member("role-{{template_params['business_unit_name_code']|lower}}-classified") then cast(pii_{{template_params['business_unit_name_code']|lower}}_decrypt_aes_ndet(`{{col['column_name']}}`) as `{{col['adb_data_type']}}`)
    else cast(NULL as `{{col['adb_data_type']}}`) end as `{{col['column_name']}}_pii`
     {##}
     {%- else -%}
     ,`{{col['column_name']}}`
     {% endif %}
     {%- endif -%}
     {%- endfor -%}
     {%- for ik_name in template_params['integration_key_cols_per_name'] -%}
     ,`{{ik_name}}`
     {% endfor %}
     ,`Year_Month` 
FROM {{template_params['main_database']}}.{{schema_dict['object_name']}};;
"""
