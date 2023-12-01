
{{ config(materialized='pit_incremental', enabled=false) }}

{%- set yaml_metadata -%}
source_model: hub_account
src_pk: ACCOUNT_PK_HASH
as_of_dates_table: as_of_date
satellites: 
  sat_account_details:
    pk:
      PK: ACCOUNT_PK_HASH
    ldts:
      LDTS: CREATED_DATETIMESTAMP

stage_tables: 
  STG_SALESFORCE_ACCOUNTS: CREATED_DATETIMESTAMP

src_ldts: CREATED_DATETIMESTAMP
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{% set source_model = metadata_dict['source_model'] %}
{% set src_pk = metadata_dict['src_pk'] %}
{% set as_of_dates_table = metadata_dict['as_of_dates_table'] %}
{% set satellites = metadata_dict['satellites'] %}
{% set stage_tables_ldts = metadata_dict['stage_tables_ldts'] %}
{% set src_ldts = metadata_dict['src_ldts'] %}

{{ automate_dv.pit(source_model=source_model, src_pk=src_pk,
                   as_of_dates_table=as_of_dates_table,
                   satellites=satellites,
                   stage_tables_ldts=stage_tables_ldts,
                   src_ldts=src_ldts) }}