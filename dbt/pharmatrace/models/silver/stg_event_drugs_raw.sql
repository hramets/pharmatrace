{{
    config(
        schema='silver',
        materialized='incremental',
        unique_key=[
            'safetyreportid',
            'medicinalproduct',
            'activesubstancename',
            'drugcharacterization',
            'drugstartdate',
            'drugstructuredosagenumb'
        ]
    )
}}

with raw as (
    select
        results,
        filename
    from read_json(
        '{{ var("bronze_path") }}/*.json',
        maximum_object_size=50000000,
        union_by_name=true,
        ignore_errors=true,
        filename=True
    )
),

unnested as (
    select
        unnest(results) as record,
        filename as source_file
    from raw
),

unnested_drugs as (
    select
        record.safetyreportid as safetyreportid ,
        unnest(record.patient.drug) as drug,
        source_file
    from unnested
)

select
    safetyreportid,
    drug.medicinalproduct,
    drug.activesubstance.activesubstancename,
    drug.drugcharacterization,
    drug.drugstartdate,
    drug.drugenddate,
    drug.drugstructuredosagenumb,
    drug.drugstructuredosageunit,
    drug.drugcumulativedosagenumb,
    drug.drugcumulativedosageunit,
    source_file
from unnested_drugs
{% if is_incremental() %}
where source_file not in (select distinct source_file from {{ this }})
{% endif %}