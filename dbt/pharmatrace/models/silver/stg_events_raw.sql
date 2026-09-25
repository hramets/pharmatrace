{{
    config(
        schema='silver',
        materialized='incremental',
        unique_key=[
            'safetyreportid'
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
        filename=true
    )
),

unnested as (
    select 
        unnest(results) as record,
        filename as source_file
    from raw
)

select
    record.safetyreportid,
    record.transmissiondate,
    record.receivedate,
    record.serious,
    record.seriousnessdeath,
    record.seriousnesshospitalization,
    record.seriousnesslifethreatening,
    record.seriousnessdisabling,
    record.seriousnesscongenitalanomali,
    record.seriousnessother,
    record.patient.patientonsetage,
    record.patient.patientonsetageunit,
    record.patient.patientsex,
    record.patient.patientweight,
    source_file
from unnested
{% if is_incremental() %}
where source_file not in (select distinct source_file from {{ this }})
{% endif %}