{{
    config(
        schema='silver',
        materialized='incremental',
        unique_key=[
            'safetyreportid',
            'reactionmeddrapt',
            'reactionoutcome'
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
),

unnested_reactions as (
    select
        record.safetyreportid as safetyreportid ,
        unnest(record.patient.reaction) as reaction,
        source_file
    from unnested
)

select
    safetyreportid,
    reaction.reactionmeddrapt,
    reaction.reactionoutcome,
    source_file
from unnested_reactions
{% if is_incremental() %}
where source_file not in (select distinct source_file from {{ this }})
{% endif %}