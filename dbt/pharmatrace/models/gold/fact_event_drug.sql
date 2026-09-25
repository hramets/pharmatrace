{{
    config(
        schema='gold',
        materialized='incremental',
        unique_key=[
            'event_id',
            'drug_id',
            'drug_role',
            'drug_start_date',
            'dosage'
        ]
    )
}}
select
    sed.event_id,
    i.id as drug_id,
    sed.drug_role,
    sed.drug_start_date,
    sed.drug_end_date,
    sed.dosage,
    sed.dosage_unit,
    sed.cumulative_dosage,
    sed.cumulative_dosage_unit
from {{ ref('int_event_drugs') }} sed
left join {{ ref('dim_drug_ingredient') }} i
    on i.medicinal_product = sed.medicinal_product and i.active_substance = sed.active_substance
{% if is_incremental() %}
where sed.event_id not in (select distinct event_id from {{ this }})
{% endif %}




