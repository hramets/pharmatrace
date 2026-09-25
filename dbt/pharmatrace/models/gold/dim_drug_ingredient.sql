{{
    config(
        schema='gold',
        materialized='incremental',
        unique_key='id'
    )
}}

with new_drugs as (
    select distinct
        coalesce(medicinal_product, 'unknown') as medicinal_product,
        coalesce(active_substance, 'unknown') as active_substance
    from {{ ref('int_event_drugs') }}
    {% if is_incremental() %}
    where
        (medicinal_product, active_substance) not in (
            select distinct
                medicinal_product,
                active_substance
            from {{ this }}
        )
    {% endif %}
),

max_id as (
    {% if is_incremental() %}
    select
        max(id) as max_id
    from {{ this }}
    {% else %}
    select 0 as max_id
    {% endif %}
)

select
    row_number() over (order by medicinal_product, active_substance) + max_id.max_id as id,
    medicinal_product,
    active_substance
from new_drugs
cross join max_id