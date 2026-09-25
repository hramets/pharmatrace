{{ 
    config(
        schema='gold',
        materialized='incremental',
        unique_key='id'
    ) 
}}

select
    id,
    transmission_date,
    receive_date,
    serious,
    seriousness_death,
    seriousness_hospitalization,
    seriousness_life_threatening,
    seriousness_disabling,
    seriousness_congenital_anomaly,
    seriousness_other,
    patient_onset_age,
    patient_onset_age_unit,
    patient_sex,
    patient_weight
from {{ ref('int_events') }}
{% if is_incremental() %}
where transmission_date >= (select max(transmission_date) from {{ this }})
{% endif %}
