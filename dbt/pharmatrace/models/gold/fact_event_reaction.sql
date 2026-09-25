{{ 
	config(
		schema='gold',
		materialized='incremental',
		unique_key=[
			'event_id',
			'reaction_name'
		]
	) 
}}

select 
    event_id,
    reaction_name,
    reaction_outcome
from {{ ref('int_event_reactions') }}
{% if is_incremental() %}
where event_id not in (select distinct event_id from {{ this }})
{% endif %}