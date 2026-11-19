{{ 
	config(
		schema='gold',
		materialized='incremental',
		unique_key=[
			'event_id'
		],
		incremental_strategy='insert_overwrite',
	) 
}}

select 
    event_id,
    reaction_name,
    reaction_outcome
from {{ ref('int_event_reactions') }}
