{{ 
	config(
		schema='silver',
		materialized='incremental',
		unique_key=[
			'event_id',
			'reaction_name'
		]
	) 
}}

with cleaned as (
	select
		cast(safetyreportid as integer) as event_id,
		trim(
			regexp_replace(	
				regexp_replace(	
					regexp_replace(
						lower(trim(reactionmeddrapt)),
					    '[^a-z0-9\s\-]',
					    '',
					    'g'
					),
					'\b\d+\b',
					'',
					'g'
				),
				'\s+',
				' ',
				'g'
			)
		) as reaction_name,
		case
			when reactionoutcome = 1 then 'recovered'
			when reactionoutcome = 2 then 'recovering'
			when reactionoutcome = 3 then 'not recovered'
			when reactionoutcome = 4 then 'recovered with sequelae'
			when reactionoutcome = 5 then 'fatal'
			else 'unknown'
		end as reaction_outcome
	from 
	    {{ref('stg_event_reactions_raw')}}
),

deduplicated as (
	select 
		*,
		row_number() over (
		    partition by event_id, reaction_name
		    order by reaction_outcome nulls last
		) as rn
	from cleaned
)

select * exclude(rn) from deduplicated where rn = 1
