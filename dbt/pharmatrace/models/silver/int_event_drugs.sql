{{ 
	config(
		schema='silver',
		materialized='table',
		unique_key=[
			'event_id',
			'medicinal_product',
			'active_substance',
			'drug_role',
			'drug_start_date',
			'dosage'
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
						regexp_replace(
							regexp_replace(
								regexp_replace(
									regexp_replace(
										lower(trim(medicinalproduct)),
										'[\\/]+|\s*(,|\band\b)\s*',
										'\\',
										'g'
									),
									'\s+(xr|er|sr|cr|dr|ir|xl|\d+\s*mg|\d+\s*ml)\s*$',
									'',
									'gi'
								),
								'[^a-z0-9\s\-\\]',
								'',
								'g'
							),
							'\b\d+\b',
							'',
							'g'
						),
						'\\+',
						'\\',
						'g'
					),
					'\s+',
					' ',
					'g'
				),
				'^\s*-+\s*|\s*-+\s*$',
				'',
				'g'
			)
		) as medicinal_product,
		lower(
			regexp_replace(
				regexp_replace(
					regexp_replace(
						lower(trim(activesubstancename)),
						'[\\/]+|\s*(,|\band\b)\s*',
						'\\',
						'gi'
					),
					'\b\d+\b',
					'',
					'g'
				),
				'\\+',
				'\\',
				'g'
			)
		) as active_substance,
		case 
			when drugcharacterization = '1' then 'suspect' /* the drug was considered by the reporter to be the cause */
			when drugcharacterization= '2' then 'concomitant' /* the drug was reported as being taken along with the suspect drug */
			when drugcharacterization = '3' then 'interacting' /* the drug was considered by the reporter to have interacted with the suspect drug */
			else 'unknown'
		end as drug_role,
		try_cast(
			strptime(
				case 
					when len(drugstartdate) = 8 then drugstartdate
					when len(drugstartdate) = 6 then concat(drugstartdate, '01')
					when len(drugstartdate) = 4 then concat(drugstartdate, '0101')
					else null
				end,
				'%Y%m%d'
			) as date
		) as drug_start_date,
		try_cast(
			strptime(
				case 
					when len(drugenddate) = 8 then drugenddate
					when len(drugenddate) = 6 then concat(drugenddate, '01')
					when len(drugenddate) = 4 then concat(drugenddate, '0101')
					else null
				end,
				'%Y%m%d'
			) as date
		) as drug_end_date,
		try_cast(drugstructuredosagenumb as float) as dosage,
		case
			when drugstructuredosageunit = '001' then 'kg'
			when drugstructuredosageunit = '002' then 'g'
			when drugstructuredosageunit = '003' then 'mg'
			when drugstructuredosageunit = '004' then 'mcg'
			else NULL
		end as dosage_unit,
		try_cast(drugcumulativedosagenumb as float) as cumulative_dosage,
		case
			when drugcumulativedosageunit = '001' then 'kg'
			when drugcumulativedosageunit = '002' then 'g'
			when drugcumulativedosageunit = '003' then 'mg'
			when drugcumulativedosageunit = '004' then 'mcg'
			else NULL
		end as cumulative_dosage_unit
	from {{ref('stg_event_drugs_raw')}}
	{% if is_incremental() %}
	where cast(safetyreportid as integer) in (
		select id
		from {{ ref('int_events') }}
		where transmission_date >= (select max(transmission_date) from {{ ref('int_events') }})
	)
	{% endif %}
),
deduplicated as (
	select *,
	row_number() over (
	    partition by 
	    	event_id,
	    	medicinal_product,
	    	active_substance,
	    	drug_role,
	    	drug_start_date,
	    	dosage
	    order by drug_start_date
	) as rn
	from cleaned
)

select * exclude(rn) from deduplicated where rn = 1
