{{ 
	config(
		schema='silver',
		materialized='incremental',
		unique_key='id'
	) 
}}

with cleaned as (
	select
		cast(safetyreportid as integer) as id,
		cast(strptime(transmissiondate, '%Y%m%d') as date) as transmission_date,
		cast(strptime(receivedate, '%Y%m%d') as date) as receive_date,
		cast(
			(
				case
					when serious = '1' then 1
					else 0
				end
			) as boolean
		) as serious,
		cast(
			(
				case
					when seriousnessdeath = '1' then 1
					else 0
				end
			) as boolean
		) as seriousness_death,
		cast(
			(
				case
					when seriousnesshospitalization = '1' then 1
					else 0
				end
			) as boolean
		) as seriousness_hospitalization,
		cast(
			(
				case
					when seriousnesslifethreatening = '1' then 1
					else 0
				end
			) as boolean
		) as seriousness_life_threatening,
		cast(
			(
				case
					when seriousnessdisabling = '1' then 1
					else 0
				end
			) as boolean
		) as seriousness_disabling,
		cast(
			(
				case
					when seriousnesscongenitalanomali = '1' then 1
					else 0
				end
			) as boolean
		) as seriousness_congenital_anomaly,
		cast(
			(
				case
					when seriousnessother = '1' then 1
					else 0
				end
			) as boolean
		) as seriousness_other,
		cast(patientonsetage  as float) as patient_onset_age,
		case
			when patientonsetageunit = '800' then 'decade'
			when patientonsetageunit = '801' then 'year'
			when patientonsetageunit = '802' then 'month'
			when patientonsetageunit = '803' then 'week'
			when patientonsetageunit = '804' then 'day'
			when patientonsetageunit = '805' then 'hour'
		end as patient_onset_age_unit,
		case
			when patientsex = '1' then 'male'
			when patientsex = '2' then 'female'
			else 'unknown'
		end as patient_sex,
		cast(patientweight  as float) as patient_weight
	from {{ref('stg_events_raw')}}
	{% if is_incremental() %}
	where cast(strptime(transmissiondate, '%Y%m%d') as date) >= (
		select max(transmission_date) from {{ this }}
	)
	{% endif %}
)

select * from cleaned

