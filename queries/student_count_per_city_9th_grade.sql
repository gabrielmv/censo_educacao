select
	city_name as city,
	count(1) as students_count
from
	public.censo
where
	enrollment_stage_name = 'Ensino Fundamental de 9 anos - 9º Ano'
group by
	city_name 
order by
	students_count DESC
limit 10
