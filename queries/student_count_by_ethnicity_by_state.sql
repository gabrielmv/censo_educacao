select
	state_abrv as state,
	student_ethnicity,
	count(1) as student_count
from
	public.censo c
where
	student_ethnicity != 'Não declarada'
group by
	state_abrv,
	student_ethnicity
order by
	state_abrv,
	student_ethnicity