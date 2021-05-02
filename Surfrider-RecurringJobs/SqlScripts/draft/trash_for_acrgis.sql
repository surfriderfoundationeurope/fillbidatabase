drop table if exists bi.trash_for_arcgis;
create table bi.trash_for_arcgis as
with cte as (
select

	u.nickname user_nickname,
	t.id_ref_campaign_fk id_campaign,
	c.locomotion locomotion_mode,
	c.isaidriven ai_driven,
	c.riverside river_side,

	t.time as detection_date,

	t.lat as latitude,
	t.lon as longitude,
	t.elevation as altitude,

	tr.river_name,
	tt.name trash_type,

	t.municipality_name,
	t.department_name,
	t.state_name province_name,
	t.country_name,


	t.municipality_code::int,
	t.department_code,
	t.state_code province_code,
	t.country_code



from
	bi_temp.trash t

inner join bi_temp.trash_river tr on tr.id_ref_trash_fk = t.id
inner join bi_temp."campaign" c on c.id = t.id_ref_campaign_fk
inner join bi_temp.trash_type tt on t.id_ref_trash_type_fk = tt.id
inner join bi_temp.user u on u.id = c.id_ref_user_fk


)
select
	row_number() over() as id,
	trash_type,
	id_campaign,
	user_nickname,
	locomotion_mode,
	river_side,
	river_name,
	ai_driven,
	detection_date,
	latitude,
	longitude,
	altitude,
	municipality_name,
	municipality_code,
	province_name,
	province_code,
	country_code,
	country_name

from
	cte
;


