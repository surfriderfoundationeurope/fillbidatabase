INSERT INTO bi_temp.user (id, nickname, lastloggedon)
SELECT

	id,
	nickname,
	lastloggedon

FROM campaign.user;

insert into bi_temp.trash_type (id,name)
select id, name
from campaign.trash_type tt;
------------------------------
INSERT INTO bi_temp.campaign (id, locomotion, isaidriven, remark, id_ref_user_fk, riverside, createdon)
SELECT

	id,
	locomotion,
	isaidriven,
	remark,
	id_ref_user_fk,
	riverside,
	createdon

FROM campaign.campaign;
------------------------------
INSERT INTO bi_temp.trash (id, id_ref_campaign_fk, the_geom, elevation, id_ref_trash_type_fk, precision, id_ref_model_fk, time, lat, lon, createdon )
SELECT

	id,
	id_ref_campaign_fk,
	the_geom,
	elevation,
	id_ref_trash_type_fk,
	precision,
	id_ref_model_fk,
	time,
	st_y(st_transform(the_geom, 4326)),
	st_x(st_transform(the_geom, 4326)),
	createdon

FROM campaign.trash;
------------------------------

insert into bi_temp.trajectory_point (
								id,
								the_geom,
								id_ref_campaign_fk,
								elevation,
								distance,
								time_diff,
								time,
								speed,
								lat,
								lon,
								createdon

							)
SELECT
	id,
	the_geom,
	id_ref_campaign_fk,
	elevation,
	distance,
	time_diff,
	time,
	speed,
	st_y(st_transform(the_geom, 4326)),
	st_x(st_transform(the_geom, 4326)),
	createdon

from campaign.trajectory_point tp;




drop index if exists trash_the_geom;
drop index if exists trajectory_point_the_geom;


create index trash_the_geom  on bi_temp.trash using gist(the_geom);
create index trajectory_point_the_geom  on bi_temp.trajectory_point using gist(the_geom);




;