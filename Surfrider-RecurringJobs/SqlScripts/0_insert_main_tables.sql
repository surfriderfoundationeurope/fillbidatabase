/*
INSERT INTO bi_temp.user (id, nickname, lastloggedon)
SELECT

	id,
	nickname,
	lastloggedon

FROM campaign.user
where createdon >= @last_run

;
*/

/*

insert into bi_temp.trash_type (id,name)
select id, name
from campaign.trash_type tt
;
*/
/* ------------------------------------------------------------------------------------------------------------------------ */
INSERT INTO logs.bi (id, campaign_id)
	SELECT c.id, c.id
	from campaign.campaign c
	LEFT JOIN logs.bi l ON c.id = l.campaign_id
	WHERE l.campaign_id is null AND c.id IN (@campaign_ids);

/* ------------------------------------------------------------------------------------------------------------------------ */
INSERT INTO bi_temp.campaign (id, locomotion, isaidriven, remark, id_ref_user_fk, riverside, createdon)
	SELECT

		id,
		locomotion,
		isaidriven,
		remark,
		id_ref_user_fk,
		riverside,
		createdon

	FROM campaign.campaign
	WHERE id IN (@campaign_ids)
;

/* ------------------------------------------------------------------------------------------------------------------------ */
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

	FROM campaign.trash
	WHERE id_ref_campaign_fk IN (@campaign_ids)
	;

/* ------------------------------------------------------------------------------------------------------------------------ */
INSERT INTO bi_temp.trajectory_point (
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
		null,
		null,
		time,
		speed,
		st_y(st_transform(the_geom, 4326)),
		st_x(st_transform(the_geom, 4326)),
		createdon

	FROM campaign.trajectory_point tp
	WHERE id_ref_campaign_fk IN (@campaign_ids)
	;
/* ------------------------------------------------------------------------------------------------------------------------ */
DROP INDEX IF EXISTS bi_temp.trash_the_geom;
DROP INDEX IF EXISTS bi_temp.trajectory_point_the_geom;

CREATE INDEX trash_the_geom  on bi_temp.trash using gist(the_geom);
CREATE INDEX trajectory_point_the_geom  on bi_temp.trajectory_point using gist(the_geom);

;