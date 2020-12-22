/*
This script migrates data from campaign.* to bi_temp.*
*/

-- QUERY 1: insert campaign ids in bi_temp.pipelines

INSERT INTO bi_temp.pipelines (campaign_id)
SELECT id
FROM campaign.campaign
WHERE has_been_computed = Null
;

-- QUERY 2: insert data from campaign.campaign to bi_temp.campaign
INSERT INTO bi_temp.campaign (id, locomotion, isaidriven, remark, id_ref_user_fk, riverside, createdon, pipeline_id)
SELECT
	id,
	locomotion,
	isaidriven,
	remark,
	id_ref_user_fk,
	riverside,
	createdon,
	@pipelineID
FROM campaign.campaign
WHERE id IN (@campaignID)
;

-- QUERY 3: insert data from campaign.trash to bi_temp.trasg
INSERT INTO bi_temp.trash (id, id_ref_campaign_fk, the_geom, elevation, id_ref_trash_type_fk, precision, id_ref_model_fk, time, lat, lon, createdon, pipeline_id )
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
	createdon,
	@pipelineID

FROM campaign.trash
WHERE id_ref_campaign_fk IN (@campaignID)
;

-- QUERY 4: insert data from campaign.trajectory_point to bi_temp.trajectory_point
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
								createdon,
								pipeline_id

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
	createdon,
	@pipelineID

FROM campaign.trajectory_point tp
WHERE id_ref_campaign_fk IN (@campaignID)
;

-- QUERY 5: DROP AND CREATE indexes

DROP INDEX IF EXISTS bi_temp.trash_the_geom;
DROP INDEX IF EXISTS bi_temp.trajectory_point_the_geom;

CREATE INDEX trash_the_geom  on bi_temp.trash using gist(the_geom);
CREATE INDEX trajectory_point_the_geom  on bi_temp.trajectory_point using gist(the_geom);

;