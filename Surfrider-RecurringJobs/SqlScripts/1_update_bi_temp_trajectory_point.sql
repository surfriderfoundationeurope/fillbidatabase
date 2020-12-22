/*
This script computes fields:
    - distance
    - time_diff
    - speed
    - lat
    - lon

for table bi_temp.trajectory_point
*/

-- QUERY 1: Compute distance and time_diff (duration between two points) in a temporary table
CREATE TEMP TABLE trajectory_point_agg as
SELECT

	id,
	st_distance(the_geom, lag(the_geom) over( partition by id_ref_campaign_fk order by time asc )) distance,
	age("time", lag("time") over( partition by id_ref_campaign_fk order by time asc )) time_diff

FROM bi_temp.trajectory_point
WHERE id_ref_campaign_fk in (@campaignID)
;

-- QUERY 2: update trajectory_point table with distance and time_diff
UPDATE bi_temp.trajectory_point t
SET
	distance = agg.distance,
	time_diff = agg.time_diff

FROM   trajectory_point_agg  agg
WHERE agg.id = t.id;

-- QUERY 3: update speed in trajectory_point table
UPDATE bi_temp.trajectory_point
SET speed = (distance/EXTRACT(epoch FROM time_diff))*3.6
WHERE speed IS NULL
		AND EXTRACT(epoch FROM time_diff) > 0
		AND distance > 0
		AND id_ref_campaign_fk in (@campaignID);

-- QUERY 4: update lat/lon values in trajectory_point tables
UPDATE bi_temp.trajectory_point
SET
	lat = st_y(st_transform(the_geom, 4326)),
	lon = st_x(st_transform(the_geom, 4326))

WHERE id_ref_campaign_fk in (@campaignID)
;

-- QUERY 5: creates partial spatial index on trajectory point geometry

DROP INDEX IF EXISTS bi_temp_trajectory_point;
CREATE INDEX bi_temp_trajectory_point on bi_temp.trajectory_point (the_geom) WHERE id_ref_campaign_fk IN (@campaignID);

