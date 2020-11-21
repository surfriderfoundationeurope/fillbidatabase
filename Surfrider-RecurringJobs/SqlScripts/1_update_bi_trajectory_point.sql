CREATE TEMP TABLE trajectory_point_agg as
SELECT

	id,
	st_distance(the_geom, lag(the_geom) over( partition by id_ref_campaign_fk order by time asc )) distance,
	age("time", lag("time") over( partition by id_ref_campaign_fk order by time asc )) time_diff

FROM bi_temp.trajectory_point
WHERE id_ref_campaign_fk in (@campaign_ids)
;
/* ------------------------------------------------------------------------------------------------------------------------ */
update bi_temp.trajectory_point t
SET
	distance = agg.distance,
	time_diff = agg.time_diff

from   trajectory_point_agg  agg
where agg.id = t.id;

/* ------------------------------------------------------------------------------------------------------------------------ */
UPDATE bi_temp.trajectory_point
SET speed = (distance/EXTRACT(epoch FROM time_diff))*3.6
WHERE speed IS NULL

		AND EXTRACT(epoch FROM time_diff) > 0
		AND distance > 0
		AND id_ref_campaign_fk in (@campaign_ids)
	;
/* ------------------------------------------------------------------------------------------------------------------------ */
UPDATE bi_temp.trajectory_point
SET
	lat = st_y(st_transform(the_geom, 4326)),
	lon = st_x(st_transform(the_geom, 4326))

WHERE id_ref_campaign_fk in (@campaign_ids)
	;
/* ------------------------------------------------------------------------------------------------------------------------ */
DROP TABLE IF EXISTS trajectory_point_agg;