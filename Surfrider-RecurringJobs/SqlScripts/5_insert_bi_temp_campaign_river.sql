/*
This script associates to each campaign a river name.
*/

-- QUERY 1: compute metrics for table bi_temp.campaign_river
INSERT INTO bi_temp.campaign_river (

								id_ref_campaign_fk,
								the_geom,
								distance,
								river_name,
								id_ref_river_fk,
								pipeline_id

                 )
WITH subquery_1 AS (

	SELECT
		t.id_ref_campaign_fk,
		st_makevalid(st_makeline(closest_point_the_geom ORDER BY t.time)) the_geom,
		river_name,
		st_union(river_the_geom) river_the_geom,
		id_ref_river_fk,
		tr.pipeline_id

	FROM
		(
             SELECT *
             FROM bi_temp.trajectory_point_river
             WHERE id_ref_campaign_fk in (@campaignID)
             ORDER BY random()
             LIMIT 1000
		 )  tr

	INNER JOIN bi_temp.trajectory_point t on t.id = tr.id_ref_trajectory_point_fk
	GROUP BY t.id_ref_campaign_fk,
					river_name,
					id_ref_river_fk,
					tr.pipeline_id

)
SELECT
DISTINCT  ON (id_ref_campaign_fk)
	id_ref_campaign_fk,
	the_geom,
	st_length(the_geom) distance,
	river_name,
	id_ref_river_fk,
	pipeline_id
FROM
  subquery_1

ORDER BY id_ref_campaign_fk, st_length(the_geom) DESC ;


