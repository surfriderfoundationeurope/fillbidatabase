DO $$

DECLARE campaign_ids uuid[] := ARRAY[@campaign_ids];
BEGIN

INSERT INTO bi.campaign_river (

								id_ref_campaign_fk,
								the_geom,
								distance,
								river_name



                 )
WITH subquery_1 as (

	SELECT
		t.id_ref_campaign_fk,
		st_makevalid(st_makeline(closest_point_the_geom ORDER BY t.time)) the_geom,
		river_name,
		st_union(river_the_geom) river_the_geom

	FROM
		bi.trajectory_point_river  tr

	INNER JOIN campaign.trajectory_point t on t.id = tr.id_ref_trajectory_point_fk
	WHERE tr.id_ref_campaign_fk in (select unnest(campaign_ids))

	GROUP BY t.id_ref_campaign_fk, river_name

)
SELECT

	id_ref_campaign_fk,
	the_geom,
	st_length(the_geom) distance,
	river_name

FROM
  subquery_1;

END$$;


