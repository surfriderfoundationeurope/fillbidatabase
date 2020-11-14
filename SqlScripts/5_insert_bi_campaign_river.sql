drop index if exists bi_temp.trajectory_point_river_closest_point_the_geom;
create index trajectory_point_river_closest_point_the_geom on bi_temp.trajectory_point_river using gist(closest_point_the_geom);


INSERT INTO bi_temp.campaign_river (

								id_ref_campaign_fk,
								the_geom,
								distance,
								river_name,
								id_ref_river_fk

                 )
with subquery_1 AS (

	SELECT
		t.id_ref_campaign_fk,
		st_makevalid(st_makeline(closest_point_the_geom ORDER BY t.time)) the_geom,
		river_name,
		st_union(river_the_geom) river_the_geom,
		id_ref_river_fk

	FROM
		(

		 SELECT *
		 FROM bi_temp.trajectory_point_river
		 WHERE id_ref_campaign_fk in (@campaign_ids)
		 ORDER BY random()
		 LIMIT 100
		 )  tr

	INNER JOIN bi_temp.trajectory_point t on t.id = tr.id_ref_trajectory_point_fk

	GROUP BY t.id_ref_campaign_fk,
					river_name,
					id_ref_river_fk

)
SELECT

	id_ref_campaign_fk,
	the_geom,
	st_length(the_geom) distance,
	river_name,
	id_ref_river_fk

FROM
  subquery_1;



