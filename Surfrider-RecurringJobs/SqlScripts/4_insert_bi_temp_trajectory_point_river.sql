/*
This script associated each trajectory point to a river section (river name)
*/

-- QUERY 2: Insert data in trajectory_point_river
INSERT INTO  bi_temp.trajectory_point_river (

                                             id_ref_trajectory_point_fk,
                                             id_ref_campaign_fk,
                                             id_ref_river_fk,
                                             trajectory_point_the_geom,
                                             river_the_geom,
                                             distance_river_trajectory_point,
                                             projection_trajectory_point_river_the_geom,
                                             closest_point_the_geom,
                                             river_name,
                                             createdon,
                                             pipeline_id

                                             )

WITH subquery_1 AS (

   SELECT
     t.id id_ref_trajectory_point_fk,
     t.id_ref_campaign_fk id_ref_campaign_fk,

     closest_r.id id_ref_river_fk,
     t.the_geom trajectory_point_the_geom,
     closest_r.the_geom river_the_geom,
     st_closestpoint(closest_r.the_geom, t.the_geom) closest_point_the_geom,
     closest_r.name river_name,
     t.pipeline_id
   FROM
       bi_temp.trajectory_point t

   INNER JOIN LATERAL (

                       SELECT
                       *
                       FROM
                       referential.river r

                       WHERE name IS NOT NULL
                       ORDER BY r.the_geom <-> t.the_geom

                       LIMIT 1

                       ) closest_r ON TRUE


WHERE t.id_ref_campaign_fk IN (@campaignID)


   )

SELECT
  id_ref_trajectory_point_fk,
  id_ref_campaign_fk,
  id_ref_river_fk,
  trajectory_point_the_geom,
  river_the_geom,
  st_distance(closest_point_the_geom, trajectory_point_the_geom) distance_river_trajectory_point,
  st_makeline(trajectory_point_the_geom, closest_point_the_geom) projection_trajectory_point_river_the_geom,
  closest_point_the_geom,
  river_name,
  current_timestamp,
  pipeline_id

FROM
  subquery_1

WHERE st_distance(closest_point_the_geom, trajectory_point_the_geom) < 100

;

-- QUERY 3: creates partial spatial index on trajectory_point_river

-- on id_ref_campaign_fk
DROP INDEX IF EXISTS bi_temp.trajectory_point_river_id_ref_trajectory_point_fk;

CREATE INDEX trajectory_point_river_id_ref_trajectory_point_fk
ON bi_temp.trajectory_point_river (id_ref_trajectory_point_fk)
WHERE id_ref_campaign_fk IN (@campaignID);


-- on closest_point_the_geom
DROP INDEX IF EXISTS bi_temp.trajectory_point_river_closest_point_the_geom;

CREATE INDEX trajectory_point_river_closest_point_the_geom
ON bi_temp.trajectory_point_river USING gist(closest_point_the_geom)
WHERE id_ref_campaign_fk IN (@campaignID);


