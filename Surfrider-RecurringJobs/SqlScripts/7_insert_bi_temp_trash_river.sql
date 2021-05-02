/*
this script associates each trash to a river (river name)
*/

-- QUERY 1: compute metrics for table bi_temp.trash_river
INSERT INTO bi_temp.trash_river (

                                  id_ref_trash_fk,
                                  id_ref_campaign_fk,
                                  id_ref_river_fk,
                                  trash_the_geom,
                                  river_the_geom,
                                  distance_river_trash,
                                  projection_trash_river_the_geom,
                                  closest_point_the_geom,
                                  importance,
                                  river_name,
                                  createdon,
                                  pipeline_id

                                  )
 WITH subquery_1 AS (

   SELECT
     t.id id_ref_trash_fk,
     t.id_ref_campaign_fk id_ref_campaign_fk,
     closest_r.id id_ref_river_fk,
     t.the_geom trash_the_geom,
     closest_r.the_geom river_the_geom,
     st_closestpoint(closest_r.the_geom, t.the_geom) closest_point_the_geom,
     closest_r.importance,
     closest_r.name river_name,
     pipeline_id

   FROM bi_temp.trash  t

   INNER JOIN LATERAL (

           SELECT
                *
           FROM referential.river r
           WHERE name IS NOT NULL
           ORDER BY r.the_geom <-> t.the_geom

           LIMIT 1

   ) closest_r ON TRUE

   WHERE t.id_ref_campaign_fk IN (@campaignID)
)
 SELECT
   id_ref_trash_fk,
   id_ref_campaign_fk,
   id_ref_river_fk,
   trash_the_geom,
   river_the_geom,
   st_distance(closest_point_the_geom, trash_the_geom) distance_river_trash,
   st_makeline(trash_the_geom, closest_point_the_geom) projection_trash_river_the_geom,
   closest_point_the_geom,
   importance,
   river_name,
   current_timestamp,
   pipeline_id

 FROM
   subquery_1;

-- QUERY 2: create partial and spatial indexes
DROP INDEX IF EXISTS bi_temp.trash_river_id_ref_trash_fk;

CREATE INDEX trash_river_id_ref_trash_fk
ON bi_temp.trash_river (id_ref_trash_fk)
WHERE id_ref_campaign_fk IN (@campaignID);


-- on closest_point_the_geom
DROP INDEX IF EXISTS bi_temp.trash_river_closest_point_the_geom;

CREATE INDEX trash_river_closest_point_the_geom
ON bi_temp.trash_river USING gist(closest_point_the_geom)
WHERE id_ref_campaign_fk IN (@campaignID);
