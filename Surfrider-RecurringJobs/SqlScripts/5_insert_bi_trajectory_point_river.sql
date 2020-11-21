DO $$

DECLARE campaign_ids uuid[] := ARRAY[@campaign_ids];
BEGIN

      INSERT INTO bi.trajectory_point_river (

                                             id_ref_trajectory_point_fk,
                                             id_ref_campaign_fk,
                                             id_ref_river_fk,
                                             trajectory_point_the_geom,
                                             river_the_geom,
                                             distance_river_trajectory_point,
                                             projection_trajectory_point_river_the_geom,
                                             closest_point_the_geom,
                                             importance,
                                             river_name,
                                             createdon

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
        closest_r.importance

      FROM
          campaign.trajectory_point t

      INNER JOIN LATERAL (

                          SELECT
                          *
                          FROM
                          referential.river r

                          WHERE name IS NOT NULL
                          ORDER BY r.the_geom <-> t.the_geom

                          LIMIT 1

                          ) closest_r ON TRUE


      WHERE t.id_ref_campaign_fk IN (SELECT UNNEST(campaign_ids))

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
        importance,
        river_name,
        current_timestamp
      FROM
        subquery_1;

      DROP INDEX IF EXISTS bi.trajectory_point_river_id_ref_trajectory_point_fk;
      CREATE INDEX trajectory_point_river_id_ref_trajectory_point_fk ON bi.trajectory_point_river (id_ref_trajectory_point_fk);

END$$;


