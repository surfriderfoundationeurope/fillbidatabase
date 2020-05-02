DROP FUNCTION IF EXISTS bi.insert_bi_trash_river(uuid[]);

CREATE OR REPLACE FUNCTION bi.insert_bi_trash_river(campaigns_uuids uuid[])
RETURNS BOOLEAN AS $$

BEGIN

      INSERT INTO bi.trash_river (

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
                                  createdon

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
        closest_r.name river_name

      FROM
        (SELECT * FROM campaign.trah WHERE id_ref_campaign_fk IN (SELECT UNNEST(campaign_id)) t

      INNER JOIN LATERAL (

        SELECT
        *
        FROM
        referential.river r
        WHERE name IS NOT NULL

        ORDER BY r.the_geom <-> t.the_geom
        LIMIT 1
        ) closest_r ON TRUE

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
        current_timestamp
      FROM
        subquery_1;

      DROP INDEX IF EXISTS bi_trash_river_closest_point_the_geom;
      CREATE INDEX bi_trash_river_closest_point_the_geom on bi.trash_river using gist(closest_point_the_geom);

RETURN TRUE;
END;

$$ LANGUAGE plpgsql;


SELECT * FROM bi.insert_bi_trash_river(campaigns_uuids=>ARRAY[uuid1, uuid2, uuid3]);



