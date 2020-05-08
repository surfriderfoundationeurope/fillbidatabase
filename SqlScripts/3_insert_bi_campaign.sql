DO $$

DECLARE campaign_ids uuid[] := ARRAY[@campaign_ids];

BEGIN

    INSERT INTO bi.campaign (

                              id_ref_campaign_fk,
 							  locomotion,
							  isaidriven,
							  remark,
							  id_ref_user_fk,
							  riverside,
							  container_url,
							  blob_name,
							  id_ref_model_fk,
                              start_date,
                              end_date,
                              duration,
                              start_point,
                              end_point,
                              distance_start_end,
                              total_distance,
                              avg_speed,
                              trash_count,
							  createdon

                              )
SELECT
    DISTINCT ON (c.id)

        c.id,
        c.locomotion,
        c.isaidriven,
        c.remark,
        c.id_ref_user_fk,
        c.riverside,
        c.container_url,
        c.blob_name,
        c.id_ref_model_fk,
        point.min_time,
        point.max_time,
        age(point.max_time,point.min_time),
        start_geom.the_geom,
        end_geom.the_geom,
        st_distance(start_geom.the_geom, end_geom.the_geom),
        agg.total_distance,
        agg.avg_speed,
        trash_n.trash_count,
        current_timestamp


    FROM campaign.campaign c

    LEFT JOIN (

              SELECT
                id_ref_campaign_fk,
                min(time) min_time,
                max(time) max_time

              FROM campaign.trajectory_point
              GROUP BY id_ref_campaign_fk

                ) point ON point.id_ref_campaign_fk = c.id

    LEFT JOIN campaign.trajectory_point start_geom ON start_geom.id_ref_campaign_fk = c.id AND start_geom.time = point.min_time
    LEFT JOIN campaign.trajectory_point end_geom   ON end_geom.id_ref_campaign_fk = c.id AND end_geom.time = point.max_time

    LEFT JOIN  (

              SELECT
                id_ref_campaign_fk,
                sum(distance) total_distance,
                avg(speed) avg_speed
              FROM campaign.trajectory_point tp
              WHERE distance > 0
              GROUP BY id_ref_campaign_fk

                ) agg ON agg.id_ref_campaign_fk = c.id


    LEFT JOIN (

                SELECT
                  id_ref_campaign_fk,
                  count(*) trash_count
                FROM campaign.trash
                GROUP BY id_ref_campaign_fk

               ) trash_n ON trash_n.id_ref_campaign_fk = c.id


    WHERE c.id in (select unnest(campaign_ids))
    ;

    DROP INDEX IF EXISTS bi.campaign_id_ref_campaign_fk;
    CREATE INDEX campaign_id_ref_campaign_fk on bi.campaign (id_ref_campaign_fk);

    DROP INDEX IF EXISTS bi.campaign_start_point;
    CREATE INDEX campaign_start_point on bi.campaign using gist(start_point);

    DROP INDEX IF EXISTS bi.campaign_end_point;
    CREATE INDEX campaign_end_point on bi.campaign using gist(end_point);

END$$;





