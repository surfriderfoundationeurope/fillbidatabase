DROP FUNCTION IF EXISTS bi.update_campaign_trajectory_point(uuid[]);

CREATE OR REPLACE FUNCTION bi.update_campaign_trajectory_point(campaigns_uuids uuid[])
RETURNS BOOLEAN AS $$

BEGIN

		UPDATE bi.trajectory_point
    SET speed = (distance/EXTRACT(epoch FROM time_diff))*3.6
    WHERE speed IS NULL

          AND EXTRACT(epoch FROM time_diff) > 0
          AND distance > 0
          AND id_ref_campaign_fk = (SELECT UNNEST(campaign_id))

     ;
RETURN TRUE;
END;
$$ LANGUAGE plpgsql;


SELECT * FROM bi.update_campaign_trajectory_point(campaigns_uuids=>ARRAY[uuid1, uuid2, uuid3]);