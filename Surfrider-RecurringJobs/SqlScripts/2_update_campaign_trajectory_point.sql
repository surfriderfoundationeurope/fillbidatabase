DO $$

DECLARE campaign_ids uuid[] := ARRAY[@campaign_ids];
BEGIN

	UPDATE bi.trajectory_point
	SET speed = (distance/EXTRACT(epoch FROM time_diff))*3.6
	WHERE speed IS NULL

  AND EXTRACT(epoch FROM time_diff) > 0
  AND distance > 0
  AND id_ref_campaign_fk in (SELECT UNNEST(campaign_ids))
	;
END$$;
