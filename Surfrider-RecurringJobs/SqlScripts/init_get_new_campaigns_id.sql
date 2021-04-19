SELECT id AS pipeline_id, campaign_id
FROM bi_temp.pipelines 
WHERE campaign_has_been_computed IS NULL 
	  AND river_has_been_computed  IS NULL