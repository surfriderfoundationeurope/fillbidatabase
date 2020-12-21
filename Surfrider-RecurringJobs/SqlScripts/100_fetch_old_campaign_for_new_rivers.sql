SELECT id_ref_campaign_fk
FROM bi.campaign_river
WHERE river_name IN 
	(
	SELECT DISTINCT A.river_name
	FROM bi_temp.campaign_river A
	INNER JOIN bi_temp.pipelines B
	ON A.id_ref_campaign_fk = B.campaign_id
	-- WHERE A.pipeline_id = [?current_id?]  // comment on récup cet id_pipeline et sous quel format ?
	) 
	;


-- NEXT => aller copier pour toutes ces campaign_id les data du schéma bi dans le schéma bi_temp ? 
-- Sur quel périmètre ? 

