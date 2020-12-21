SELECT campaign_id
FROM bi_temp.pipelines
WHERE campaign_has_been_computed = True 
AND river_has_been_computed = True 
; 

-- Puis copier pour ces campaign_id les data du schéma bi_temp dans le schéma bi (inverse du T100) ? 
