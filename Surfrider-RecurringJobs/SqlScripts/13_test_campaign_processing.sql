/*
This script returns TRUE if all processing has been properly compute
*/
SELECT p.pipeline_id IS NOT NULL
FROM bi_temp.pipelines p
WHERE campaign_has_been_computed = True
AND river_has_been_computed = True
AND campaign_id = @campaign_id
;