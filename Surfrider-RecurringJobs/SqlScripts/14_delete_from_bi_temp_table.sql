/*
This script delete data in bi_temp schema
*/

SELECT campaign_id
FROM bi_temp.pipelines
WHERE campaign_has_been_computed = True AND river_has_been_computed = True
;

DELETE FROM bi_temp.trash WHERE pipeline_id in (@pipelineID);
DELETE FROM bi_temp.campaign WHERE pipeline_id in (@pipelineID);
DELETE FROM bi_temp.trajectory_point WHERE pipeline_id in (@pipelineID);

DELETE FROM bi_temp.campaign_river WHERE pipeline_id in (@pipelineID);
DELETE FROM bi_temp.trash_river WHERE pipeline_id in (@pipelineID);
DELETE FROM bi_temp.trajectory_point_river WHERE pipeline_id in (@pipelineID);
