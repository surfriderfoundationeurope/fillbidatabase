/*
This script delete data in bi_temp schema
*/

DELETE FROM bi_temp.trash WHERE pipeline_id in (@pipelineID);
DELETE FROM bi_temp.campaign WHERE pipeline_id in (@pipelineID);
DELETE FROM bi_temp.trajectory_point WHERE pipeline_id in (@pipelineID);

DELETE FROM bi_temp.campaign_river WHERE pipeline_id in (@pipelineID);
DELETE FROM bi_temp.trash_river WHERE pipeline_id in (@pipelineID);
DELETE FROM bi_temp.trajectory_point_river WHERE pipeline_id in (@pipelineID);
