/*
This script migrates from bi schema to bi_temp tables:
    - campaign_river
    - trash_river
*/

-- QUERY 1: migration for campaign_river table
INSERT INTO bi_temp.campaign_river (id,id_ref_campaign_fk, river_name, distance, the_geom, createdon,pipeline_id)
SELECT
DISTINCT ON (id_ref_campaign_fk) -- to remove
bi_cr.id, bi_cr.id_ref_campaign_fk, bi_cr.river_name, bi_cr.distance,bi_cr.the_geom, bi_cr.createdon, @pipelineID
FROM bi.campaign_river bi_cr
WHERE bi.id_ref_campaign_fk in (@oldCampaignId) AND bi_temp_cr.id_ref_campaign_fk <> bi_cr.id_ref_campaign_fk
;

-- QUERY 2: migration for trash_river tables
INSERT INTO bi_temp.trash_river (id, id_ref_trash_fk, id_ref_campaign_fk, id_ref_river_fk, trash_the_geom, river_the_geom, closest_point_the_geom, distance_river_trash, projection_trash_river_the_geom, importance, river_name, createdon, pipeline_id)
SELECT
id, id_ref_trash_fk, id_ref_campaign_fk, id_ref_river_fk, trash_the_geom, river_the_geom, closest_point_the_geom, distance_river_trash, projection_trash_river_the_geom, importance, river_name, createdon, @pipelineID
FROM bi.trash_river bi_tr
WHERE bi_tr.id_ref_campaign_fk in (@oldCampaignId)
;

