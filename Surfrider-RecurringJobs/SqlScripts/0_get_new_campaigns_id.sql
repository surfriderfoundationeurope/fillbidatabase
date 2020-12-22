/*
This query returns new campaign_id to be processed by the pipeline
*/
SELECT id
FROM campaign.campaign
WHERE has_been_computed = Null
;
