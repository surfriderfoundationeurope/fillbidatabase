INSERT INTO bi_temp.pipelines (campaign_id)
SELECT id
FROM campaign.campaign
WHERE has_been_computed = Null
;