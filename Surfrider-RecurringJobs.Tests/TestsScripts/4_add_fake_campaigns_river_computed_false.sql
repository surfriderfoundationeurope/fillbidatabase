-- 
INSERT INTO bi_temp.pipelines (id, campaign_id, campaign_has_been_computed, river_has_been_computed) VALUES (
    uuid_generate_v4(),
    '@campaignId',
    true,
    false
    )