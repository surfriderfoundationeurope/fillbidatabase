INSERT INTO bi_temp.pipelines (id, campaign_id, campaign_has_been_computed) VALUES (
    uuid_generate_v4(),
     '@campaignIdSuccessfullyComputed_1',
     TRUE
     );


INSERT INTO bi_temp.pipelines (id, campaign_id, campaign_has_been_computed) VALUES (
    uuid_generate_v4(),
     '@campaignIdSuccessfullyComputed_2',
     TRUE
     );


INSERT INTO bi_temp.pipelines (id, campaign_id, campaign_has_been_computed) VALUES (
    uuid_generate_v4(),
     '@campaignIdFailedComputed_1',
     FALSE
     );
     -- Adds multiple campaigns where 1 has not been computed successfully (campaign_has_been_computed == FALSE)