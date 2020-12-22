/*
This script returns oldCampaignId
*/
SELECT
    bi_cr.id_ref_campaign_fk
FROM bi.campaign_river bi_cr
INNER join bi_temp.campaign_river bi_temp_cr ON bi_temp_cr.river_name = bi_cr.river_name
INNER join bi_temp.pipelines p ON p.campaign_id = bi_temp_cr.id_ref_campaign_fk and p.campaign_has_been_computed = true
WHERE bi_temp_cr.id_ref_campaign_fk in (@campaignID)
;