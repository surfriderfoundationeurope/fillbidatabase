/*
This script returns river_name for a specific campaignId
*/
SELECT cr.river_name
FROM bi_temp.campaign_river cr
WHERE id_ref_campaign_fk in (@campaignID);
