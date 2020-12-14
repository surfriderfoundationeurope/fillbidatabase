/*
    Récupère les noms de toutes les rivieres pour lesquelles il y a une nouvelle campagne

    ET QU'ELLE EST SUCCESSFULLY COMPUTED PAR LE PIPELINE NON?
 */
SELECT r.name
FROM bi_temp.campaign_river cr
inner join bi_temp.river r on r.name = cr.river_name
inner join bi_temp.pipelines pp on pp.campaign_id = cr.id_ref_campaign_fk
WHERE id_ref_campaign_fk = '@campaignId' AND pp.campaign_has_been_computed = TRUE;

