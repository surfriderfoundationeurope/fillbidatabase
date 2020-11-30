SELECT r.name
FROM bi_temp.campaign_river cr
inner join bi_temp.river r on r.name = cr.river_name
WHERE id_ref_campaign_fk in (@campaign_ids);

