SELECT r.id
FROM bi.campaign_river cr
inner join bi.river r on r.name = cr.river_name
WHERE id_ref_campaign_fk in (select unnest(@campaign_ids));

