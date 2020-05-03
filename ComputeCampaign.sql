

INSERT INTO bi.campaign (id_ref_campaign_fk, start_date, end_date, duration, start_point, end_point, distance_start_end, total_distance, avg_speed, trash_count)
select
distinct on (c.id)
 c.id,
point.min_time,
point.max_time,
age(point.max_time,point.min_time),
start_geom.the_geom,
end_geom.the_geom,
st_distance(start_geom.the_geom, end_geom.the_geom),
agg.total_distance,
agg.avg_speed,
trash_n.trash_count
FROM campaign.campaign c
LEFT JOIN (SELECT id_ref_campaign_fk, min(time) min_time, max(time) max_time from campaign.trajectory_point group by id_ref_campaign_fk) point on point.id_ref_campaign_fk = c.id
LEFT JOIN campaign.trajectory_point start_geom ON start_geom.id_ref_campaign_fk = c.id and start_geom.time = point.min_time
LEFT JOIN campaign.trajectory_point end_geom   ON end_geom.id_ref_campaign_fk = c.id and end_geom.time = point.max_time
LEFT JOIN  (
 SELECT
id_ref_campaign_fk,
sum(distance) total_distance,
avg(speed) avg_speed
FROM campaign.trajectory_point tp
WHERE distance > 0
GROUP BY id_ref_campaign_fk
) agg on agg.id_ref_campaign_fk = c.id
LEFT JOIN (select id_ref_campaign_fk, count(*) trash_count from campaign.trash group by id_ref_campaign_fk) trash_n on trash_n.id_ref_campaign_fk = c.id

----------------------------------------------------------------------------------------------------------------------------------





