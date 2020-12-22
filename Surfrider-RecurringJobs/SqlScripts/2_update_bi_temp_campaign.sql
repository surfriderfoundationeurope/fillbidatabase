/*
this script computes:
            - start_date (minimum timestamp of the campaign)
            - end_date (biggest timestamp of the campaign)
            - duration (difference between start_date and end_date of the campaign)
            - start_point (first point of the campaign)
            - end_point (last point of the campaign)
            - distance_start_end (distance between start_point and end_point)

for table bi_temp.campaign
*/

-- QUERY 1: drops and creates indexes
DROP INDEX IF EXISTS bi_temp_campaign_id;
CREATE INDEX bi_temp_campaign_id
ON bi_temp.campaign (id)
WHERE id IN (@campaignID);

-- QUERY 2: update start_date, end_date and duration
UPDATE  bi_temp.campaign c
SET start_date  = point.min_time,
	end_date=point.max_time,
	duration = age(point.max_time,point.min_time)

FROM (
       SELECT

       id_ref_campaign_fk,
       min(time) min_time,
       max(time) max_time

       FROM campaign.trajectory_point
       GROUP BY id_ref_campaign_fk

     ) point

WHERE point.id_ref_campaign_fk = c.id AND c.id in (@campaignID);

-- QUERY 3: updates start_point
UPDATE  bi_temp.campaign c
SET start_point = start_geom.the_geom
FROM bi_temp.trajectory_point start_geom
WHERE start_geom.id_ref_campaign_fk = c.id and start_geom.time = c.start_date and c.id in (@campaignID);

-- QUERY 4: updates end_point
UPDATE  bi_temp.campaign c
SET end_point = end_geom.the_geom
FROM bi_temp.trajectory_point end_geom
WHERE end_geom.id_ref_campaign_fk = c.id and end_geom.time = c.end_date and c.id in (@campaignID);

-- QUERY 5: updates distance_start_end
UPDATE  bi_temp.campaign c
SET distance_start_end = st_distance(start_point, end_point)
WHERE c.id in (@campaignID);

-- QUERY 6: updates total distance and avg speed
UPDATE bi_temp.campaign c
SET total_distance = agg.total_distance,
	avg_speed = agg.avg_speed
FROM (
			SELECT
                id_ref_campaign_fk,
                sum(distance) total_distance,
                avg(speed) avg_speed
              FROM bi_temp.trajectory_point tp
              WHERE distance > 0
              GROUP BY id_ref_campaign_fk
  ) agg
where agg.id_ref_campaign_fk = c.id and c.id in (@campaignID);


-- QUERY 7: updates trash_count and createdon
UPDATE bi_temp.campaign c
SET trash_count = trash_n.trash_count,
	createdon = current_timestamp
FROM (
          SELECT
            id_ref_campaign_fk,
            count(*) trash_count
          FROM bi_temp.trash
          GROUP BY id_ref_campaign_fk

      ) trash_n

WHERE trash_n.id_ref_campaign_fk = c.id AND c.id IN (@campaignID);

-- QUERY 8: drop and creates spatial partial indexes

DROP INDEX IF EXISTS bi_temp.campaign_start_point;
CREATE INDEX campaign_start_point ON bi_temp.campaign USING gist(start_point)
WHERE id_ref_campaign_fk IN (@campaignID);

;

DROP INDEX IF EXISTS bi_temp.campaign_end_point;
CREATE INDEX campaign_end_point ON bi_temp.campaign USING gist(end_point);
WHERE id_ref_campaign_fk IN (@campaignID)
;
