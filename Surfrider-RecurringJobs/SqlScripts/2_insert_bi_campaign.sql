

DROP INDEX IF EXISTS bi_temp.campaign_id ;
CREATE INDEX campaign_id  on bi_temp.campaign (id);


/*
INSERT INTO bi_temp.campaign (

                              id,
                              locomotion,
                              isaidriven,
							                remark,
							                id_ref_user_fk,
							                riverside,
							                id_ref_model_fk,
                              start_date,
                              end_date,
                              duration,
                              start_point,
                              end_point,
                              distance_start_end,
                              total_distance,
                              avg_speed,
                              trash_count,
							                createdon

                              )
SELECT
    DISTINCT ON (c.id)

        c.id,
        c.locomotion,
        c.isaidriven,
        c.remark,
        c.id_ref_user_fk,
        c.riverside,
        c.id_ref_model_fk,
        point.min_time,
        point.max_time,
        age(point.max_time,point.min_time),
        start_geom.the_geom,
        end_geom.the_geom,
        st_distance(start_geom.the_geom, end_geom.the_geom),
        agg.total_distance,
        agg.avg_speed,
        trash_n.trash_count,
        current_timestamp


    FROM campaign.campaign c

    LEFT JOIN (

              SELECT
                id_ref_campaign_fk,
                min(time) min_time,
                max(time) max_time

              FROM campaign.trajectory_point
              GROUP BY id_ref_campaign_fk

                ) point ON point.id_ref_campaign_fk = c.id

    LEFT JOIN campaign.trajectory_point start_geom ON start_geom.id_ref_campaign_fk = c.id AND start_geom.time = point.min_time
    LEFT JOIN campaign.trajectory_point end_geom   ON end_geom.id_ref_campaign_fk = c.id AND end_geom.time = point.max_time

    LEFT JOIN  (

              SELECT
                id_ref_campaign_fk,
                sum(distance) total_distance,
                avg(speed) avg_speed
              FROM bi_temp.trajectory_point tp
              WHERE distance > 0
              GROUP BY id_ref_campaign_fk

                ) agg ON agg.id_ref_campaign_fk = c.id


    LEFT JOIN (

                SELECT
                  id_ref_campaign_fk,
                  count(*) trash_count
                FROM bi_temp.trash
                GROUP BY id_ref_campaign_fk

               ) trash_n ON trash_n.id_ref_campaign_fk = c.id



    WHERE c.id in (@campaign_ids)

    ;
*/

/* ------------------------------------------------------------------------------------------------------------------------ */
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

 WHERE point.id_ref_campaign_fk = c.id AND c.id in (@campaign_ids);

/* ------------------------------------------------------------------------------------------------------------------------ */
UPDATE  bi_temp.campaign c
SET start_point = start_geom.the_geom
FROM bi_temp.trajectory_point start_geom
WHERE start_geom.id_ref_campaign_fk = c.id and start_geom.time = c.start_date and c.id in (@campaign_ids);

/* ------------------------------------------------------------------------------------------------------------------------ */
UPDATE  bi_temp.campaign c
SET end_point = end_geom.the_geom
FROM bi_temp.trajectory_point end_geom
WHERE end_geom.id_ref_campaign_fk = c.id and end_geom.time = c.end_date and c.id in (@campaign_ids);

/* ------------------------------------------------------------------------------------------------------------------------ */
UPDATE  bi_temp.campaign c
SET distance_start_end = st_distance(start_point, end_point)
WHERE c.id in (@campaign_ids);

/* ------------------------------------------------------------------------------------------------------------------------ */
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
where agg.id_ref_campaign_fk = c.id and c.id in (@campaign_ids);


/* ------------------------------------------------------------------------------------------------------------------------ */
UPDATE bi_temp.campaign c
SET trash_count = trash_n.trash_count,
	createdon = current_timestamp
from (

                SELECT
                  id_ref_campaign_fk,
                  count(*) trash_count
                FROM bi_temp.trash
                GROUP BY id_ref_campaign_fk

               ) trash_n

 where trash_n.id_ref_campaign_fk = c.id and c.id in (@campaign_ids);

/* ------------------------------------------------------------------------------------------------------------------------ */
DROP INDEX IF EXISTS bi_temp.campaign_start_point;
CREATE INDEX campaign_start_point on bi_temp.campaign using gist(start_point);

DROP INDEX IF EXISTS bi_temp.campaign_end_point;
CREATE INDEX campaign_end_point on bi_temp.campaign using gist(end_point);

/* ------------------------------------------------------------------------------------------------------------------------ */
DO
$$
DECLARE
	test bool = False;
BEGIN

SELECT
  sum((rc.id is not null)::int) > 0 into test

FROM
bi_temp.campaign  c
inner join referential.country rc on st_contains(rc.the_geom, c.start_point) or st_contains(rc.the_geom, c.end_point)

where c.id  in (@campaign_ids)
group by c.id
;
if not test then
	 raise exception 'country referential is not available yet for this campaign';
end if;

end;
$$
language plpgsql;

/* ------------------------------------------------------------------------------------------------------------------------ */
