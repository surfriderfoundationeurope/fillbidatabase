DO $$

DECLARE rivers_id int[] := ARRAY[@rivers_id];
BEGIN

    UPDATE  bi.river
    SET  distance_monitored = st_length(st_intersection(r2.the_geom_monitored,river.the_geom)),
       the_geom_monitored = r2.the_geom_monitored
    FROM
      (
      SELECT
        r.river_name,
        st_union(st_buffer(r.the_geom, 0.01)) the_geom_monitored
      FROM
        bi.campaign_river r

	  INNER JOIN bi.river br on br.name = r.river_name
      WHERE br.id in (select(unnest(rivers_id)))
      GROUP BY r.river_name

      ) r2
    WHERE r2.river_name = river.name
    ;


    UPDATE bi.river
    SET count_trash  = t.count_trash,
        trash_per_km = t.count_trash/(NULLIF(distance_monitored,0)/1000)
    FROM
    (
      SELECT
        tr.river_name,
        count(distinct(tr.id_ref_trash_fk)) count_trash
      FROM
        bi.trash_river tr

	  INNER JOIN bi.river br on br.name = tr.river_name
      WHERE br.id in (select(unnest(rivers_id)))
      GROUP BY tr.river_name

    ) t
    WHERE t.river_name = river.name
    ;

END$$;