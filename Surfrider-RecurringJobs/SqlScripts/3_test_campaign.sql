/*
This script tests data
*/

-- QUERY 1: Test data campaign
DO
$$
DECLARE
	test bool = False;
BEGIN

    SELECT
      sum((rc.id IS NOT NULL)::int) > 0 INTO test

    FROM bi_temp.campaign  c
    INNER JOIN referential.country rc ON st_contains(rc.the_geom, c.start_point) OR st_contains(rc.the_geom, c.end_point)
    WHERE c.id  IN (@campaignID)
    GROUP BY c.id
    ;

IF NOT TEST THEN
	 RAISE EXCEPTION 'country referential is not available yet for this campaign';
END IF;

END;
$$
LANGUAGE plpgsql;

