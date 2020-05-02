DROP FUNCTION IF EXISTS bi.get_campaigns_ids(timestampt);
CREATE OR REPLACE FUNCTION bi.get_campaigns_ids(some_date timestamp) RETURNS SETOF uuid AS $$

BEGIN
RETURN QUERY (SELECT id FROM campaign.campaign WHERE createdon >= some_date);
END;

$$ LANGUAGE plpgsql;

SELECT * FROM bi.get_campaigns_ids(some_date =>  '{last_run}':timestamp );