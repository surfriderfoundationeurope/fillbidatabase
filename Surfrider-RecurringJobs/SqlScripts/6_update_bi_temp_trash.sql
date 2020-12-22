/*
this script computes:
            - municipality_code
            - municipality_name
            - state_code
            - state_name
            - country_code
            - country_name

for table bi_temp.trash
*/

-- QUERY 1: Associates each trash to an administrative area
CREATE TEMP TABLE trash_admin AS
SELECT
	t.id,
	mun.code as municipality_code,
	mun.name as municipality_name,
	dep.code as department_code,
	dep.name as department_name,
	s.code as state_code,
	s.name as state_name,
	c.code as country_code,
	c.name as country_name
FROM
	bi_temp.trash  t

LEFT JOIN referential.municipality mun on st_contains(mun.the_geom, t.the_geom)
LEFT JOIN referential.department dep on dep.id = mun.id_ref_department_fk
LEFT JOIN referential.state s on s.id = dep.id_ref_state_fk
LEFT JOIN referential.country c on c.id = s.id_ref_country_fk

WHERE t.id_ref_campaign_fk in (@campaignID);
;


-- QUERY 2: updates values in bi_temp.trash
UPDATE bi_temp.trash t
SET
	municipality_code = ta.municipality_code,
	municipality_name = ta.municipality_name,
	department_code = ta.department_code,
	department_name = ta.department_name,
	state_code = ta.state_code,
	state_name = ta.state_name,
	country_code = ta.country_code,
	country_name = ta.country_name

FROM trash_admin ta
WHERE ta.id = t.id
;

-- QUERY 3: creates partial spatial indexes
DROP INDEX IF EXISTS bi_temp_trash_geom;
CREATE INDEX bi_temp_trash_geom ON bi_temp.trash using gist(the_geom)
WHERE id_ref_campaign_fk IN (@campaignID);