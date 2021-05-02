-- work in progress


DO $$

DECLARE campaign_ids uuid[] := ARRAY[@campaign_ids];

BEGIN




drop table if exists trash_06 ;
create table trash_06 as

/* ------------------------------------------------------------------------------------------------------------------------ */
SELECT
	t.*
FROM
	raw_data.trash t

INNER JOIN raw_data.departement d on  st_intersects(t.the_geom, d.geometry)
WHERE d.insee_dep = '06'
;


/* ------------------------------------------------------------------------------------------------------------------------ */
drop table if exists hydro_node_06 ;
create table hydro_node_06 as

/* ------------------------------------------------------------------------------------------------------------------------ */
SELECT
	node.*
FROM
	raw_data.noeud_hydrographique node

INNER JOIN raw_data.departement d on  st_intersects(node.geometry, d.geometry)
WHERE d.insee_dep = '06'
;

/* ------------------------------------------------------------------------------------------------------------------------ */
DROP TABLE IF EXISTS river_06;
CREATE TABLE river_06 as
select
	river.*,
	st_force2d(river.geometry) as the_geom
FROM
	raw_data.troncon_hydrographique  river
INNER join  raw_data.departement d on  st_intersects(river.geometry, d.geometry)
WHERE   d.insee_dep = '06';
drop index if exists river_06_the_geom; create index river_06_the_geom on river_06 using gist(the_geom);

/* ------------------------------------------------------------------------------------------------------------------------ */
DROP TABLE IF EXISTS cours_d_eau_06;
CREATE TABLE cours_d_eau_06 as
select
	c.*
FROM
	raw_data.cours_d_eau  c
INNER join  raw_data.departement d on  st_intersects(c.geometry, d.geometry)
WHERE   d.insee_dep = '06';

/* ------------------------------------------------------------------------------------------------------------------------ */
------------------------------------------------------------------------
------------------------------------------------------------------------
------------------------------------------------------------------------
------------------------------------------------------------------------

-- Build topology

CREATE EXTENSION postgis_topology;
SET search_path = topology,public;
select topology.DropTopology('river_topo');
SELECT topology.CreateTopology('river_topo', 2154);
SELECT topology.AddTopoGeometryColumn('river_topo', 'public', 'river_06', 'topo_geom', 'LINESTRING');
UPDATE river_06 SET topo_geom = topology.toTopoGeom(the_geom, 'river_topo', 1, 1.0);

/* ------------------------------------------------------------------------------------------------------------------------ */
------------------------------------------------------------------------
------------------------------------------------------------------------
------------------------------------------------------------------------
------------------------------------------------------------------------

alter table river_topo.edge_data add cost float;
update river_topo.edge_data set cost = st_length(geom) ;
alter table river_topo.node add the_geom geometry;
alter table river_topo.node add node_buffer geometry;

update river_topo.node set the_geom = geom;
update river_topo.node set node_buffer = st_buffer(the_geom, 100);

create index river_topo_node_the_geom_buffer on  river_topo.node using gist(node_buffer);
create index river_topo_node_the_geom on  river_topo.node using gist(the_geom);

alter table river_topo.node add limit_land_sea bool;
alter table river_topo.node add confluent_id text;
alter table river_topo.node add is_confluent boolean;
alter table river_topo.node add id_ce_amon text;
alter table river_topo.node add id_ce_aval text;

update river_topo.node
set limit_land_sea= lm.id is not null

FROM raw_data.limite_terre_mer lm
WHERE lm.id IS NOT NULL and st_intersects(lm.geometry, node_buffer)
;

update river_topo.node n
set
	confluent_id  = nh.id,
	is_confluent  = nh.id is not null,
	id_ce_amon    = nh.id_ce_amon,
	id_ce_aval    = nh.id_ce_aval

FROM (select geometry, id, id_ce_amon, id_ce_aval from raw_data.noeud_hydrographique where categorie = 'Confluent') nh
WHERE  nh.id IS NOT NULL  and st_intersects(nh.geometry, n.the_geom)
;

/* ------------------------------------------------------------------------------------------------------------------------ */
------------------------------------------
------------------------------------------
------------------------------------------
------------------------------------------
------------------------------------------

drop index if exists trash_06_the_geom; create index trash_06_the_geom on  trash_06 using gist(the_geom);

DROP TABLE IF EXISTS closest_limit_land_sea;
create table closest_limit_land_sea AS
SELECT
t.the_geom trash_geom,
lls.node_id,
lls.the_geom limit_land_sea_the_geom
FROM
trash_06 t

left join lateral (

	select
	*
	from
	river_topo.node n
	where limit_land_sea

	order by ST_DISTANCE(n.the_geom,t.the_geom)
	limit 50

				) lls on True
;

/* ------------------------------------------------------------------------------------------------------------------------ */
DROP TABLE IF EXISTS closest_node;
create table closest_node AS
SELECT
t.the_geom trash_geom,
lls.node_id,
lls.the_geom closest_node_the_geom
FROM
trash_06 t

left join lateral (

	select
	*
	from
	river_topo.node n
	order by ST_DISTANCE(n.the_geom,t.the_geom)
	limit 1

				) lls on True
;

/* ------------------------------------------------------------------------------------------------------------------------ */
drop table if exists closest_node_temp;
create temp table closest_node_temp as
select distinct node_id from closest_node ;

drop table if exists closest_limit_land_sea_temp;
create temp table closest_limit_land_sea_temp as
select distinct node_id from closest_limit_land_sea;

/* ------------------------------------------------------------------------------------------------------------------------ */
DROP TABLE IF EXISTS shortest_path;
CREATE TABLE shortest_path as
SELECT
pg_r.*
FROM pgr_dijkstra('
   SELECT edge_id::int4 AS id,
          start_node::int4 AS source,
          end_node::int4 AS target,
          cost::float8 AS cost
   FROM river_topo.edge_data'::text,
(select   array_agg(node_id) from closest_node_temp),
(select   array_agg(node_id) from closest_limit_land_sea_temp),
false) pg_r
;


/* ------------------------------------------------------------------------------------------------------------------------ */
drop table shortest_path_visualization;
create table shortest_path_visualization as
select ed.geom from shortest_path sp
inner join river_topo.edge_data ed on sp.edge = ed.edge_id;

select   array_agg(node_id) from closest_limit_land_sea_temp

create temp table best_track as
select
distinct on (start_vid)
start_vid,
end_vid,
sum(cost)

from
shortest_path

group by
start_vid, end_vid
order by start_vid, sum(cost) asc
;

/* ------------------------------------------------------------------------------------------------------------------------ */
DROP TABLE IF EXISTS best_track_viz;
CREATE TABLE best_track_viz as

SELECT
ed.geom

FROM
shortest_path sp

INNER JOIN river_topo.edge_data ed ON sp.edge = ed.edge_id
INNER JOIN best_track   bt ON bt.start_vid = sp.start_vid AND bt.end_vid = sp.end_vid;

END$$;