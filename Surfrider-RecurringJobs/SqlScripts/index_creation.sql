/*
This script drops and creates indexes in bi schema
*/

drop index if exists bi_trash_the_geom;
drop index if exists bi_campaign_the_geom;
drop index if exists bi_trajectory_point_the_geom;
drop index if exists bi_trash_river_the_geom;
drop index if exists bi_campaign_river_the_geom;
drop index if exists bi_trajectory_point_river_the_geom;
drop index if exists bi_river_the_geom;

create index bi_trash_the_geom on bi_temp.trash using gist (the_geom);
create index bi_campaign_the_geom on bi_temp.campaign using gist (the_geom);
create index bi_trajectory_point_the_geom on bi_temp.trajectory_point using gist (the_geom);

create index bi_trash_river_the_geom on bi_temp.trash_river using gist (the_geom);
create index bi_campaign_river_the_geom on bi_temp.campaign_river using gist (the_geom);
create index bi_trajectory_point_river_the_geom on bi_temp.trajectory_point_river using gist (the_geom);
create index bi_river_the_geom on bi_temp.river using gist (the_geom);