/*
This script migrates from bi_temp to bi schema
*/

-- QUERY 1: migration for table campaign
insert into bi.campaign (id,locomotion,isaidriven,remark,id_ref_user_fk,riverside,start_date,end_date,start_point,end_point,total_distance,avg_speed,duration,start_point_distance_sea,end_point_distance_sea,trash_count,distance_start_end,id_ref_model_fk,createdon)
select
id, locomotion, isaidriven, remark, id_ref_user_fk, riverside, start_date, end_date, start_point, end_point, total_distance, avg_speed, duration, start_point_distance_sea, end_point_distance_sea, trash_count, distance_start_end, id_ref_model_fk, createdon
from bi_temp.campaign
where pipeline_id in (@pipeline_ids) AND id_ref_campaign_fk in (@campaign_ids)
;

-- QUERY 2: migration for table capaign_river
insert into bi_temp.campaign_river (id, id_ref_campaign_fk, river_name, id_ref_river_fk, distance, the_geom, createdon)
select
id, id_ref_campaign_fk, river_name, id_ref_river_fk, distance, the_geom, createdon
from bi_temp.campaign_river
where pipeline_id  in (@pipeline_ids) AND id_ref_campaign_fk in (@campaign_ids)
;

-- QUERY 3: migration for table trajectory_point
insert into bi.trajectory_point (id, the_geom, id_ref_campaign_fk, elevation, distance, time_diff, time, speed, lat, lon, createdon)
select
id, the_geom, id_ref_campaign_fk, elevation, distance, time_diff, time, speed, lat, lon, createdon
from bi_temp.trajectory_point tp
where pipeline_id in (@pipeline_ids) AND id_ref_campaign_fk in (@campaign_ids)
;

-- QUERY 4: migration for table trajectory_point_river
insert into bi.trajectory_point_river (id, id_ref_trajectory_point_fk, id_ref_campaign_fk, id_ref_river_fk, trajectory_point_the_geom, river_the_geom, closest_point_the_geom, distance_river_trajectory_point, projection_trajectory_point_river_the_geom, importance, river_name, createdon)
select
id, id_ref_trajectory_point_fk, id_ref_campaign_fk, id_ref_river_fk, trajectory_point_the_geom, river_the_geom, closest_point_the_geom, distance_river_trajectory_point, projection_trajectory_point_river_the_geom, importance, river_name, createdon
from bi_temp.trajectory_point_river
where pipeline_id in (@pipeline_ids) AND id_ref_campaign_fk in (@campaign_ids)
;

-- QUERY 5: migration for table trash
insert into bi.trash (id, id_ref_campaign_fk, the_geom, elevation, id_ref_trash_type_fk, precision, id_ref_model_fk,   time, lat, lon, municipality_code, municipality_name, department_code, department_name, state_code, state_name, country_code, country_name, createdon)
select
id, id_ref_campaign_fk, the_geom, elevation, id_ref_trash_type_fk, precision, id_ref_model_fk,   time, lat, lon, municipality_code, municipality_name, department_code, department_name, state_code, state_name, country_code, country_name, createdon
from bi_temp.trash
where pipeline_id in (@pipelineID) AND id_ref_campaign_fk in (@campaignID)
;

-- QUERY 6: migration for table trash_river
insert into bi.trash_river (id, id_ref_trash_fk, id_ref_campaign_fk, id_ref_river_fk, trash_the_geom, river_the_geom, closest_point_the_geom, distance_river_trash, projection_trash_river_the_geom, importance, river_name, createdon)
select
id, id_ref_trash_fk, id_ref_campaign_fk, id_ref_river_fk, trash_the_geom, river_the_geom, closest_point_the_geom, distance_river_trash, projection_trash_river_the_geom, importance, river_name, createdon
from bi_temp.trash_river
where pipeline_id in (@pipelineID) AND id_ref_campaign_fk in (@campaignID)
;

-- QUERY 7: update table river
update bi.river
set count_trash = bi_temp_r.count_trash ,
    distance_monitored = bi_temp_r.distance_monitored ,
    the_geom_monitored = bi_temp_r.the_geom_monitored ,
    trash_per_km = bi_temp_r.trash_per_km
from bi_temp.river bi_temp_r
where bi_temp_r.name = bi_temp_r.name AND id_ref_campaign_fk in (@campaignID) AND pipeline_id in (@pipelineID)
;
