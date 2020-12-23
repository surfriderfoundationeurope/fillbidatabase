## SqlScript documentation



### init_get_new_campaigns_id.sql

Goal:

* returns a list of id_campaign to process

  

Tables involved:

- Campaign.campaign 



Parameters: 

- None

### 0_insert_main_tables.sql

Goal: 

* data migration of new campaign from campaign.* to bi_temp.*

  

Tables involved:

* bi_temp.pipelines

* Campaign.*

* bi_temp.*

  

Parameters: 

- campaignID
- pipelineID

Todo :

- Add partial index on all tables

### 1_update_bi_temp_trajectory_point.sql

Goal: 

* computes the following fields for each trajectory point
  * distance
  * time_diff
  * speed
  * lat
  * Lon

Tables involved: 

- bi_temp.trajectory_point

Parameters:

- campaignID

### 2_update_bi_temp_campaign.sql

Goal: 

* Computes the following fields for each campaign 
  * start_date
  * end_date
  * duration
  * start_point
  * end_point
  * distance_start_end

Tables involved: 

* bi_temp.campaign

Parameters:

* campaignID
* pipelineID

### 3_test_campaign.sql

Goal:

* unit test data campaign:
  * raise exception if the campaign is not contained in one country of the referential
  * ... more to come

Tables involved: 

* Referential.country
* bi_temp.campaign

Parameters:

* campaignID



### 4_insert_bi_temp_trajectory_point.sql

Goal: 

* Associates each trajectory point to a river section
* Computes the following field :
  * trajectory point projection on river section
  * distance between observed and estimated geopoint

Tables involved: 

* bi_temp.trajectory_point
* referential.river

Parameters:

* campaignID
* pipelineID



### 5_insert_bi_temp_campaign_river.sql

Goal: 

* associates each campaign to a river section 
* computes the following fields: 
  * campaign geom on referential river
  * campaign distance on referential river

Tables involved: 

* bi_temp.campaign

Parameters:

* campaignID
* pipelineID



### 6_update_bi_temp_trash.sql

Goal: 

* computes the following fields : 
  * Municipality_code
  * municipality_name 
  * State_code
  * State_name
  * Country_code
  * Country_name

Tables involved: 

* referential.country
* bi_temp.trash

Parameters:

* campaignID
* pipelineID

### 7_insert_bi_temp_trash_river.sql

Goal: 

* associates each trash to a river section 
* computes the following field 
  * distance between river section and observed trash 
  * projection of the trash on the river section

Tables involved: 

* bi_temp.trash 
* Referential.river

Parameters:

* campaignID
* piplineID



### 8_get_old_campaign_id.sql

Goal: 

- This script returns oldCampaignID 

Tables involved: 

- bi.campaign_river
- bi_temp.campaign_river
- bi_temp.pipelines

Parameters:

* campaignID

### 9_get_river_name.sql

Goal: 

* returns river_name for a campaignID

Tables involved: 

*  Bi_temp.campaign_river

Parameters:

* campaignID

TODO:

* rename file

### 10_import_bi_table_to_bi_temp.sql

Goal: 

* data migration from bi.* to bi_temp.*

Tables involved: 

* bi_temp.campaign_river
* Bi_temp.trash_river

Parameters:

* oldCampaignID



### 11_update_bi_river.sql

Goal: 

* update the following fields :
  * distance monitored
  * the_geom_monitored
  * count_trash 
  * Trash_per_km

Tables involved: 

* bi_temp.river
* bi_temp.campaign_river
* bi_temp.trash_river

Parameters:

* campaignID
* pipelineID



### 12_import_bi_temp_table_to_bi.sql

Goal: 

* data miragration from bi_temp.* to bi.*

Tables involved: 

_insert_ into:

* bi.campaign from bi_temp.campaign
* bi.campaign_river from bi_temp.campaign_river
* bi.trajectory_point from bi_temp.trajectory_point
* bi.trajectory_point_river from bi_temp.bi.trajectory_point_river
* Bi.trash from bi_temp.trash 

_update_: 

* bi.river from bi_temp.river



Parameters:

* campaignID
* pipelineID



### 13_test_campaign_processing.sql

Goal:

* returns True if pipeline has been successfully computes for a campaignID: campaign_has_been_computed=True, river_has_been_computed=True

Tables involved: 

* bi_temp.pipelines

Parameters:

* campaignID



### 14_delete_from_bi_temp_table.sql

Goal: 

* Delete data from bi_temp schema for a pipelineID

Tables involved: 

* bi_temp.trash 
* bi_temp.campaign 
* bi_temp.trajectory_point
* bi_temp.campaign_river
* bi_temp.trash_river
* bi_temp.trajectory_point_river

Parameters:

* pipelineID







