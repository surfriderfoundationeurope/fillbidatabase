<h1 align="left">Fill Bi Database</h1>

<a href="https://www.plasticorigins.eu/"><img width="80px" src="https://github.com/surfriderfoundationeurope/The-Plastic-Origins-Project/blob/master/assets/PlasticOrigins_logo.png" width="5%" height="5%" align="left" hspace="0" vspace="0"></a>

  <p align="justify">Proudly Powered by <a href="https://surfrider.eu/">SURFRIDER Foundation Europe</a>, this open-source initiative is a part of the <a href="https://www.plasticorigins.eu/">PLASTIC ORIGINS</a> project - a citizen science project that uses AI to map plastic pollution in European rivers and share its data publicly. Browse the <a href="https://github.com/surfriderfoundationeurope/The-Plastic-Origins-Project">project repository</a> to know more about its initiatives and how you can get involved. Please consider starring :star: the project's repositories to show your interest and support. We rely on YOU for making this project a success and thank you in advance for your contributions.</p>

_________________

<!--- OPTIONAL: You can add badges and shields to reflect the current status of the project, the licence it uses and if any dependencies it uses are up-to-date. Plus they look pretty cool! You can find a list of badges or design your own at https://shields.io/ --->

Welcome to **'Fill Bi Database'**, code for the recurring job that fills Bi database schema that allows  to launch a series of SQL scripts.
These scripts do calculations on the campaigns and store the results in a BI database.
The program ensures that the scripts run smoothly in an orderly fashion, when one of the scripts fails to update information in a log table for manual resolution.

>This function runs **every day at 02:00**

### **Principles**

Several ordered SQL request will be executed on the server in order to compute BI data.

![image-20200505161802892](/_images/image-20200505161802892.png)

*In red, the steps we think will take time. In grey, the optionnal step we will do in a second time.*

| Step | Name                                                         | Goal                                                         | Tables involved                                              | Parameters            |
| ---- | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ | --------------------- |
| 000  | Test geometries are in countries we have river referential and test SRID for each campaign |                                                              |                                                              |                       |
| 00   | init_get_new_campaigns_id.sql                                | Returns a list of id_campaign to process                     | Campaign.campaign                                            | None                  |
| 0    | 0_insert_main_tables.sql                                     | Data migration of new campaign from campaign.* to bi_temp.*  | bi_temp.pipelines Campaign.* bi_temp.*                       | campaignID pipelineID |
| 1    | 1_update_bi_temp_trajectory_point.sql                        | computes the following fields for each trajectory point : distance, time_diff, speed, lat, lon | bi_temp.trajectory_point                                     | campaignID            |
| 2    | 2_update_bi_temp_campaign.sql                                | Computes the following fields for each campaign start_date end_date duration start_point end_point distance_start_end | bi_temp.campaign                                             | campaignID pipelineID |
| 3    | 3_test_campaign.sql                                          | unit test data campaign: raise exception if the campaign is not contained in one country of the referential ... more to come | Referential.country bi_temp.campaign                         | campaignID            |
| 4    | 4_insert_bi_temp_trajectory_point_river.sql                  | Associates each trajectory point to a river section Computes the following field :  trajectory point projection on river section distance between observed and estimated geopoint | bi_temp.trajectory_point referential.river                   | campaignID pipelineID |
| 5    | 5_insert_bi_temp_campaign_river.sql                          | associates each campaign to a river section computes the following fields:  campaign geom on referential river campaign distance on referential river | bi_temp.campaign                                             | campaignID pipelineID |
| 6    | 6_update_bi_temp_trash.sql                                   | computes the following fields : Municipality_code municipality_name State_code State_name Country_code Country_name | referential.country bi_temp.trash                            | campaignID pipelineID |
| 7    | 7_insert_bi_temp_trash_river.sql                             | associates each trash to a river section computes the following field  distance between river section and observed trash projection of the trash on the river section | bi_temp.trash Referential.river                              | campaignID piplineID  |
| 8    | 8_get_old_campaign_id.sql                                    | This script returns oldCampaignID                            | bi.campaign_river bi_temp.campaign_river bi_temp.pipelines   | campaignID            |
| 9    | 9_get_river_name.sql                                         | returns river_name for a campaignID                          | Bi_temp.campaign_river                                       | campaignID            |
| 10   | 10_import_bi_table_to_bi_temp.sql                            | data migration from bi.* to bi_temp.*                        | bi_temp.campaign_river Bi_temp.trash_river                   | oldCampaignID         |
| 11   | 11_update_bi_river.sql                                       | update the following fields : distance monitored the_geom_monitored count_trash Trash_per_km | bi_temp.river bi_temp.campaign_river bi_temp.trash_river     | campaignID pipelineID |
| 12   | 12_import_bi_temp_table_to_bi.sql                            | data miragration from bi_temp.* to bi.*                      | *insert* into: bi.campaign from bi_temp.campaign bi.campaign_river from bi_temp.campaign_river bi.trajectory_point from bi_temp.trajectory_point bi.trajectory_point_river from bi_temp.bi.trajectory_point_river Bi.trash from bi_temp.trash *update*: bi.river from bi_temp.river | campaignID pipelineID |
| 13   | 13_test_campaign_processing.sql                              | returns True if pipeline has been successfully computes for a campaignID: campaign_has_been_computed=True, river_has_been_computed=True | bi_temp.pipelines                                            | campaignID            |
| 14   | 14_delete_from_bi_temp_table.sql                             | Delete data from bi_temp schema for a pipelineID             | bi_temp.trash bi_temp.campaign bi_temp.trajectory_point bi_temp.campaign_river bi_temp.trash_river bi_temp.trajectory_point_river | pipelineID            |

### **Architectural Decision**

* Why keep separation between campaign.campaign and campaign.media?
The idea here is to stay aligned with the "S" of the SOLID principles : Single Responsability.
As Campaign and Media are two different entities with different business function/purpose, we need to keep them separated in the Database. It allows to extend the possibilities around campaigns without manipulating it.
If not, in long term, this could be impactful on the campaign table which is the core of the whole Plastic Origins application.

## **Getting Started**

### **Prerequisites**

Before you begin, ensure you have met the following requirements:

* You have installed [`.Net Core 3.1 or lastest`](https://dotnet.microsoft.com/download/dotnet/3.1)
* You have installed the latest version of [`Azure Emulator`](https://docs.microsoft.com/en-us/azure/storage/common/storage-use-emulator) if you want to use on your local machine
* You have a `PostgreSQL 11.6 minimum` database for local use on your machine OR `Microsoft Azure PostgreSQL` (you can create a free account [here](https://azure.microsoft.com/fr-fr/services/postgresql/))
* You have `Visul Studio 2019` or latest version
  Or You have latest version of `Visual code`   

#### **Technical stack**

* Language: `C#`
* Framework: `.Net Core`
* Functionality : `Azure function`
* Unit test framework: `XUnit`

### **Installation**

To install Surfrider-Recuring Jobs, follow these steps:

* Under Visual studio 2019, select the project `Surfrider-Recuring Jobs`, choose the option publish then Azure
* Use the following azure service to host your application: `Azure function application (Windws)`


### **Usage**

To use `Surfrider-RecurringJobs`, follow these steps:

* Update the configuration file `local.settings.json` and specify the following parameters :
  postgre_connection
  blob_storage_connection

<!--- If needed add here any Extra Sections (must have their own titles).Specifically, the Security section should be here if it wasn't important enough to be placed above.-->

### **API references**

* Azure.Identity (1.1.1)
* Azure.Security.keyVault.Secrets (4.0.3)
* Azure.Storage.Blobs (12.4.4)
* Microsoft.NET.Sdk.Functions (3.0.1)
* Npgsql (4.1.3.1)
* System.Data.SqlClient (4.8.1) 

<!--- If an external API file is work in progress and/or you are planning to host API specification in the Swagger documentation, you can use the text below as exaple: add, duplicate or remove as required 
*SOON: To see API specification used by this repository browse to the Swagger documentation (currently not available).* -->

## **Build and Test**

'Fill Bi Database' is easy to test w/ VSCode and a local installation of [Node.js](https://nodejs.org/en/).

In order to test and run 'Fill Bi Database' locally, you need to install the following VSCode extensions:

* [C# extension](https://marketplace.visualstudio.com/items?itemName=ms-dotnettools.csharp)
* [Azure Function extension](https://marketplace.visualstudio.com/items?itemName=ms-azuretools.vscode-azurefunctions)
* Azurite extension (optional, provides a local Azure blob storage emulator)

After extensions installed, please check your local environement can run azure functions : open your Terminal and run "func". Is everything goes well, the installed version is displayed.

To test 'Fill Bi Database', go to Debug section, then click on Run.

To start the function immediately without having to wait util 02 am, you can replace the line below in the class `PowerBIFillDatabase.cs`

public static async Task Run([TimerTrigger("0 0 2 * * *")] TimerInfo myTimer, ILogger logger)// runs everyd ay at 02:00b

by following:

public static async Task Run([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] Microsoft.AspNetCore.Http.HttpRequest req,  ILogger logger)

## **Contributing**

It's great to have you here! We welcome any help and thank you in advance for your contributions.

* Feel free to **report a problem/bug** or **propose an improvement** by creating a [new issue](https://github.com/surfriderfoundationeurope/fillbidatabase/issues). Please document as much as possible the steps to reproduce your problem (even better with screenshots). If you think you discovered a security vulnerability, please contact directly our [Maintainers](##Maintainers).

* Take a look at the [open issues](https://github.com/surfriderfoundationeurope/fillbidatabase/issues) labeled as `help wanted`, feel free to **comment** to share your ideas or **submit a** [**pull request**](https://github.com/surfriderfoundationeurope/fillbidatabase/pulls) if you feel that you can fix the issue yourself. Please document any relevant changes.

## **Maintainers**

If you experience any problems, please don't hesitate to ping:
<!--- Need to check the full list of Maintainers and their GIThub contacts -->
* [@ChristopheHvd](https://github.com/ChristopheHvd)
* [@clembac](https://github.com/clembac)
* [@MaxLemarchand](https://github.com/MaxLemarchand)

Special thanks to all our [Contributors](https://github.com/orgs/surfriderfoundationeurope/people).

## **License**

We’re using the `MIT` License. For more details, check [`LICENSE`](https://github.com/surfriderfoundationeurope/fillbidatabase/blob/master/LICENSE) file.

## **Additional information**

### Good to mention
* Documentation advise the use of AZURE_FUNCTIONS_ENVIRONMENT variable instead of ASPNETCORE_ENVIRONMENT (cf link to doc below).

### Useful links
* Application settings reference for Azure Functions : https://docs.microsoft.com/en-us/azure/azure-functions/functions-app-settings 
