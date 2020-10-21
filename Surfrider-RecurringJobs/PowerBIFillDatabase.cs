using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
using System.Collections.Generic;
using Npgsql;
using System.IO;
using Azure.Storage.Blobs;

namespace Surfrider.Jobs.Recurring
{
    public static class PowerBIFillDatabase
    {
        public static IDatabase Database;
        public static string ScriptVersion = "0.3";
        private static string ListOfCampaignsIds;
        [FunctionName("PowerBIFillDatabase")]
        public static async Task Run([TimerTrigger("0 0 2 * * *")]TimerInfo myTimer, ILogger logger)// runs everyd ay at 02:00
        {
            Console.WriteLine("USING " + Helper.GetConnectionString());
            Database = new PostgreDatabase(Helper.GetConnectionString());
            await UpdateJsonFileWithDataAsync(55, 66, 77);
            // TODO
            // 0. Faire les modif sur le schéma campaign
            // 0.1 Créer une bd de prod avec juste campaign.campaign, campaign.user, campign.media et le schéma label
            // 1. Faire un mecanisme de retry ou de log d'erreur
            // 2. Log les campaign pour lesquelles on a des erreur de calcul quelque part
            // ==> comme on va faire des batch de campaign, ça sera le batch qui serra en erreur
            // ==> si erreur, on vient pas mettre à jour le status de la campaign dans logs.bi. Par contre, on
            // vient mettre à jour le finished_on, reason et failed_step.
            // >>>> Changer le nom du champs script_version en pipeline_version.

            // 3. Clément : voir si on peut déporter en C# certains calculs
            // 3.1 mettre numéro avec chaque boite du schéma
            // 3.2 Faire un Excel avec chaque boite et une colonne pour dire OUI/NON c'est déportable, et détailler les I/O pour chaque truc.

        //     var startedOn = DateTime.Now;

        //     //    IList<Guid> newCampaignsIds = await RetrieveNewCampaigns(logger);
        //     // ********************************* LOCAL TEST ONLY ***********************
        //     IList<Guid> newCampaignsIds = new List<Guid>();
        //     newCampaignsIds.Add(new Guid("d115922a-3ca9-49f7-b363-06c9383b6563"));
        //     newCampaignsIds.Add(new Guid("2155da04-c2bb-433b-9a90-8ec8b8d74ee9"));
        //     // *************************************************************************
        //     ListOfCampaignsIds = FormatGuidsForSQL(newCampaignsIds);
        //     PipelineStatus.Status = OperationStatus.OK;
        //     PipelineStatus.Reason = string.Empty;
            
        //     await ExecuteScript(@"./SqlScripts/2_update_campaign_trajectory_point.sql");
        //     if(PipelineStatus.Status == OperationStatus.OK)
        //      await ExecuteScript(@"./SqlScripts/3_insert_bi_campaign.sql");//inserts new campaigns into BI db schema
        //     if(PipelineStatus.Status == OperationStatus.OK)
        //    await ExecuteScript(@"./SqlScripts/4_insert_bi_campaign_distance_to_sea.sql");
        //     if(PipelineStatus.Status == OperationStatus.OK)
        //    await ExecuteScript(@"./SqlScripts/5_insert_bi_trajectory_point_river.sql"); //Pour chaque Trash on récupère la rivière associée et on projete la geometry du trash sur la rivière
        //     if(PipelineStatus.Status == OperationStatus.OK)
        //    await ExecuteScript(@"./SqlScripts/6_insert_bi_campaign_river.sql");
        //     if(PipelineStatus.Status == OperationStatus.OK)
        //    await ExecuteScript(@"./SqlScripts/7_get_bi_rivers_id.sql");
        //     if(PipelineStatus.Status == OperationStatus.OK)
        //    await ExecuteScript(@"./SqlScripts/8_update_bi_trash.sql");
        //     if(PipelineStatus.Status == OperationStatus.OK)
        //    await ExecuteScript(@"./SqlScripts/9_insert_bi_trash_river.sql");
        //     if(PipelineStatus.Status == OperationStatus.OK)
        //    await ExecuteScript(@"./SqlScripts/10_update_bi_river.sql");

            // await CleanErrors(); // on vient clean toutes les campagnes pour lesquelles on a eu un probleme de calcul à un moment

            Console.WriteLine("-------------------- ALL DONE ---------------------");

        }

        private static string FormatGuidsForSQL(IList<Guid> newCampaignsIds)
        {
            var res = "";
            for(int i = 0; i < newCampaignsIds.Count; i++){
                
                res += "'" + newCampaignsIds[i] + "'";
                if(i < newCampaignsIds.Count - 1)
                    res += ",";
            }
            return res;
        }

        private static async Task CleanErrors()
        {
           //
        }

        private static async Task ExecuteScript(string scriptPath){
            string command = string.Empty;
            try {
                command = System.IO.File.ReadAllText(scriptPath);
            }catch(Exception e){
                Console.WriteLine($"-------------- ERROR READING SQL FILE {scriptPath}");
                 PipelineStatus.Status = OperationStatus.ERROR;
                 PipelineStatus.Reason = e.ToString();
            }
            if(command != string.Empty){
                try {

                command = command.Replace("@campaign_ids", ListOfCampaignsIds);
                await Database.ExecuteNonQuery(command);
                }catch(Exception e){
                    Console.WriteLine($"-------------- ERROR DURING SQL FILE EXECUTION {scriptPath}");
                    PipelineStatus.Status = OperationStatus.ERROR;
                    PipelineStatus.Reason = e.ToString();
                }
            }
           PipelineStatus.Status = OperationStatus.OK;

        }

        private static async Task InsertLog(DateTime startedOn, OperationStatus status, ILogger log)
        {
            var finishedOn = DateTime.Now;
            var elapsedTime = finishedOn - startedOn;
            // see https://stackoverflow.com/a/23163325/12805412 
            var command = $"INSERT INTO bi.Logs VALUES (@id, @startedOn, @finishedOn, @elapsedTime, @status)";
            IDictionary<string, object> args = new Dictionary<string, object>();
            args.Add("@id", Guid.NewGuid());
            args.Add("@startedOn", startedOn);
            args.Add("@finishedOn", finishedOn);
            args.Add("@elapsedTime", elapsedTime.TotalSeconds);
            args.Add("@status", status.ToString());
            await Database.ExecuteNonQuery(command, args);
        }

        private static async Task<IList<Guid>> RetrieveNewCampaigns(ILogger log)
        {

            IList<Guid> campaigns = new List<Guid>();
            var current_ts = new DateTime(2020, 05, 04);
            var command = $"SELECT id FROM campaign.campaign WHERE createdon >=  '{current_ts}'";
            using (var conn = new NpgsqlConnection(Helper.GetConnectionString()))
                {
                    conn.Open();
                    using (var cmd = new NpgsqlCommand())
                    {
                        cmd.Connection = conn;
                        cmd.CommandText = command;
                        using (var reader = await cmd.ExecuteReaderAsync())
                        {
                            
                            while (reader.Read())
                            {
                                campaigns.Add(reader.GetFieldValue<Guid>(0));
                            }
                            
                        }
                    }
                }
                
            return campaigns;
        }

        private static async Task UpdateJsonFileWithDataAsync(int contributors, int coveredKm, int trashPerKm){
            // Create a BlobServiceClient object which will be used to create a container client
            Console.WriteLine("USING BLOB STORAGE CONNECTION STRING --> " + Helper.GetBlobStorageConnectionString());
            //BlobServiceClient blobServiceClient = new BlobServiceClient(Helper.GetBlobStorageConnectionString());

            // Create the container if not exists and return a container client object
            string containerName = "public";
            BlobContainerClient containerClient = new BlobContainerClient(Helper.GetBlobStorageConnectionString(),containerName);
            containerClient.CreateIfNotExists(Azure.Storage.Blobs.Models.PublicAccessType.BlobContainer);
            
            // Create a local file in the ./data/ directory for uploading and downloading
            string localPath = "./";
            string fileName = "data_home_page.json";
            string localFilePath = Path.Combine(localPath, fileName);
            // Write text to the file
            await File.WriteAllTextAsync(localFilePath, "{\"contributors\": " + contributors + ",\"coveredKm\": " + coveredKm + ",\"trashPerKm\": " + trashPerKm + "}");

            // Get a reference to a blob
            BlobClient blobClient = containerClient.GetBlobClient(fileName);

            Console.WriteLine("Uploading to Blob storage as blob:\n\t {0}\n", blobClient.Uri);
            // Open the file and upload its data
            using (FileStream file = File.OpenRead(localFilePath))
            {
                await blobClient.UploadAsync(file);
            }

        }
    }

}
