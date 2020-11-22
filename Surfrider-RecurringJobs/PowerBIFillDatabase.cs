using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
using System.Collections.Generic;
using Npgsql;
using System.IO;
using Azure.Storage.Blobs;

namespace Surfrider.Jobs
{
    public static class PowerBIFillDatabase
    {
        public static IDatabase Database;
        public static string ScriptVersion = "0.3";
        private static string ListOfCampaignsIds;
        [FunctionName("PowerBIFillDatabase")]
        public static async Task Run([TimerTrigger("0 0 2 * * *")] TimerInfo myTimer, ILogger logger)// runs everyd ay at 02:00
        {
            Console.WriteLine("USING " + Helper.GetConnectionString());
            // Database = new PostgreDatabase(Helper.GetConnectionString());
            // IDataFileWriter fileWriter = new DataFileWriter();
            // await fileWriter.UpdateJsonFileWithDataAsync(55, 66, 77);
            // TODO


            var startedOn = DateTime.Now;
            // 0. RETRIEVE NEW CAMPAIGNS
            IList<Guid> newCampaignsIds = await RetrieveNewCampaigns(logger);
            // 1. COMPUTE ON CAMPAIGNS
            await ComputeOnCampaignsAsync(newCampaignsIds);
            // 2. SELECT RIVERS NAME
            IDictionary<Guid, string> RiversToComputeOn = await SelectRiversAsync();
            // 3. COMPUTE ON RIVERS
            await ComputeOnRiversAsync(RiversToComputeOn);
            // 4. COMMIT PROD DATA
            await CommitProductionDataAsync(newCampaignsIds);



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

            // await CleanErrors(); // on vient clean toutes les campagnes pour ùquelles on a eu un probleme de calcul à un moment

            Console.WriteLine("-------------------- ALL DONE ---------------------");

        }

        private static async Task CommitProductionDataAsync(IList<Guid> newCampaignsIds)
        {
                                                              
        }

        private static async Task ComputeOnRiversAsync(IDictionary<Guid, string> RiversToComputeOn)
        {
            IRiverPipeline RiverPipeline = new RiverPipeline();
            foreach (KeyValuePair<Guid, string> RiverToComputeOn in RiversToComputeOn){
                if(await RiverPipeline.ComputePipelineOnSingleRiverAsync(RiverToComputeOn.Value))
                    await RiverPipeline.MarkRiverPipelineAsSuccessedAsync(RiverToComputeOn.Key);
                else
                    await RiverPipeline.MarkRiverPipelineAsFailedAsync(RiverToComputeOn.Key);
            }
        }

        private static async Task<IDictionary<Guid, string>> SelectRiversAsync()
        {
            IRiverPipeline RiverPipeline = new RiverPipeline();
            return await RiverPipeline.RetrieveSuccessfullComputedCampaignsIds();
        }

        private static async Task ComputeOnCampaignsAsync(IList<Guid> newCampaignsIds)
        {
            var startedOn = DateTime.UtcNow;
            ICampaignPipeline CampaignPipeline = new CampaignPipeline();
            // TODO boucle à optimiser ( /!\ perf )
            foreach(var newCampaignId in newCampaignsIds){
                if (await CampaignPipeline.ComputeOnSingleCampaignAsync(newCampaignId))
                    await CampaignPipeline.MarkCampaignPipelineAsFailedAsync(newCampaignId);
                else
                    await CampaignPipeline.MarkCampaignPipelineAsSuccessedAsync(newCampaignId);
            }
        }

        private static string FormatGuidsForSQL(IList<Guid> newCampaignsIds)
        {
            var res = "";
            for (int i = 0; i < newCampaignsIds.Count; i++)
            {

                res += "'" + newCampaignsIds[i] + "'";
                if (i < newCampaignsIds.Count - 1)
                    res += ",";
            }
            return res;
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
    }

}
