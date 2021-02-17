using System;
using System.Linq;
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
            IDictionary<Guid, string> RiversToComputeOn = await SelectRiversAsync(newCampaignsIds);
            // 3. COMPUTE ON RIVERS
            await ComputeOnRiversAsync(RiversToComputeOn);
            // 4. COMMIT PROD DATA
            await CommitProductionDataAsync(newCampaignsIds);

            Console.WriteLine("-------------------- ALL DONE ---------------------");

        }

        // Task 101
        private static async Task CommitProductionDataAsync(IList<Guid> newCampaignsIds)
        {
            throw new NotImplementedException();
              // si dans le pipeline, la cmapaign et la river ont été bien computed, alors on copie les données pour ces campaign là                                                
        }

        private static async Task ComputeOnRiversAsync(IDictionary<Guid, string> RiversToComputeOn)
        {
            IRiverPipeline RiverPipeline = new RiverPipeline(Helper.GetConnectionString());
            foreach (KeyValuePair<Guid, string> RiverToComputeOn in RiversToComputeOn){
                if(await RiverPipeline.ComputePipelineOnSingleRiverAsync(RiverToComputeOn.Value))
                    await RiverPipeline.MarkRiverPipelineAsSuccessedAsync(RiverToComputeOn.Key);
                else
                    await RiverPipeline.MarkRiverPipelineAsFailedAsync(RiverToComputeOn.Key);
            }
        }

        // returns a dictionary of {campaignId; riverName}
        private static async Task<IDictionary<Guid, string>> SelectRiversAsync(IList<Guid> newCampaignsIds)
        {
            IRiverPipeline riverPipeline = new RiverPipeline(Helper.GetConnectionString());
            // 1. On recupere les "id" des rivieres des nouvelles campagnes
            IDictionary<string, Guid> riversIdsFromNewCampaigns = await riverPipeline.RetrieveRiversFromSuccessfullyComputedCampaignsAsync(newCampaignsIds);
            // 2. On recupere les campaignId des anciennes campaign qui sont concernées par les rivieres des nouvelles campagnes
            IDictionary<string, Guid> riversIdsFromOldCampaigns = await riverPipeline.GetOldCampaignsFromRivers(riversIdsFromNewCampaigns.Select(x => x.Key).ToList<string>());

            // now I have two dictionnaries : one associates new campaigns and river, one associates old campaigns and river
            IDictionary<string, Guid> allCampaignsAndRivers = riverPipeline.MergeCampaignRiverDictionnaries(riversIdsFromNewCampaigns, riversIdsFromOldCampaigns);
            
            return new Dictionary<Guid, string>();
        }

        private static async Task ComputeOnCampaignsAsync(IList<Guid> newCampaignsIds)
        {
            var startedOn = DateTime.UtcNow;
            ICampaignPipeline CampaignPipeline = new CampaignPipeline(Helper.GetConnectionString());
            // TODO boucle à optimiser ( /!\ perf )
            foreach(var newCampaignId in newCampaignsIds){
                if (await CampaignPipeline.ComputeOnSingleCampaignAsync(newCampaignId, GetStepsToExecute()))
                    await CampaignPipeline.MarkCampaignPipelineAsFailedAsync(newCampaignId);
                else
                    await CampaignPipeline.MarkCampaignPipelineAsSuccessedAsync(newCampaignId);
            }
        }
        private static SortedList<int, string> GetStepsToExecute(){
            SortedList<int, string> SqlSteps = new SortedList<int, string>();
            SqlSteps.Add(1, @"./SqlScripts/1_update_bi_trajectory_point.sql");
            SqlSteps.Add(2, @"./SqlScripts/2_insert_bi_campaign.sql");
            SqlSteps.Add(3, @"./SqlScripts/4_insert_bi_campaign_distance_to_sea.sql");
            SqlSteps.Add(4, @"./SqlScripts/7_update_bi_trash.sql");
            return SqlSteps;
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
            IDictionary<string, string> args = new Dictionary<string, string>();
            args.Add("@id", Guid.NewGuid().ToString());
            args.Add("@startedOn", startedOn.ToString());
            args.Add("@finishedOn", finishedOn.ToString());
            args.Add("@elapsedTime", elapsedTime.TotalSeconds.ToString());
            args.Add("@status", status.ToString());
            await Database.ExecuteNonQueryAsync(command, args);
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
