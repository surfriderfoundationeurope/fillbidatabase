using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
using System.Collections.Generic;
using Npgsql;

namespace Surfrider.Jobs.Recurring
{
    public static class PowerBIFillDatabase
    {
        public static IDatabase Database;
        private static string ListOfCampaignsIds;
        [FunctionName("PowerBIFillDatabase")]
        public static async Task Run([TimerTrigger("0 0 2 * * *")]TimerInfo myTimer, ILogger logger)// runs everyd ay at 02:00
        {
            Console.WriteLine("USING " + Helper.GetConnectionString());
            Database = new PostgreDatabase(Helper.GetConnectionString());
            // TODO
            // * DROP les campaign pour lesquels on a une erreur de calcul quelque part
            // ** ComputeMetricsOnCampaignRiver()
            // ** ComputeTrajectoryPointRiver()
            // * Log les campaign pour lesquelles on a des erreur de calcul quelque part

            var startedOn = DateTime.Now;

            //    IList<Guid> newCampaignsIds = await RetrieveNewCampaigns(logger);
            // ********************************* LOCAL TEST ONLY ***********************
            IList<Guid> newCampaignsIds = new List<Guid>();
            newCampaignsIds.Add(new Guid("d115922a-3ca9-49f7-b363-06c9383b6563"));
            newCampaignsIds.Add(new Guid("2155da04-c2bb-433b-9a90-8ec8b8d74ee9"));
            // *************************************************************************
            ListOfCampaignsIds = FormatGuidsForSQL(newCampaignsIds);

            await ExecuteScript(@"./SqlScripts/2_update_campaign_trajectory_point.sql");
            await ExecuteScript(@"./SqlScripts/3_insert_bi_campaign.sql");//inserts new campaigns into BI db schema
            await ExecuteScript(@"./SqlScripts/4_insert_bi_campaign_distance_to_sea.sql");
            await ExecuteScript(@"./SqlScripts/5_insert_bi_trajectory_point_river.sql"); //Pour chaque Trash on récupère la rivière associée et on projete la geometry du trash sur la rivière
            await ExecuteScript(@"./SqlScripts/6_insert_bi_campaign_river.sql");
            await ExecuteScript(@"./SqlScripts/7_get_bi_rivers_id.sql");
            await ExecuteScript(@"./SqlScripts/8_update_bi_trash.sql");
            await ExecuteScript(@"./SqlScripts/9_insert_bi_trash_river.sql");
            await ExecuteScript(@"./SqlScripts/10_update_bi_river.sql");

            // await CleanErrors(); // on vient clean toutes les campagnes pour lesquelles on a eu un probleme de calcul à un moment

            // var status = await InsertNewCampaignsInBI(newCampaignsIds, logger);
            // if (newCampaignsIds.Count > 0) await InsertLog(startedOn, status, logger);

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
            var command = System.IO.File.ReadAllText(scriptPath);
            command = command.Replace("@campaign_ids", ListOfCampaignsIds);
            await Database.ExecuteNonQuery(command);
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
