using System;
using System.Linq;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
using System.Collections.Generic;
using Npgsql;
using System.IO;
using Azure.Storage.Blobs;
using Microsoft.Azure.WebJobs.Extensions.Http;

namespace Surfrider.Jobs
{
    public static class PowerBIFillDatabase
    {
        public static string ScriptVersion = "V0";
        private static bool runNextScript;
        private static Dictionary<string, string> dnewCampaign;

        [FunctionName("PowerBIFillDatabase")]
        //To Test open this adress in navigator http://localhost:7071/api/PowerBIFillDatabase
        //public static async Task Run([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] Microsoft.AspNetCore.Http.HttpRequest req,  ILogger logger)
        
        //Production  runs everyd ay at 02:00 
        public static async Task Run([TimerTrigger("0 0 2 * * *")] TimerInfo myTimer, ILogger logger)
        {

            Console.WriteLine("USING " + Helper.GetConnectionString());
            // Database = new PostgreDatabase(Helper.GetConnectionString());
            // IDataFileWriter fileWriter = new DataFileWriter();
            // await fileWriter.UpdateJsonFileWithDataAsync(55, 66, 77);



            await RunScriptAsync(0, @"./SqlScripts/init_insert_new_campaigns_id.sql");


            // 0. RETRIEVE NEW CAMPAIGNS
            //script init_get_new_campaigns_id.sql
            IList<Campaign> newCampaigns = await RetrieveNewCampaigns(logger);


            foreach (var newCampaign in newCampaigns)
            {
                runNextScript = true;
                var dnewCampaign = FillQueryPrameters(newCampaign);

                Console.WriteLine("Compaign ID : " + newCampaign.CompaignId);
                Console.WriteLine("Pipline ID : " + newCampaign.PipelineId);

                if (runNextScript)
                    await RunScriptAsync(0, @"./SqlScripts/0_insert_main_tables.sql", dnewCampaign);
                if (runNextScript)
                    await RunScriptAsync(1, @"./SqlScripts/1_update_bi_temp_trajectory_point.sql", dnewCampaign);
                if (runNextScript)
                    await RunScriptAsync(2, @"./SqlScripts/2_update_bi_temp_campaign.sql", dnewCampaign);
                if (runNextScript)
                    await RunScriptAsync(3, @"./SqlScripts/3_test_campaign.sql", dnewCampaign); //               OK
                if (runNextScript)
                    await RunScriptAsync(4, @"./SqlScripts/4_insert_bi_temp_trajectory_point_river.sql", dnewCampaign);
                if (runNextScript)
                    await RunScriptAsync(5, @"./SqlScripts/5_insert_bi_temp_campaign_river.sql", dnewCampaign);   // OK
                if (runNextScript)
                    await RunScriptAsync(6, @"./SqlScripts/6_update_bi_temp_trash.sql", dnewCampaign); //OK
                if (runNextScript)
                    await RunScriptAsync(7, @"./SqlScripts/7_insert_bi_temp_trash_river.sql", dnewCampaign);  //OK


                // SELECT OLD CAMPAIGNS
                if (runNextScript)
                {
                    var oldCampaignsIds = await RunSelectScriptAsync(8, @"./SqlScripts/8_get_old_campaign_id.sql", dnewCampaign);
                    foreach (var oldCampaignId in oldCampaignsIds)
                    {
                        Dictionary<string, string> doldCompaigns = new Dictionary<string, string>();
                        doldCompaigns.Add("oldCampaignId", oldCampaignId);
                        if (runNextScript)
                            await RunScriptAsync(10, @"./SqlScripts/10_import_bi_table_to_bi_temp.sql", doldCompaigns);
                    }
                }

                if (runNextScript)
                {
                    // SELECT RIVERS NAME
                    var riversNames = await RunSelectScriptAsync(9, @"./SqlScripts/9_get_river_name.sql", dnewCampaign);
                    // COMPUTE ON RIVERS
                    foreach (var riverName in riversNames)
                    {
                        Dictionary<string, string> driversNames = new Dictionary<string, string>();
                        driversNames.Add("riverName", riverName);
                        if(runNextScript)
                            await RunScriptAsync(11, @"./SqlScripts/11_update_bi_river.sql", driversNames);
                    }
                }

                //COMMIT PROD DATA
                if (runNextScript)
                    await RunScriptAsync(12, @"./SqlScripts/12_import_bi_temp_table_to_bi.sql", dnewCampaign);
                if (runNextScript)
                    await RunScriptAsync(13, @"./SqlScripts/13_delete_from_bi_temp_table.sql", dnewCampaign);


                // LOG SCRIPT'S EXECUTION RESULT
                if (runNextScript)
                {
                    dnewCampaign.Add("finishedOn", DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss"));
                    await RunScriptAsync(14, @"./SqlScripts/logs_status_pipeline.sql", dnewCampaign);
                }

            }

            Console.WriteLine("-------------------- ALL DONE ---------------------");

        }


        private static Dictionary<string, string> FillQueryPrameters(Campaign newCampaign)
        {

            dnewCampaign = new Dictionary<string, string>();
            dnewCampaign.Add("campaignID", newCampaign.CompaignId.ToString());
            dnewCampaign.Add("pipelineID", newCampaign.PipelineId.ToString());
            dnewCampaign.Add("startDate", DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss"));
            dnewCampaign.Add("status", "SUCCESS");
            dnewCampaign.Add("reason", "");
            dnewCampaign.Add("scriptVersion", ScriptVersion);
            dnewCampaign.Add("failedStep", "");
            dnewCampaign.Add("campaign_has_been_computed", "true");
            dnewCampaign.Add("river_has_been_computed", "true");

            return dnewCampaign;
        }

        private static async Task<IList<Campaign>> RetrieveNewCampaigns(ILogger log)
        {
            IList<Campaign> campaigns = new List<Campaign>();

            //var command = $"SELECT id FROM campaign.campaign WHERE createdon >=  '{current_ts}'";

            var command = @"SELECT id AS pipeline_id, campaign_id
            FROM bi_temp.pipelines
            WHERE campaign_has_been_computed IS NULL
                 AND river_has_been_computed IS null";



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
                            campaigns.Add(new Campaign( reader.GetFieldValue<Guid>(0), reader.GetFieldValue<Guid>(1)));
                        }
                    }
                }
            }

            return campaigns;
        }

        private static async Task <int> RunScriptAsync(int step, string scriptPath, Dictionary<string, string> parms = null)
        {

            Console.WriteLine("Running script {0}  ...", scriptPath);

            string command = string.Empty;

            try
            {
                command = System.IO.File.ReadAllText(scriptPath);
            }
            catch (Exception e)
            {
                Console.WriteLine($"-------------- ERROR READING SQL FILE {scriptPath} : " + e.Message);
            }
            if (command != string.Empty)
            {
                try
                {
                    if (parms != null)
                    {
                        foreach (var parm in parms)
                        {
                            if(parm.Value.Contains("'"))
                                command = command.Replace(new String("@" + parm.Key), "\"" + parm.Value + "\"");
                            else
                                command = command.Replace(new String("@" + parm.Key),  "'" +parm.Value + "'");
                        }
                    }

                    string query = command;

                    using (var conn = new NpgsqlConnection(Helper.GetConnectionString()))
                    {
                        conn.Open();
                        using (var cmd = new NpgsqlCommand())
                        {
                            cmd.Connection = conn;
                            cmd.CommandText = query;

                            return await cmd.ExecuteNonQueryAsync();
                        }
                    }
                }
                catch (Exception e)
                {
     
                     Console.WriteLine($"-------------- ERROR DURING SQL FILE EXECUTION {scriptPath}\n" + e.Message);
                     await UpdateBILogTable(e.Message, step);
                }
            }
            return 0;
        }

        static async Task<int> UpdateBILogTable(string errormesssage, int step)
        {
            runNextScript = false;

            dnewCampaign["reason"] = errormesssage;
            dnewCampaign["status"] = "FAILED";
            dnewCampaign["failedStep"] = step.ToString();
            dnewCampaign["finishedOn"] = DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss");


            if (step < 7)
            {
                dnewCampaign["campaign_has_been_computed"] = "false";
                dnewCampaign["river_has_been_computed"] = "false";
            }
            if (step > 7 & step < 13)
            {
                dnewCampaign["campaign_has_been_computed"] = "true";
                dnewCampaign["river_has_been_computed"] = "false";
            }

            Console.WriteLine("Running script log script  ...");

            string command = string.Empty;

            try
            {
                command = System.IO.File.ReadAllText(@"./SqlScripts/logs_status_pipeline.sql");
            }
            catch (Exception e)
            {
                Console.WriteLine($"-------------- ERROR READING SCRIPT LOG : " + e.Message);
            }
            if (command != string.Empty)
            {
                try
                {
                    if (dnewCampaign != null)
                    {
                        foreach (var parm in dnewCampaign)
                        {
                            command = command.Replace(new String("@" + parm.Key), "'" + parm.Value.Replace("'",string.Empty) + "'");
                        }
                    }

                    string query = command;

                    using (var conn = new NpgsqlConnection(Helper.GetConnectionString()))
                    {
                        conn.Open();
                        using (var cmd = new NpgsqlCommand())
                        {
                            cmd.Connection = conn;
                            cmd.CommandText = query;

                            return await cmd.ExecuteNonQueryAsync();
                        }
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine($"-------------- ERROR DURING LOG SQL EXECUTION : " + e.Message);
                }
            }

            return 0;
        }

        private static async Task<IList<String>> RunSelectScriptAsync(int step, string scriptPath, Dictionary<string, string> parms = null)
        {

            Console.WriteLine("Running script {0}  ...", scriptPath);


            IList<String> results = new List<String>();

            string command = string.Empty;
            try
            {
                command = System.IO.File.ReadAllText(scriptPath);
            }
            catch (Exception e)
            {
                Console.WriteLine($"-------------- ERROR READING SQL FILE {scriptPath} " + e.Message);

            }
            if (command != string.Empty)
            {
                try
                {
                    if (parms != null)
                    {
                        foreach (var parm in parms)
                        {
                            if (parm.Value.Contains("'"))
                                command = command.Replace(new String("@" + parm.Key), "\"" + parm.Value + "\"");
                            else
                                command = command.Replace(new String("@" + parm.Key), "'" + parm.Value + "'");
                        }
                    }

                    string query = command;

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
                                    results.Add(reader.GetFieldValue<String>(0));
                                }
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                     Console.WriteLine($"-------------- ERROR DURING SQL FILE EXECUTION {scriptPath}\n" + e.Message);
                     await UpdateBILogTable(e.Message, step);
                }
            }
            return results ;
        }
    }
}
