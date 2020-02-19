using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using System.Data.SqlClient;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace Surfrider.Jobs.Recurring
{
    public static class PowerBIFillDatabase
    {
        public static string ConnectionString = Environment.GetEnvironmentVariable("sqldb_connection");
        [FunctionName("PowerBIFillDatabase")]
        public static async Task Run([TimerTrigger("*/15 * * * * *")]TimerInfo myTimer, ILogger log)
        {

            // TODO 
            // DONE - faire la bonne query sql pour update
            // DONE - tester en local
            // upload avec timer 1x/h
            // rajouter des logs
            // save le code sur github
            // faire les différents calculs
            
            var startedOn = DateTime.Now;
            var newCampaigns = await RetrieveNewCampaigns(log);
            await InsertNewCampaignsInBI(newCampaigns, log);
            await InsertLog(startedOn, "OK", log);
        }

        private static async Task InsertNewCampaignsInBI(IList<Guid> newCampaigns, ILogger log)
        {
            using (SqlConnection conn = new SqlConnection(ConnectionString))
            {
                conn.Open();
                foreach(var campaignId in newCampaigns){
                    var command = $"INSERT INTO bi.Campaign (Id) VALUES ('{campaignId}')";
                    using (SqlCommand cmd = new SqlCommand(command, conn))
                    {
                        await cmd.ExecuteNonQueryAsync();
                        log.LogInformation("Inserted new campaign with Id = " + campaignId);
                    }

                }
            }
        }

        private static async Task InsertLog(DateTime startedOn, string status, ILogger log)
        {
            var finishedOn = DateTime.Now;
            var elapsedTime = finishedOn - startedOn;
            var command = $"INSERT INTO bi.Logs VALUES ('{Guid.NewGuid()}','{startedOn}', '{finishedOn}', {elapsedTime}, '{status}')";
            using (SqlConnection conn = new SqlConnection(ConnectionString))
            {
                conn.Open();
                using (SqlCommand cmd = new SqlCommand(command, conn))
                {
                    await cmd.ExecuteNonQueryAsync();
                    log.LogInformation("Inserted logs with status = " + status);
                }
            }
        }

        private static async Task<IList<Guid>> RetrieveNewCampaigns(ILogger log)
        {
            IList<Guid> campaigns = new List<Guid>();
            var campaignToInsertCommand = "SELECT cam.Id FROM dbo.Campaign cam LEFT JOIN bi.Campaign cbi ON cam.Id = cbi.Id WHERE cbi.Id IS NULL";
            using (SqlConnection conn = new SqlConnection(ConnectionString))
            {
                conn.Open();
                using (SqlCommand cmd = new SqlCommand(campaignToInsertCommand, conn))
                {
                    using (SqlDataReader reader = await cmd.ExecuteReaderAsync())
                    {
                        while (reader.Read())
                        {
                            campaigns.Add(reader.GetGuid(0));
                            log.LogInformation($"{reader.GetGuid(0).ToString()} id retrieved");
                        }
                    }
                }
            }
            return campaigns;
        }
    }
}
