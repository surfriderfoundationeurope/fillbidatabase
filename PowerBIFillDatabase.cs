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
        public static async Task Run([TimerTrigger("0 0 * * * *")]TimerInfo myTimer, ILogger log)
        {

            // TODO 
            // DONE - faire la bonne query sql pour update
            // DONE - tester en local
            // DONE - upload avec timer 1x/h
            // DONE - rajouter des logs
            // DONE - save le code sur github
            // faire les différents calculs
            
            var startedOn = DateTime.Now;
            var newCampaigns = await RetrieveNewCampaigns(log);
            var status = await InsertNewCampaignsInBI(newCampaigns, log);
            if(newCampaigns.Count > 0) await InsertLog(startedOn, status, log);
        }

        private static async Task<OperationStatus> InsertNewCampaignsInBI(IList<Guid> newCampaigns, ILogger log)
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
            return OperationStatus.OK; // for now, let assume that everything went well
        }

        private static async Task InsertLog(DateTime startedOn, OperationStatus status, ILogger log)
        {
            var finishedOn = DateTime.Now;
            var elapsedTime = finishedOn - startedOn;
            // see https://stackoverflow.com/a/23163325/12805412 
            var command = $"INSERT INTO bi.Logs VALUES (@id, @startedOn, @finishedOn, @elapsedTime, @status)";
            using (SqlConnection conn = new SqlConnection(ConnectionString))
            {
                conn.Open();
                using (SqlCommand cmd = new SqlCommand(command, conn))
                {
                    cmd.Parameters.AddWithValue("@id", Guid.NewGuid());
                    cmd.Parameters.AddWithValue("@startedOn", startedOn);
                    cmd.Parameters.AddWithValue("@finishedOn", finishedOn);
                    cmd.Parameters.AddWithValue("@elapsedTime", elapsedTime.TotalSeconds);
                    cmd.Parameters.AddWithValue("@status", status.ToString());
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
