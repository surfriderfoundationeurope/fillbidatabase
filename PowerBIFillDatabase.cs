using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System.Data.SqlClient;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;

namespace Surfrider.Jobs.Recurring
{
    public static class PowerBIFillDatabase
    {
        public static string ConnectionString = Environment.GetEnvironmentVariable("sqldb_connection");
        [FunctionName("PowerBIFillDatabase")]
        public static async Task Run([TimerTrigger("0 0 * * * *")]TimerInfo myTimer, ILogger logger)
        {

            // TODO
            // * DROP les campaign pour lesquels on a une erreur de calcul quelque part
            // ** ComputeMetricsOnCampaignRiver()
            // ** ComputeTrajectoryPointRiver()
            // * Log les campaign pour lesquelles on a des erreur de calcul quelque part

            var startedOn = DateTime.Now;



            IDictionary<Guid, string> newCampaignsIds = await RetrieveNewCampaigns(logger);
            await CleanKayakRawData(logger, newCampaignsIds); // cleans real kayak traces

            await InsertNewCampaignsInBiSchema(newCampaignsIds);//inserts new campaigns into BI db schema

            await ProjectTrashOnClosestRiver(newCampaignsIds); // projects trash point to closest river point

            await ComputeTrajectoryPointRiver(newCampaignsIds);

            await ComputeMetricsOnCampaignRiver(newCampaignsIds);

            await CleanErrors(); // on vient clean toutes les campagnes pour lesquelles on a eu un probleme de calcul à un moment

            var status = await InsertNewCampaignsInBI(newCampaignsIds, logger);
            if (newCampaignsIds.Count > 0) await InsertLog(startedOn, status, logger);


        }

        private static Task CleanErrors()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// recup des traces GPS du kayak (trajectory_point)
        ///     On vient calculer la distance, la vitesse, le temps entre deux trajectory_point consécutifs
        ///      => calculer la distance réelle parcouru par le kayak pendant la campagne
        ///      => estimer la distance parcouru sur le référentiel rivière
        /// ==> Cleaning des données réelles
        /// fais un update sur les trajectory_point du schéma campagne
        /// </summary>
        /// <param name="logger"></param>
        /// <param name="newCampaignsIds"></param>
        /// <returns></returns>
        private static async Task CleanKayakRawData(ILogger logger, IDictionary<Guid, string> newCampaignsIds)
        {
            // ************************************ CLEMENT
            var command = "SELECT * FROM campaign.campaign;";
            // ************************************
            using (SqlConnection conn = new SqlConnection(ConnectionString))
            {
                conn.Open();
                using (SqlCommand cmd = new SqlCommand(command, conn))
                {
                    cmd.Parameters.AddWithValue("@campaign_id", "1234");
                    await cmd.ExecuteNonQueryAsync();
                }
            }
        }

        /// <summary>
        /// Pour chaque riviere, on calcule la taille de cette riviere, le nb de trash sur cette riviere, la distance parcourue sur cette riviere
        /// </summary>
        /// <param name="newCampaignsIds"></param>
        /// <returns></returns>
        private static async Task ComputeMetricsOnCampaignRiver(IDictionary<Guid, string> newCampaignsIds)
        {
             // ************************************ CLEMENT
            var command = "SELECT * FROM campaign.campaign;";
            // ************************************
            using (SqlConnection conn = new SqlConnection(ConnectionString))
            {
                conn.Open();
                using (SqlCommand cmd = new SqlCommand(command, conn))
                {
                    cmd.Parameters.AddWithValue("@campaign_id", "1234");
                    await cmd.ExecuteNonQueryAsync();
                }
            }
        }

        private static async Task ComputeTrajectoryPointRiver(IDictionary<Guid, string> newCampaignsIds)
        {
            // ************************************ CLEMENT
            var command = "SELECT * FROM campaign.campaign;";
            // ************************************
            using (SqlConnection conn = new SqlConnection(ConnectionString))
            {
                conn.Open();
                using (SqlCommand cmd = new SqlCommand(command, conn))
                {
                    cmd.Parameters.AddWithValue("@campaign_id", "1234");
                    await cmd.ExecuteNonQueryAsync();
                }
            }
        }

        /// <summary>
        /// Pour chaque Trash on récupère la rivière associée et on projete la geometry du trash sur la rivière
        /// </summary>
        /// <param name="newCampaignsIds"></param>
        /// <returns></returns>
        private static async Task ProjectTrashOnClosestRiver(IDictionary<Guid, string> newCampaignsIds)
        {
                // ************************************ CLEMENT
                var command = "SELECT * FROM campaign.campaign;";
                // ************************************
                using (SqlConnection conn = new SqlConnection(ConnectionString))
                {
                    conn.Open();
                    using (SqlCommand cmd = new SqlCommand(command, conn))
                    {
                        cmd.Parameters.AddWithValue("@campaign_ids", newCampaignsIds.First().Value);
                        await cmd.ExecuteNonQueryAsync();
                    }
                }
        }

        /// <summary>
        /// 
        /// Création des nouvelles campagnes dans le schéma BI, avec les données intégrées plus d'autres indicateurs : 
        /// distance totale parcouru,
        /// point de départ
        /// point d'arrivée,
        /// durée totale de la campagne,
        /// nb de trash détectés sur la campagne
        /// </summary>
        /// <param name="newCampaignsIds"></param>
        /// <returns></returns>
        private static async Task InsertNewCampaignsInBiSchema(IDictionary<Guid, string> newCampaignsIds)
        {
             var command = "SELECT * FROM campaign.campaign;";
                // ************************************
                using (SqlConnection conn = new SqlConnection(ConnectionString))
                {
                    conn.Open();
                    using (SqlCommand cmd = new SqlCommand(command, conn))
                    {
                        cmd.Parameters.AddWithValue("@campaign_ids", newCampaignsIds.First().Value);
                        await cmd.ExecuteNonQueryAsync();
                    }
                }

            await ComputeDistanceToSea(newCampaignsIds);
        }

        private static async Task ComputeDistanceToSea(IDictionary<Guid, string> newCampaignsIds)
        {
            var command = "SELECT * FROM campaign.campaign;";
                // ************************************
                using (SqlConnection conn = new SqlConnection(ConnectionString))
                {
                    conn.Open();
                    using (SqlCommand cmd = new SqlCommand(command, conn))
                    {
                        cmd.Parameters.AddWithValue("@campaign_ids", newCampaignsIds.First().Value);
                        await cmd.ExecuteNonQueryAsync();
                    }
                }
        }

        private static async Task<OperationStatus> InsertNewCampaignsInBI(IDictionary<Guid, string> newCampaigns, ILogger log)
        {
            using (SqlConnection conn = new SqlConnection(ConnectionString))
            {
                conn.Open();
                foreach (var campaignId in newCampaigns)
                {
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

        private static async Task<IDictionary<Guid, string>> RetrieveNewCampaigns(ILogger log)
        {
                
            IDictionary<Guid, string> campaigns = new Dictionary<Guid, string>();
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
                            campaigns.Add(reader.GetGuid(0), "river_name");
                            log.LogInformation($"{reader.GetGuid(0).ToString()} id retrieved");
                        }
                    }
                }
            }
            return campaigns;
        }
    }
}
