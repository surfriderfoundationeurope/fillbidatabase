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

            // faire les différents calculs
            
            var startedOn = DateTime.Now;

            // recup des traces GPS du kayak (trajectory_point)
            //     On vient calculer la distance, la vitesse, le temps entre deux trajectory_point consécutifs
            //      => calculer la distance réelle parcouru par le kayak pendant la campagne
            //      => estimer la distance parcouru sur le référentiel rivière
            // ==> Cleaning des données réelles


            IList<Guid> newCampaignsIds = await RetrieveNewCampaigns(log);
            CleanKayakRawData(newCampaignsIds); // fais un update sur les trajectory_point du schéma campagne
            
            
            // recup des nouvelles campagnes avec les données intégrées plus d'autres indicateurs : 
            // distance totale parcouru,
            // point de départ
            // point d'arrivée,
            // durée totale de la campagne,
            // nb de trash détectés sur la campagne
            await ComputeOnNewCampaigns(newCampaignsIds);// insere les nouvelles campagnes dans schema BI

            ComputeTrashRiver(newCampaignsIds); //Pour chaque Trash on récupère la rivière associée et on projete la geometry du trash sur la rivière

            ComputeTrajectoryPointRiver(newCampaignsIds);

            ComputeCampaignRiver(newCampaignsIds);// Pour chaque riviere, on calcule la taille de cette riviere, le nb de trash sur cette riviere, la distance parcourue sur cette riviere


            CleanErrors(erroredCampaigns); // on vient clean toutes les campagnes pour lesquelles on a eu un probleme de calcul à un moment



            var status = await InsertNewCampaignsInBI(newCampaigns, log);
            if(newCampaignsIds.Count > 0) await InsertLog(startedOn, status, log);


        }

        private static void ComputeCampaignRiver(IList<Guid> newCampaignsIds)
        {
             foreach(var newCampaignId in newCampaignsIds){
                // ************************************ CLEMENT
                var riverName = "";// une SQL command pour venir chercher le nom de la riviere
                var resultCode = ExecuteSqlCommand(superCommandeDeClement.sql, riverName);
            // ************************************

                if(resultCode == Error){
                    // DROP de la ligne dans la table Bi Campaign <<<< CLEMENT
                    // DROP également 
                }
            }
        }

        private static void ComputeTrajectoryPointRiver(IList<Guid> newCampaignsIds)
        {
            foreach(var newCampaignId in newCampaignsIds){
                // ************************************ CLEMENT
                var resultCode = ExecuteSqlCommand(ComputeTrajectoryPoint.sql, newCampaignIds);
            // ************************************

                if(resultCode == Error){
                    // DROP de la ligne dans la table Bi Campaign <<<< CLEMENT
                    // DROP également 
                }
            }
        }

        private static void ComputeTrashRiver(IList<Guid> newCampaignsIds)
        {
            foreach(var newCampaignId in newCampaignsIds){
                // ************************************ CLEMENT
                var resultCode = ExecuteSqlCommand(ComputeTrashRiver.sql, newCampaignIds);
            // ************************************

                if(resultCode == Error){
                    // DROP de la ligne dans la table Bi Campaign <<<< CLEMENT
                }
            }

            // return liste des campagnes 
        }

        private static Task ComputeOnNewCampaigns(IList<Guid> newCampaignsIds)
        {
            // boucle sur l'ensemble des nouvelles campaign
            foreach(var campaignId in newCampaignsIds){
                var sqlCommand = "INSERT INTO bi.campaign (id_ref_campaign_fk, start_date, end_date, duration, start_point, end_point, distance_start_end, total_distance, avg_speed, trash_count)
                var resultCode = executeSqlCommand();
                if(resultCode == ERROR){
                    // OSEF
                }
            }


            ComputeDistanceToSea(newCampaignsIds);

            // on peut logger que tout va bien
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
