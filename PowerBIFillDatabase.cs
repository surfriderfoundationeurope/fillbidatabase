using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System.Data.SqlClient;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using System.Data.Common;
using System.Collections;
using Npgsql;

namespace Surfrider.Jobs.Recurring
{
    public static class PowerBIFillDatabase
    {
        public static IDatabase Database;
        [FunctionName("PowerBIFillDatabase")]
        public static async Task Run([TimerTrigger("0/10 * * * * *")]TimerInfo myTimer, ILogger logger)// runs everyd ay at 00:00
        {
            Database = new PostgreDatabase(Helper.GetConnectionString());
            Console.WriteLine("USING " + Helper.GetConnectionString());
            // TODO
            // * DROP les campaign pour lesquels on a une erreur de calcul quelque part
            // ** ComputeMetricsOnCampaignRiver()
            // ** ComputeTrajectoryPointRiver()
            // * Log les campaign pour lesquelles on a des erreur de calcul quelque part

            var startedOn = DateTime.Now;

            IList<Guid> newCampaignsIds = await RetrieveNewCampaigns(logger);
            var requestGuids = FormatGuidsForSQL(newCampaignsIds);

            await CleanKayakRawData(logger, requestGuids); // cleans real kayak traces

            await InsertNewCampaignsInBiSchema(newCampaignsIds);//inserts new campaigns into BI db schema

            await ProjectTrashOnClosestRiver(newCampaignsIds); // projects trash point to closest river point

            await ComputeTrajectoryPointRiver(newCampaignsIds);

            await ComputeMetricsOnCampaignRiver(newCampaignsIds);

            await CleanErrors(); // on vient clean toutes les campagnes pour lesquelles on a eu un probleme de calcul à un moment

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
        private static async Task CleanKayakRawData(ILogger logger, string newCampaignsIds)
        {
            // ************************************ CLEMENT
            var command = $"DO $$ DECLARE campaign_ids uuid[] := ARRAY[{newCampaignsIds}];";
            command += " BEGIN UPDATE bi.trajectory_point";
            command += " SET speed = (distance/EXTRACT(epoch FROM time_diff))*3.6";
            command += " WHERE speed IS NULL";
            command += " AND EXTRACT(epoch FROM time_diff) > 0";
            command += " AND distance > 0";
            command += " AND id_ref_campaign_fk IN (SELECT UNNEST(campaign_ids));END $$;";

            // ************************************
            IDictionary<string, object> args = new Dictionary<string, object>();
            await Database.ExecuteNonQuery(command, args);
        }

        /// <summary>
        /// Pour chaque riviere, on calcule la taille de cette riviere, le nb de trash sur cette riviere, la distance parcourue sur cette riviere
        /// </summary>
        /// <param name="newCampaignsIds"></param>
        /// <returns></returns>
        private static async Task ComputeMetricsOnCampaignRiver(IList<Guid> newCampaignsIds)
        {
            // ************************************ CLEMENT
            var command = "SELECT * FROM campaign.campaign;";
            // ************************************
            IDictionary<string, object> args = new Dictionary<string, object>();
            args.Add("@campaign_id", "1234");
            await Database.ExecuteNonQuery(command, args);
        }

        private static async Task ComputeTrajectoryPointRiver(IList<Guid> newCampaignsIds)
        {
            // ************************************ CLEMENT
            var command = "SELECT * FROM campaign.campaign;";
            // ************************************
            IDictionary<string, object> args = new Dictionary<string, object>();
            args.Add("@campaign_id", "1234");
            await Database.ExecuteNonQuery(command, args);
        }

        /// <summary>
        /// Pour chaque Trash on récupère la rivière associée et on projete la geometry du trash sur la rivière
        /// </summary>
        /// <param name="newCampaignsIds"></param>
        /// <returns></returns>
        private static async Task ProjectTrashOnClosestRiver(IList<Guid> newCampaignsIds)
        {
            // ************************************ CLEMENT
            var command = "SELECT * FROM campaign.campaign;";
            // ************************************
            IDictionary<string, object> args = new Dictionary<string, object>();
            args.Add("@campaign_id", "1234");
            await Database.ExecuteNonQuery(command, args);
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
        private static async Task InsertNewCampaignsInBiSchema(IList<Guid> newCampaignsIds)
        {
            // ************************************ CLEMENT
            var command = "SELECT * FROM campaign.campaign;";
            // ************************************
            IDictionary<string, object> args = new Dictionary<string, object>();
            args.Add("@campaign_id", "1234");
            await Database.ExecuteNonQuery(command, args);

            await ComputeDistanceToSea(newCampaignsIds);
        }

        private static async Task ComputeDistanceToSea(IList<Guid> newCampaignsIds)
        {
            // ************************************ CLEMENT
            var command = "SELECT * FROM campaign.campaign;";
            // ************************************
            IDictionary<string, object> args = new Dictionary<string, object>();
            args.Add("@campaign_id", "1234");
            await Database.ExecuteNonQuery(command, args);
        }

        private static async Task<OperationStatus> InsertNewCampaignsInBI(IList<Guid> newCampaigns, ILogger log)
        {
            // ************************************ CLEMENT
            var command = "SELECT * FROM campaign.campaign;";
            // ************************************
            IDictionary<string, object> args = new Dictionary<string, object>();
            args.Add("@campaign_id", "1234");
            await Database.ExecuteNonQuery(command, args);
            return OperationStatus.OK; // for now, let assume that everything went well
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
            // ************************************
            // IDictionary<string, object> args = new Dictionary<string, object>();
            // args.Add("@current_ts", new DateTime(2020, 05, 04));
            // SQL PART
            string res = string.Empty;
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
