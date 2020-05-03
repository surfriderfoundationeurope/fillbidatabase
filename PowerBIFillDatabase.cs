using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System.Data.SqlClient;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using System.Data.Common;
using System.Collections;

namespace Surfrider.Jobs.Recurring
{
    public static class PowerBIFillDatabase
    {
        public static IDatabase Database;
        [FunctionName("PowerBIFillDatabase")]
        public static async Task Run([TimerTrigger("*/20 * * * * *")]TimerInfo myTimer, ILogger logger)
        {
            Database = new PostgreDatabase(Helper.GetConnectionString());
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

            // var status = await InsertNewCampaignsInBI(newCampaignsIds, logger);
            // if (newCampaignsIds.Count > 0) await InsertLog(startedOn, status, logger);

            Console.WriteLine("-------------------- ALL DONE ---------------------");

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
        private static async Task CleanKayakRawData(ILogger logger, IDictionary<Guid, string> newCampaignsIds)
        {
            // ************************************ CLEMENT
            var command = "SELECT * FROM campaign.campaign;";
            // ************************************
            IDictionary<string, object> args = new Dictionary<string, object>();
            args.Add("@campaign_id", "1234");
            await Database.ExecuteNonQuery(command, args);
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
            IDictionary<string, object> args = new Dictionary<string, object>();
            args.Add("@campaign_id", "1234");
            await Database.ExecuteNonQuery(command, args);
        }

        private static async Task ComputeTrajectoryPointRiver(IDictionary<Guid, string> newCampaignsIds)
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
        private static async Task ProjectTrashOnClosestRiver(IDictionary<Guid, string> newCampaignsIds)
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
        private static async Task InsertNewCampaignsInBiSchema(IDictionary<Guid, string> newCampaignsIds)
        {
            // ************************************ CLEMENT
            var command = "SELECT * FROM campaign.campaign;";
            // ************************************
            IDictionary<string, object> args = new Dictionary<string, object>();
            args.Add("@campaign_id", "1234");
            await Database.ExecuteNonQuery(command, args);

            await ComputeDistanceToSea(newCampaignsIds);
        }

        private static async Task ComputeDistanceToSea(IDictionary<Guid, string> newCampaignsIds)
        {
            // ************************************ CLEMENT
            var command = "SELECT * FROM campaign.campaign;";
            // ************************************
            IDictionary<string, object> args = new Dictionary<string, object>();
            args.Add("@campaign_id", "1234");
            await Database.ExecuteNonQuery(command, args);
        }

        private static async Task<OperationStatus> InsertNewCampaignsInBI(IDictionary<Guid, string> newCampaigns, ILogger log)
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

        private static async Task<IDictionary<Guid, string>> RetrieveNewCampaigns(ILogger log)
        {

            IDictionary<Guid, string> campaigns = new Dictionary<Guid, string>();
            // ************************************ CLEMENT
            var command = "SELECT * FROM campaign.campaign";
            // ************************************
            IDictionary<string, object> args = new Dictionary<string, object>();
            args.Add("@campaign_id", "1234");
            await Database.ExecuteNonQuery(command, args);
            return campaigns;
        }


    }

}
