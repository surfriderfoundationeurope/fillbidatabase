using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Azure.Storage.Blobs;

namespace Surfrider.Jobs
{
    public class RiverPipeline : IRiverPipeline
    {
        public string DatabaseConnection { get; }
        public RiverPipeline(string dbConnectionString) => this.DatabaseConnection = dbConnectionString;
        public Task<bool> ComputePipelineOnSingleRiverAsync(string riverId)
        {
            throw new NotImplementedException();
        }

        public async Task MarkRiverPipelineAsFailedAsync(Guid campaignId)
        {
            IDatabase Database = new PostgreDatabase(DatabaseConnection);
            IDictionary<string, string> Params = new Dictionary<string, string>();
            Params.Add("campaignId", campaignId.ToString());
            await Database.ExecuteNonQueryAsync("UPDATE bi_temp.pipelines SET river_has_been_computed = FALSE WHERE campaign_id = '@campaignId'", Params);
        }

        public async Task MarkRiverPipelineAsSuccessedAsync(Guid campaignId)
        {
            IDatabase Database = new PostgreDatabase(DatabaseConnection);
            IDictionary<string, string> Params = new Dictionary<string, string>();
            Params.Add("campaignId", campaignId.ToString());
            await Database.ExecuteNonQueryAsync("UPDATE bi_temp.pipelines SET river_has_been_computed = TRUE WHERE campaign_id = '@campaignId'", Params);
        }

        public async Task<IList<Guid>> RetrieveSuccessfullComputedCampaignsIdsAsync()
        {
            // IDatabase Database = new PostgreDatabase(DatabaseConnection);
            // IDictionary<string, string> Params = new Dictionary<string, string>();
            // var campaignsIds = await Database.ExecuteStringQueryAsync("SELECT campaign_id FROM bi_temp.pipelines WHERE campaign_has_been_computed = TRUE", Params);
            throw new NotImplementedException();
        }

        /// CALL TO script 6_get_bi_rivers_id.sql
        Task<IDictionary<Guid, string>> IRiverPipeline.RetrieveSuccessfullComputedCampaignsIds()
        {
            throw new NotImplementedException();
        }
    }
}