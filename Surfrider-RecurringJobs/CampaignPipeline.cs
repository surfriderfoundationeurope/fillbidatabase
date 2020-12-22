using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Surfrider.Jobs;

namespace Surfrider.Jobs
{
    public class CampaignPipeline : ICampaignPipeline
    {
        string DatabaseConnection  { get; }
        public CampaignPipeline(string connectionString) => this.DatabaseConnection = connectionString;

        public async Task<bool> ComputeOnSingleCampaignAsync(Guid newCampaignId, SortedList<int, string> sqlSteps)
        {
            PipelineStatus PipelineStatus = new PipelineStatus
            {
                Status = OperationStatus.OK,
                Reason = string.Empty
            };

            IDatabase Database = new PostgreDatabase(DatabaseConnection);

            IDictionary<string, string> Params = new Dictionary<string, string>();
            Params.Add("campaignId", newCampaignId.ToString());

            return await Database.ExecuteScriptsAsync(sqlSteps, Params);
        }

        public async Task MarkCampaignPipelineAsFailedAsync(Guid campaignId)
        {
            IDatabase Database = new PostgreDatabase(DatabaseConnection);
            IDictionary<string, string> Params = new Dictionary<string, string>();
            Params.Add("campaignId", campaignId.ToString());
            await Database.ExecuteNonQueryAsync("UPDATE bi_temp.pipelines SET campaign_has_been_computed = FALSE WHERE campaign_id = '@campaignId'", Params);
        }

        public async Task MarkCampaignPipelineAsSuccessedAsync(Guid campaignId)
        {
            IDatabase Database = new PostgreDatabase(DatabaseConnection);
            IDictionary<string, string> Params = new Dictionary<string, string>();
            Params.Add("campaignId", campaignId.ToString());
            await Database.ExecuteNonQueryAsync("UPDATE bi_temp.pipelines SET campaign_has_been_computed = TRUE WHERE campaign_id = '@campaignId'", Params);
        }
    }
}