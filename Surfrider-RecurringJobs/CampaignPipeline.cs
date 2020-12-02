using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Surfrider.Jobs;

namespace Surfrider.Jobs
{
    public class CampaignPipeline : ICampaignPipeline
    {
        public async Task<bool> ComputeOnSingleCampaignAsync(Guid newCampaignId, SortedList<int, string> sqlSteps)
        {
            PipelineStatus PipelineStatus = new PipelineStatus{
                Status = OperationStatus.OK,
                Reason = string.Empty
            };

            IDatabase Database = new PostgreDatabase(Helper.GetConnectionString());

            IDictionary<string, object> Params = new Dictionary<string, object>();
            Params.Add("campaignId", newCampaignId);

            return await Database.ExecuteScriptsAsync(sqlSteps, Params);
        }

        public async Task MarkCampaignPipelineAsFailedAsync(Guid campaignId)
        {
            IDatabase Database = new PostgreDatabase(Helper.GetConnectionString());
             IDictionary<string, object> Params = new Dictionary<string, object>();
            Params.Add("campaignId", campaignId);
            await Database.ExecuteNonQueryAsync("UPDATE bi_temp.pipelines SET campaign_has_been_computed = 1 WHERE campaign_id = @campaignId", Params);
        }

        public Task MarkCampaignPipelineAsSuccessedAsync(Guid campaignId)
        {
            throw new NotImplementedException();
        }
    }
}