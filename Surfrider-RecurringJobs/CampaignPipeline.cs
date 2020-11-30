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

        public Task MarkCampaignPipelineAsFailedAsync(Guid campaignId)
        {
            throw new NotImplementedException();
        }

        public Task MarkCampaignPipelineAsSuccessedAsync(Guid campaignId)
        {
            throw new NotImplementedException();
        }
    }
}