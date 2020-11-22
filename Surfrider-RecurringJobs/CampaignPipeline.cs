using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Surfrider.Jobs;

namespace Surfrider.Jobs
{
    public class CampaignPipeline : ICampaignPipeline
    {
        public async Task<bool> ComputeOnSingleCampaignAsync(Guid newCampaignId)
        {
            PipelineStatus PipelineStatus = new PipelineStatus();
            IDatabase Database = new PostgreDatabase(Helper.GetConnectionString());

            PipelineStatus.Status = OperationStatus.OK;
            PipelineStatus.Reason = string.Empty;
            IDictionary<string, object> Param = new Dictionary<string, object>();
            Param.Add("campaignId", newCampaignId);

            ;
            if (await Database.ExecuteScript(@"./SqlScripts/1_update_bi_trajectory_point.sql", Param).Result.Status == OperationStatus.OK)
                await Database.ExecuteScript(@"./SqlScripts/2_insert_bi_campaign.sql", Param);
            if (PipelineStatus.Status == OperationStatus.OK)
                await Database.ExecuteScript(@"./SqlScripts/4_insert_bi_campaign_distance_to_sea.sql", Param);
            if (PipelineStatus.Status == OperationStatus.OK)
                await Database.ExecuteScript(@"./SqlScripts/7_update_bi_trash.sql", Param); 
            
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