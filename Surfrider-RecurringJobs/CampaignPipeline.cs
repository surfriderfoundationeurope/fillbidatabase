using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Surfrider.Jobs.Recurring;

namespace Surfrider.Jobs
{
    public class CampaignPipeline : ICampaignPipeline
    {
        public Task<bool> ComputeOnSingleCampaignAsync(Guid newCampaignId)
        {
            IDatabase Database = new PostgreDatabase(Helper.GetConnectionString());

            PipelineStatus.Status = OperationStatus.OK;
            PipelineStatus.Reason = string.Empty;

            await ExecuteScript(@"./SqlScripts/1_update_bi_trajectory_point.sql");
            if (PipelineStatus.Status == OperationStatus.OK)
                await ExecuteScript(@"./SqlScripts/2_insert_bi_campaign.sql");
            if (PipelineStatus.Status == OperationStatus.OK)
                await ExecuteScript(@"./SqlScripts/4_insert_bi_campaign_distance_to_sea.sql");
            if (PipelineStatus.Status == OperationStatus.OK)
                await ExecuteScript(@"./SqlScripts/7_update_bi_trash.sql"); 
            
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