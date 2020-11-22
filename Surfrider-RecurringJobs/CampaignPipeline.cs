using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Azure.Storage.Blobs;

namespace Surfrider
{
    public class CampaignPipeline : ICampaignPipeline
    {
        public Task<bool> ComputeOnSingleCampaignAsync(Guid newCampaignId)
        {
            throw new NotImplementedException();
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