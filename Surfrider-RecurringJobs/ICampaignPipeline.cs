using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace Surfrider {
    public interface ICampaignPipeline
    {
        Task<bool> ComputeOnSingleCampaignAsync(Guid newCampaignId);
        Task MarkCampaignPipelineAsFailedAsync(Guid campaignId);
        Task MarkCampaignPipelineAsSuccessedAsync(Guid campaignId);
    }

}