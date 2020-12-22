using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace Surfrider.Jobs {
    public interface ICampaignPipeline
    {
        Task<bool> ComputeOnSingleCampaignAsync(Guid newCampaignId, SortedList<int, string> stepsToExecute);
        Task MarkCampaignPipelineAsFailedAsync(Guid campaignId);
        Task MarkCampaignPipelineAsSuccessedAsync(Guid campaignId);
    }

}