using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace Surfrider.Jobs {
    public interface IRiverPipeline
    { 
        // Returns a dictionary of 
        // { CampaignID , RiverID }        
        // where CampaignID is a campaign where the Campaign Pipeline has been successfull, and the RiverID is the corresponding river where
        // the campaign has been made on.
        Task<IDictionary<Guid, string>> RetrieveSuccessfullComputedCampaignsRiversAsync(IList<Guid> newCampaignsIds);
        Task<bool> ComputePipelineOnSingleRiverAsync(string riverId);
        Task MarkRiverPipelineAsSuccessedAsync(Guid campaignId);
        Task MarkRiverPipelineAsFailedAsync(Guid campaignId);
    }

}