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
        // { RiverID , CampaignID }        
        // where RiverID is the corresponding river where the campaign has been made on
        // and CampaignID is the campaign where the Campaign Pipeline has been successfull.
        // /!\ : RiverID is the river name for now.
        Task<IDictionary<string, Guid>> RetrieveRiversFromSuccessfullyComputedCampaignsAsync(IList<Guid> newCampaignsIds);
        Task<bool> ComputePipelineOnSingleRiverAsync(string riverId);
        Task MarkRiverPipelineAsSuccessedAsync(Guid campaignId);
        Task MarkRiverPipelineAsFailedAsync(Guid campaignId);
        // Returns a dictionary of 
        // { RiverId , CampaignId } 
        // where riverId is a river for which a new campaign has been made
        // and campaignId is an old campaign made on the river RiverId.
        Task<IDictionary<string, Guid>> GetOldCampaignsFromRivers(IList<string> riversIdsFromNewCampaigns);
        IDictionary<Guid, string> MergeCampaignRiverDictionnaries(IDictionary<string, Guid> riversIdsFromNewCampaigns, IDictionary<string, Guid> riverIdsFromOldCampaigns);
    }

}