using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Azure.Storage.Blobs;

namespace Surfrider.Jobs
{
    public class RiverPipeline : IRiverPipeline
    {
        public Task<bool> ComputePipelineOnSingleRiverAsync(string riverId)
        {
            throw new NotImplementedException();
        }

        public Task MarkRiverPipelineAsFailedAsync(object key)
        {
            throw new NotImplementedException();
        }

        public Task MarkRiverPipelineAsSuccessedAsync(object key)
        {
            throw new NotImplementedException();
        }

        public Task<IList<Guid>> RetrieveSuccessfullComputedCampaignsIds()
        {
            throw new NotImplementedException();
        }

        Task<IDictionary<Guid, string>> IRiverPipeline.RetrieveSuccessfullComputedCampaignsIds()
        {
            throw new NotImplementedException();
        }
    }
}