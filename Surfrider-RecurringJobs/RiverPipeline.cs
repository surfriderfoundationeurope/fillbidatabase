using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Azure.Storage.Blobs;

namespace Surfrider
{
    public class RiverPipeline : IRiverPipeline
    {
        public Task<IList<Guid>> RetrieveSuccessfullComputedCampaignsIds()
        {
            throw new NotImplementedException();
        }
    }
}