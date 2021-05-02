using System;
using System.Collections.Generic;
using System.Text;

namespace Surfrider.Jobs
{
    class Campaign
    {
        public Guid PipelineId { get; set; }
        public Guid CompaignId { get; set; }


        public Campaign(Guid pipelineId, Guid compaignId)
        {
            this.PipelineId = pipelineId;
            this.CompaignId = compaignId;
        }
    }
}
