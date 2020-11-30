using Azure.Storage.Blobs;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Surfrider;
using Surfrider.Jobs;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Surfrider.Jobs_RecurringJobs.Tests
{
    [TestClass]
    public class ExecuteScriptsTests
    {
        [TestMethod]
        public async Task ExecuteScripts_SUCCESS()
        {

            IDatabase Database = new PostgreDatabase(Helper.GetConnectionString());
            Guid fakeCampaignId = Guid.NewGuid();
            IDictionary<string, object> Params = new Dictionary<string, object>();
            Params.Add("campaignId", fakeCampaignId);

            Assert.IsTrue(await Database.ExecuteScriptsAsync(GetSuccessfulTestsStepsToExecute(), Params));
        }
        private SortedList<int, string> GetSuccessfulTestsStepsToExecute()
        {
            SortedList<int, string> SqlSteps = new SortedList<int, string>();
            SqlSteps.Add(1, @"./TestsScripts/1_add_fake_campaign.sql");
            SqlSteps.Add(2, @"./TestsScripts/2_remove_fake_campaign.sql");
            return SqlSteps;
        }
    }
}
