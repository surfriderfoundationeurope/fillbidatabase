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
        public async Task ExecuteScriptsSteps_SUCCESS()
        {
            Console.WriteLine("USING " + GetTestsConnectionString());
            
            IDatabase Database = new PostgreDatabase(GetTestsConnectionString());
            Guid fakeCampaignId = Guid.NewGuid();
            IDictionary<string, object> Params = new Dictionary<string, object>();
            Params.Add("campaignId", fakeCampaignId.ToString());

            Assert.IsTrue(await Database.ExecuteScriptsAsync(GetSuccessfulTestsStepsToExecute(), Params));
            var res = await Database.ExecuteStringQuery("SELECT * FROM logs.bi WHERE campaign_id = @campaignId", Params);
            Console.WriteLine(res);
            Assert.IsNotNull(res);
        }
        private SortedList<int, string> GetSuccessfulTestsStepsToExecute()
        {
            SortedList<int, string> SqlSteps = new SortedList<int, string>();
            SqlSteps.Add(1, @"./TestsScripts/1_add_fake_campaign.sql");
            SqlSteps.Add(2, @"./TestsScripts/2_remove_fake_campaign.sql");
            return SqlSteps;
        }
        public static string GetTestsConnectionString()
        {
                return "Server=test-pgdb.postgres.database.azure.com;Database=surfriderdb;Port=5432;User Id=testpgdbrootuser@test-pgdb;Password=LePlastiqueCaPiqueBeaucoup!;Ssl Mode=Require;";
        }
    }
}
