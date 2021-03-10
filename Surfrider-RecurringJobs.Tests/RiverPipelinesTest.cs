using Azure.Storage.Blobs;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Surfrider;
using Surfrider.Jobs;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Surfrider.Jobs_RecurringJobs.Tests
{
    [TestClass]
    public class RiverPipelinesTest
    {

        [TestCleanup()]
        public void Cleanup()
        {
            IDatabase Database = new PostgreDatabase(GetTestsConnectionString());
            Database.ExecuteNonQueryAsync("DELETE FROM bi_temp.pipelines"); //empty the table after all tests
        }

        [TestMethod]
        public async Task RetrieveSuccessfullComputedCampaignsRiversAsync_SUCCESS()
        {
             Console.WriteLine("USING " + GetTestsConnectionString());
            IRiverPipeline riverPipeline = new RiverPipeline(GetTestsConnectionString());

            IDatabase Database = new PostgreDatabase(GetTestsConnectionString());
            // Adds a pipeline where 2 campaigns have been successfully computed and no river has been computed yet
            Guid campaignIdSuccessfullyComputed_1 = Guid.NewGuid();
            Guid campaignIdSuccessfullyComputed_2 = Guid.NewGuid();
            Guid campaignIdFailedComputed_1 = Guid.NewGuid();
            IDictionary<string, string> Params = new Dictionary<string, string>();
            Params.Add("campaignIdSuccessfullyComputed_1", campaignIdSuccessfullyComputed_1.ToString());
            Params.Add("campaignIdSuccessfullyComputed_2", campaignIdSuccessfullyComputed_2.ToString());
            Params.Add("campaignIdFailedComputed_1", campaignIdFailedComputed_1.ToString());

            await Database.ExecuteNonQueryScriptAsync(@"./TestsScripts/3_add_fake_campaigns.sql", Params);
            // Mark rivers as computed successfully
            IList<Guid> campaignIds = new List<Guid>();
            campaignIds.Add(campaignIdSuccessfullyComputed_1);
            campaignIds.Add(campaignIdSuccessfullyComputed_2);
            campaignIds.Add(campaignIdFailedComputed_1);
            var successfullCampaigns =  await riverPipeline.RetrieveRiversFromSuccessfullyComputedCampaignsAsync(campaignIds);
            
            // only two campaigns were marked as successfully computed
            Assert.IsTrue(successfullCampaigns.Keys.Count == 2);
            
            // check for each retrieved campaign, there is a corresponding riverId returned
            Assert.IsFalse(successfullCampaigns.Values.Any(x => x == string.Empty || x == null)); // none of values in the dictionnary are supposed to be empty or null

            // checks retrieved campaigns are marked as successfully computed 
            foreach (var campaignId in successfullCampaigns.Keys)
            {
                var campaignIdParam = new Dictionary<string, string>();
                campaignIdParam.Add("campaignId", campaignId.ToString());
                var campaign_has_been_computed = await Database.ExecuteStringQueryAsync("SELECT campaign_has_been_computed FROM bi_temp.pipelines WHERE campaign_id = '@campaignId'", campaignIdParam);
                Assert.AreEqual("true", campaign_has_been_computed.First().ToLowerInvariant());
            }
        }
        [TestMethod]
        public async Task MarkRiverPipelineAsFailedAsync_SUCCESS()
        {
             Console.WriteLine("USING " + GetTestsConnectionString());
            IRiverPipeline riverPipeline = new RiverPipeline(GetTestsConnectionString());

            IDatabase Database = new PostgreDatabase(GetTestsConnectionString());
            // Adds a pipeline where 2 campaigns have been successfully computed and no river has been computed yet
            Guid campaignIdSuccessfullyComputed_1 = Guid.NewGuid();
            Guid campaignIdSuccessfullyComputed_2 = Guid.NewGuid();
            Guid campaignIdFailedComputed_1 = Guid.NewGuid();
            IDictionary<string, string> Params = new Dictionary<string, string>();
            Params.Add("campaignIdSuccessfullyComputed_1", campaignIdSuccessfullyComputed_1.ToString());
            Params.Add("campaignIdSuccessfullyComputed_2", campaignIdSuccessfullyComputed_2.ToString());
            Params.Add("campaignIdFailedComputed_1", campaignIdFailedComputed_1.ToString());

            await Database.ExecuteNonQueryScriptAsync(@"./TestsScripts/3_add_fake_campaigns.sql", Params);
            // Mark rivers as computed successfully
            foreach (var campaignId in Params.Values)
                await riverPipeline.MarkRiverPipelineAsFailedAsync(new Guid(campaignId));
            // retrieve the campaigns where river_has_been_computed = false
            IList<string> campaignsIds = await Database.ExecuteStringQueryAsync("SELECT campaign_id FROM bi_temp.pipelines WHERE river_has_been_computed = false");
            // only two campaigns should have been marked as river_has_been_computed = false
            Assert.IsTrue(campaignsIds.Count == 1);
            // checks rivers are now marked as fail computed
            foreach (var campaignId in campaignsIds)
            {
                var campaignIdParam = new Dictionary<string, string>();
                campaignIdParam.Add("campaignId", campaignId.ToString());
                var river_has_been_computed = await Database.ExecuteStringQueryAsync("SELECT river_has_been_computed FROM bi_temp.pipelines WHERE campaign_id = '@campaignId'", campaignIdParam);
                Assert.AreEqual("false", river_has_been_computed.First().ToLowerInvariant());
            }
        }
        [TestMethod]
        public async Task MarkRiverPipelineAsFailedAsync_CampaignPipelineDoesntExist()
        {
             Console.WriteLine("USING " + GetTestsConnectionString());
            IRiverPipeline riverPipeline = new RiverPipeline(GetTestsConnectionString());

            IDatabase Database = new PostgreDatabase(GetTestsConnectionString());
           
            Assert.await riverPipeline.MarkRiverPipelineAsFailedAsync(new Guid(campaignId));
           
        }
        [TestMethod]
        public async Task MarkRiverPipelineAsSuccessedAsync_SUCCESS()
        {
            Console.WriteLine("USING " + GetTestsConnectionString());
            IRiverPipeline riverPipeline = new RiverPipeline(GetTestsConnectionString());

            IDatabase Database = new PostgreDatabase(GetTestsConnectionString());
            // Adds a pipeline where 2 campaigns have been successfully computed and no river has been computed yet
            Guid campaignIdSuccessfullyComputed_1 = Guid.NewGuid();
            Guid campaignIdSuccessfullyComputed_2 = Guid.NewGuid();
            Guid campaignIdFailedComputed_1 = Guid.NewGuid();
            IDictionary<string, string> Params = new Dictionary<string, string>();
            Params.Add("campaignIdSuccessfullyComputed_1", campaignIdSuccessfullyComputed_1.ToString());
            Params.Add("campaignIdSuccessfullyComputed_2", campaignIdSuccessfullyComputed_2.ToString());
            Params.Add("campaignIdFailedComputed_1", campaignIdFailedComputed_1.ToString());

            await Database.ExecuteNonQueryScriptAsync(@"./TestsScripts/3_add_fake_campaigns.sql", Params);
            // Mark rivers as computed successfully
            foreach (var campaignId in Params.Values)
                await riverPipeline.MarkRiverPipelineAsSuccessedAsync(new Guid(campaignId));
            // retrieve the campaigns where river_has_been_computed = true
            IList<string> campaignsIds = await Database.ExecuteStringQueryAsync("SELECT campaign_id FROM bi_temp.pipelines WHERE river_has_been_computed = true");
            // only two campaigns should have been marked as river_has_been_computed = true
            Assert.IsTrue(campaignsIds.Count == 2);
            // checks rivers are now marked as successfully computed
            foreach (var campaignId in campaignsIds)
            {
                var campaignIdParam = new Dictionary<string, string>();
                campaignIdParam.Add("campaignId", campaignId.ToString());
                var river_has_been_computed = await Database.ExecuteStringQueryAsync("SELECT river_has_been_computed FROM bi_temp.pipelines WHERE campaign_id = '@campaignId'", campaignIdParam);
                Assert.AreEqual("true", river_has_been_computed.First().ToLowerInvariant());
            }
        }
   
        private string GetTestsConnectionString()
        {
            return "Server=test-pgdb.postgres.database.azure.com;Database=surfriderdb;Port=5432;User Id=testpgdbrootuser@test-pgdb;Password=LePlastiqueCaPiqueBeaucoup!;Ssl Mode=Require;";
        }
    }
}
