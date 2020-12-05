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
        [TestMethod]
        public async Task RetrieveSuccessfullComputedCampaignsIds_SUCCESS()
        {
            Console.WriteLine("USING " + GetTestsConnectionString());
            
            IDatabase Database = new PostgreDatabase(GetTestsConnectionString());
            
            IDictionary<string, string> Params = new Dictionary<string, string>();

            IRiverPipeline riverPipeline = new RiverPipeline(GetTestsConnectionString());
            IDictionary<Guid, string> campaigns = await riverPipeline.RetrieveSuccessfullComputedCampaignsIds();


        }
        [TestMethod]
        public async Task MarkRiverPipelineAsFailedAsync_SUCCESS()
        {
            Console.WriteLine("USING " + GetTestsConnectionString());
            
            IDatabase Database = new PostgreDatabase(GetTestsConnectionString());
            Guid fakeCampaignId = Guid.NewGuid();
            IDictionary<string, string> Params = new Dictionary<string, string>();
            Params.Add("campaignId", fakeCampaignId.ToString());

         
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

            await Database.ExecuteScriptAsync(@"./TestsScripts/3_add_fake_campaigns.sql", Params);
            // Mark river as computed successfully
            //await riverPipeline.MarkRiverPipelineAsSuccessedAsync(fakeCampaignId);
            // retrieve the campaigns where river_has_been_computed = true
            var res = Database.ExecuteStringQueryAsync("SELECT campaign_id FROM bi_temp.pipelines WHERE river_has_been_computed = true", Params);
            // checks the campaign was successfully computed
            // Assert.IsTrue()
            // checks the river is marked as successfully computed
            // Assert.IsTrue();
        }
        [TestMethod]
        public async Task RetrieveSuccessfullComputedCampaignsIdsAsync_SUCCESS()
        {
            Console.WriteLine("USING " + GetTestsConnectionString());
            IRiverPipeline riverPipeline = new RiverPipeline(GetTestsConnectionString());

            // Adds fake campaigns where 1 has not been successfully computed
            IDatabase Database = new PostgreDatabase(GetTestsConnectionString());
            Guid campaignIdSuccessfullyComputed_1 = Guid.NewGuid();
            Guid campaignIdSuccessfullyComputed_2 = Guid.NewGuid();
            Guid campaignIdFailedComputed_1 = Guid.NewGuid();
            IDictionary<string, string> Params = new Dictionary<string, string>();
            Params.Add("campaignIdSuccessfullyComputed_1", campaignIdSuccessfullyComputed_1.ToString());
            Params.Add("campaignIdSuccessfullyComputed_2", campaignIdSuccessfullyComputed_2.ToString());
            Params.Add("campaignIdFailedComputed_1", campaignIdFailedComputed_1.ToString());

            await Database.ExecuteScriptAsync(@"./TestsScripts/3_add_fake_campaigns.sql", Params);

            var successfullCampaigns = await riverPipeline.RetrieveSuccessfullComputedCampaignsIds();
            Assert.AreEqual(2, successfullCampaigns.Count());
        }
        private string GetTestsConnectionString()
        {
                return "Server=test-pgdb.postgres.database.azure.com;Database=surfriderdb;Port=5432;User Id=testpgdbrootuser@test-pgdb;Password=LePlastiqueCaPiqueBeaucoup!;Ssl Mode=Require;";
        }
    }
}
