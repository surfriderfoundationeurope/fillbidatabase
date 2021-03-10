using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Azure.Storage.Blobs;

namespace Surfrider.Jobs
{
    public class RiverPipeline : IRiverPipeline
    {
        public string DatabaseConnection { get; }
        public RiverPipeline(string dbConnectionString) => this.DatabaseConnection = dbConnectionString;
        public Task<bool> ComputePipelineOnSingleRiverAsync(string riverId)
        {
            throw new NotImplementedException();
        }

        public async Task MarkRiverPipelineAsFailedAsync(Guid campaignId)
        {
            IDatabase Database = new PostgreDatabase(DatabaseConnection);
            IDictionary<string, string> Params = new Dictionary<string, string>();
            Params.Add("campaignId", campaignId.ToString());
            await Database.ExecuteNonQueryAsync("UPDATE bi_temp.pipelines SET river_has_been_computed = FALSE WHERE campaign_id = '@campaignId' AND campaign_has_been_computed <> TRUE", Params);
        }

        public async Task MarkRiverPipelineAsSuccessedAsync(Guid campaignId)
        {
            IDatabase Database = new PostgreDatabase(DatabaseConnection);
            IDictionary<string, string> Params = new Dictionary<string, string>();
            Params.Add("campaignId", campaignId.ToString());
            await Database.ExecuteNonQueryAsync("UPDATE bi_temp.pipelines SET river_has_been_computed = TRUE WHERE campaign_id = '@campaignId' AND campaign_has_been_computed = TRUE", Params);
        }

        // Retrieve the list of rivers from new campaigns
        // Returns a dictionary {RiverId ; CampaignId}
        public async Task<IDictionary<string, Guid>> RetrieveRiversFromSuccessfullyComputedCampaignsAsync(IList<Guid> newCampaignsIds)
        {
            /// CALL TO script 7_get_bi_rivers_id.sql
            // çaa ne vas pas marcher car on ne peut pas etre sur de l'ordre dans lequel les resultats sont retournes
            // eg. on fournit une liste de campaign ids (non ordonnée), et la requete fait un where dans tous ces ids
            // Même si on renvoie une liste de river names ordonnée alphabetiquement, comment être sur qu'on fait correspondre
            // le bon campaign id à la bonne river?

            // pour l'instant, le seul moyen est de traiter campaign par campaign, donc de récup les river une par une

            IDictionary<string, Guid> CampaignsRiversDict = new Dictionary<string, Guid>();
            IDatabase Database = new PostgreDatabase(DatabaseConnection);
            foreach(var campaignId in newCampaignsIds){ // /!\ Perf!!
                
                IDictionary<string, string> Params = new Dictionary<string, string>();
                Params.Add("campaignId", campaignId.ToString());
                var riverName = await Database.ExecuteNonQueryScriptAsync("./SqlScripts/7_get_bi_rivers_id.sql", Params);
                if(riverName.Status == ScriptStatusEnum.OK){
                    CampaignsRiversDict.Add((string)riverName.Result, campaignId);    
                }
            }
            return CampaignsRiversDict;
        }

        public Task<IDictionary<Guid, string>> GetOldCampaignsFromRivers(IList<string> riversIdsFromNewCampaigns)
        {
            throw new NotImplementedException();
        }

        Task<IDictionary<string, Guid>> IRiverPipeline.GetOldCampaignsFromRivers(IList<string> riversIdsFromNewCampaigns)
        {
            throw new NotImplementedException();
        }

        public IDictionary<Guid, string> MergeCampaignRiverDictionnaries(IDictionary<string, Guid> riversIdsFromNewCampaigns, IDictionary<string, Guid> riverIdsFromOldCampaigns)
        {
            throw new NotImplementedException();
        }
    }
}