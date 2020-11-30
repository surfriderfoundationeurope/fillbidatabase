using Azure.Storage.Blobs;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Surfrider;
using Surfrider.Jobs;
using System;
using System.IO;
using System.Threading.Tasks;

namespace Surfrider.Jobs_RecurringJobs.Tests
{
    [TestClass]
    public class UpdateJsonFileWithDataTest
    {
        string LocalBlobStorageConnection = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";
        string filename = "data_home_page.json";

        [TestMethod]
        public async Task UpdateJsonFileWithData_ContainerAlreadyExists()
        {
            string localPath = "./";
            string localFilePath = Path.Combine(localPath, filename);
            // Create the container a first time
            string containerName = "public";
            BlobContainerClient containerClient = new BlobContainerClient(LocalBlobStorageConnection, containerName);
            containerClient.CreateIfNotExists(Azure.Storage.Blobs.Models.PublicAccessType.BlobContainer);

            IDataFileWriter fileWriter = new DataFileWriter();
            await fileWriter.UpdateJsonFileWithDataAsync(55, 66, 77);

            // check that the file has been uploaded properly
            BlobClient blobClient = containerClient.GetBlobClient(filename);
            await blobClient.DownloadToAsync(localPath);

            Assert.IsTrue(File.Exists(localFilePath));
            
            // Now we test if the file has been updated with new values
            string[] lines = File.ReadAllLines(localFilePath);  
            // Assert.AreEqual(lines[0], \\"contributors\": " + contributors + ",\"coveredKm\": " + coveredKm + ",\"trashPerKm\": " + trashPerKm + "})
            // assert file content is the same
         
        }
        
        [TestMethod]
        public void UpdateJsonFileWithData_ContainerNotExists()
        {
            Console.WriteLine("Test 1");
        }
        
        [TestMethod]
        public void UpdateJsonFileWithData_FileAlreadyExists()
        {
            Console.WriteLine("Test 1");
        }
    }
}
