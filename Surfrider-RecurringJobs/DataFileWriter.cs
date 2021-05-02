using System;
using System.IO;
using System.Threading.Tasks;
using Azure.Storage.Blobs;

namespace Surfrider.Jobs
{
    public class DataFileWriter : IDataFileWriter
    {
        public async Task UpdateJsonFileWithDataAsync(int contributors, int coveredKm, int trashPerKm)
        {
             // Create a BlobServiceClient object which will be used to create a container client
            Console.WriteLine("USING BLOB STORAGE CONNECTION STRING --> " + Helper.GetBlobStorageConnectionString());
            //BlobServiceClient blobServiceClient = new BlobServiceClient(Helper.GetBlobStorageConnectionString());

            // Create the container if not exists and return a container client object
            string containerName = "public";
            BlobContainerClient containerClient = new BlobContainerClient(Helper.GetBlobStorageConnectionString(),containerName);
            containerClient.CreateIfNotExists(Azure.Storage.Blobs.Models.PublicAccessType.BlobContainer);
            
            // Create a local file in the ./data/ directory for uploading and downloading
            string localPath = "./";
            string fileName = "data_home_page.json";
            string localFilePath = Path.Combine(localPath, fileName);
            // Write text to the file
            await File.WriteAllTextAsync(localFilePath, "{\"contributors\": " + contributors + ",\"coveredKm\": " + coveredKm + ",\"trashPerKm\": " + trashPerKm + "}");

            // Get a reference to a blob
            BlobClient blobClient = containerClient.GetBlobClient(fileName);

            Console.WriteLine("Uploading to Blob storage as blob:\n\t {0}\n", blobClient.Uri);
            // Open the file and upload its data
            using (FileStream file = File.OpenRead(localFilePath))
            {
                await blobClient.UploadAsync(file);
            }

        }
    }
}