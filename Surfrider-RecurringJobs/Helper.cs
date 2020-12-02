using System;
using Azure.Identity;
using Azure.Security.KeyVault.Secrets;
using Microsoft.Extensions.Configuration;

namespace Surfrider.Jobs {

public static class Helper {
    // https://docs.microsoft.com/en-us/azure/key-vault/secrets/quick-create-net
        private static string GetKeyVaultConnectionString(string secretName)
        {

            string keyVaultName = Environment.GetEnvironmentVariable("KEY_VAULT_NAME");
            var kvUri = "https://" + keyVaultName + ".vault.azure.net";

            var client = new SecretClient(new Uri(kvUri), new DefaultAzureCredential());

            KeyVaultSecret secret = client.GetSecret(secretName);
            return secret.Value;
        }
        public static string GetConnectionString()
        {
            // if (Environment.GetEnvironmentVariable("AZURE_FUNCTIONS_ENVIRONMENT") != "Local")
            //     return GetKeyVaultConnectionString("db-plastico-dev-connectionstring");
            // else
                return Environment.GetEnvironmentVariable("postgre_connection");
        }
        
        public static string GetBlobStorageConnectionString()
        {
            // if (Environment.GetEnvironmentVariable("AZURE_FUNCTIONS_ENVIRONMENT") != "Local")
            //     return GetKeyVaultConnectionString("db-plastico-dev-connectionstring");
            // else
                return Environment.GetEnvironmentVariable("blob_storage_connection");
        }
}
}