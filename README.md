# fillbidatabase
Code for the recurring job that fills Bi database schema.

# Test it
This project is easy to test w/ VSCode and a local installation of Node.js (https://nodejs.org/en/).
In order to test and run the project locally, you need to install the following VSCode extensions : 
* C# extension https://marketplace.visualstudio.com/items?itemName=ms-dotnettools.csharp
* Azure Function extension https://marketplace.visualstudio.com/items?itemName=ms-azuretools.vscode-azurefunctions
* Azurite extension (optional, provides a local Azure blob storage emulator)

After extensions installed, please check your local environement can run azure functions : open your Terminal and run "func". Is everything goes well,
the installed version is displayed.

To test the project, go to Debug section, then click on Run.

# Good to mention
* Documentation advise the use of AZURE_FUNCTIONS_ENVIRONMENT variable instead of ASPNETCORE_ENVIRONMENT (cf link to doc below).

# Useful links
Application settings reference for Azure Functions : https://docs.microsoft.com/en-us/azure/azure-functions/functions-app-settings 