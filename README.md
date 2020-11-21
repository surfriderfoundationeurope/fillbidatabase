# Fill Bi Database
Code for the recurring job that fills Bi database schema.
This function runs **every day at 02:00**

# Principles
Several ordered SQL request will be executed on the server in order to compute BI data.

![image-20200505161802892](/_images/image-20200505161802892.png)

*In red the steps we think will take time. In grey, the optionnal step we will do in a second time.*



# Architectural Decision
* Why keep separation between campaign.campaign and campaign.media?
The idea here is to stay aligned with the "S" of the SOLID principles : Single Responsability.
As Campaign and Media are two different entities with different business function/purpose, we need to keep them separated in the Database. It allows to extend the possibilities around campaigns without manipulating it.
If not, in long term, this could be impactful on the campaign table which is the core of the whole Plastic Origins application.

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
