# Google_Ads_ad_account_generator
Python script (back-end) created by **Dimitrios Panourgias**
<br/> July 2020

<br/> This python script is used for bulk generating an entire Google Ads ad account. It populates the account with campaigns, ad groups, keywords and text ads with prespecified settings and based on a prespecified structure that fits the business model. **It can create in a few minutes hundreds of campaigns with thousands of ad groups and keywords. The uploaded script is fit for the Travel Industry, thus the ad groups refer to origin-destination routes based on certain means of transportation.**

<br/> This script can be further integrated with a solid database structure that aggregates data (search queries and their respective metrics) from Google Search Console API and Google Ads API, identifies keyword themes (using NLP techniques) and feeds this python script with the most appropriate ones to be set up in an ad account. In this repository the aforementioned database structure and data pipeline is not analyzed (although it is used in reality along with this script).

<br/> The script is semi-manual (on purpose) since in certain steps requires user input. The steps that are implemented once the script runs are described shortly below:
1.	The script begins with fetching a csv file (database output) that contains the desired structure of the ad account, meaning a flat table with four columns (campaign name, ad group name, final URL, keyword). On campaign level the keyword theme is defined, while on ad group level the route (origin-destination) is defined (see image in folder "img". In the campaign name, fr stands for the domain, fra, for the country, French for the language, train for the transportation type and aujourd'hui for the keyword theme).
2.	Then it asks the user to input the desired attributes (domain, country, language and transportation type).
3.	It proceeds with the generation of all entities except the ads. Note that the entities generation is implemented with the use of batch processes in order to avoid overloading the API with multiple requests.
4.	Before creating the ads, the script again asks the user for input regarding the second headline as well as the first and second description.
5.	Finally, the user should see an exit code 0 (successful implementation of the script) and the entire structure correctly set up in the Google Ads user interface.

<br/> For drafting this script the official guidelines and examples of Adwords API were used. The script is still maintained and constantly optimized and readjusted to fit the purposes of the business, however no more details or updates will be uploaded in this repository.

<br/> **This work can demonstrate the power and potential for automations of Python, SQL and APIs, when combined in a strategical and functional way.**

