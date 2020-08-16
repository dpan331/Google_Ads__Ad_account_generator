"""
created by Dimitrios Panourgias
July 2020

DOES:
- User input for tld, country, language, transportation type
- Creates campaigns based on EC (Networks: Google Search,
                                Shared budget: will be replaced by user in UI,
                                Bidding strategy: will be replaced by user in UI,
                                Location, Language: from user input country, language,
                                trackingURL: done)
- Creates ad groups based on EC
- Creates keywords in ad groups based on EC
- User input for Headline 2 and Description 1 of text ads
- Creates text ad in ad groups based on EC

TO DO NEXT:

DON'T FORGET:
- to add as many locations and Languages as possible in the respective
dictionaries in the Location and Language Criterion functions


"""

import random
import time
import uuid
from urllib.request import urlopen
import pandas as pd
from googleads import adwords

# Request necessary input from user
tld       = input('Please specify tld (i.e. fr): ')
country   = input('Please specify country (i.e. fra): ')
language  = input('Please specify language (i.e. French): ')
transType = input('Please specify transportation type (i.e. train): ')
print('\n')
print('You requested ads to be made for the following combination: ' + tld
      + '_' + country + '_' + language + '_' + transType +
      ' in the Google Ads ad account: ' + tld)
print('\n')

# Send the aforementioned user input to Keyword Library -KL-
# in order for KL to create the temporary data table Entities to Create -EC-


# get EC data table from Keyword Library
ec = pd.read_csv('xentitiestocreate_fr_fra_French_train_2020_08_07.csv', header=None, error_bad_lines=False, encoding= 'unicode escape')

# Drop rows with keyword length greater than 79
ec = ec[ec.iloc[:,3].map(len) < 79]

# Retrieve campaign names from EC
camps = ec.iloc[:,:1].copy()
camps.dropna(inplace=True)
camps = camps.iloc[1:]
camp_nam_dupl = camps[0].to_list()
camp_nam = list(dict.fromkeys(camp_nam_dupl))

# Retrieve ad group names from EC
adgs = ec.iloc[:,1].copy()
adgs.dropna(inplace=True)
adgs = adgs.iloc[1:]
adgs_nam_dupl = adgs.to_list()
adgs_nam = list(dict.fromkeys(adgs_nam_dupl))

# Retrieve keywords from EC
keyws = ec.iloc[:,3].copy()
keyws.dropna(inplace=True)
keyws = keyws.iloc[1:]
keyws_ls_dupl = keyws.to_list()
keyws_ls = list(dict.fromkeys(keyws_ls_dupl))

# Drop campaign column and rename columns
adg_kew = ec.iloc[:,1:4].copy()
adg_kew.dropna(thresh=1, inplace=True)
adg_kew = adg_kew.iloc[1:]
adg_kew.columns=['adg_name','url','keyword']
adg_kew.to_csv('check2.csv')


NUMBER_OF_CAMPAIGNS_TO_ADD = len(camp_nam)

MAX_POLL_ATTEMPTS = 5
PENDING_STATUSES = ('ACTIVE', 'AWAITING_FILE', 'CANCELING')
PAGE_SIZE = 100


def mainAddCampaigns(client, number_of_campaigns, campaign_names, loc, lang):
  # Initialize BatchJobHelper.
  batch_job_helper = client.GetBatchJobHelper(version='v201809')

  # Create a BatchJob.
  batch_job = AddBatchJob(client)
  # Retrieve the URL used to upload the BatchJob operations.
  upload_url = batch_job['uploadUrl']['url']
  batch_job_id = batch_job['id']
  print('Created ADD CAMPAIGNS BatchJob with ID "%d", status "%s", and upload URL "%s"' % (
      batch_job['id'], batch_job['status'], upload_url))

  # Generate operations to upload.
  budget_operations = BuildBudgetOperations(batch_job_helper)
  campaign_operations = BuildCampaignOperations(
      batch_job_helper, budget_operations, number_of_campaigns, campaign_names)
  location_criterion_operations = BuildLocationCriterionOperations(
      campaign_operations, loc)
  language_criterion_operations = BuildLanguageCriterionOperations(
      campaign_operations, lang)

  # Upload operations.
  batch_job_helper.UploadOperations(
      upload_url, budget_operations, campaign_operations, location_criterion_operations, language_criterion_operations)

  # Download and display results.
  download_url = GetBatchJobDownloadUrlWhenReady(client, batch_job_id)
  response = urlopen(download_url).read()
  PrintResponse(batch_job_helper, response)



def mainGetCampaigns(client):
  # Initialize appropriate service.
  campaign_service = client.GetService('CampaignService', version='v201809')
  print('Get created campaigns and their respective ids')
  # Construct selector and get all campaigns.
  offset = 0
  selector = {
      'fields': ['Id', 'Name', 'Status'],
      'paging': {
          'startIndex': str(offset),
          'numberResults': str(PAGE_SIZE)
      }
  }
  camp_dict = {}
  more_pages = True
  while more_pages:
    page = campaign_service.get(selector)

    # Display results.
    if 'entries' in page:
      for campaign in page['entries']:
        camp_dict[campaign['name']] = campaign['id']
        print('Campaign with id "%s", name "%s", and status "%s" was '
              'found.' % (campaign['id'], campaign['name'],
                          campaign['status']))
    else:
      print('No campaigns were found.')
    offset += PAGE_SIZE
    selector['paging']['startIndex'] = str(offset)
    more_pages = offset < int(page['totalNumEntries'])
  print('\n')
  return camp_dict


def mainGetAdGroups(client, campaign_id, dict):
  # Initialize appropriate service.
  ad_group_service = client.GetService('AdGroupService', version='v201809')
  #print('Get created ad groups and their respective ids')
  # Construct selector and get all ad groups.
  offset = 0
  selector = {
      'fields': ['Id', 'Name', 'Status'],
      'predicates': [
          {
              'field': 'CampaignId',
              'operator': 'EQUALS',
              'values': [campaign_id]
          }
      ],
      'paging': {
          'startIndex': str(offset),
          'numberResults': str(PAGE_SIZE)
      }
  }
  tempo_adg_dict = {}
  more_pages = True
  while more_pages:
    page = ad_group_service.get(selector)

    # Display results.
    if 'entries' in page:
      for ad_group in page['entries']:
        tempo_adg_dict[ad_group['name']] = ad_group['id']
        """print('Ad group with name "%s", id "%s" and status "%s" was '
              'found.' % (ad_group['name'], ad_group['id'],
                          ad_group['status']))"""
    else:
      print('No ad groups were found.')
    offset += PAGE_SIZE
    selector['paging']['startIndex'] = str(offset)
    more_pages = offset < int(page['totalNumEntries'])
    z = {**dict, **tempo_adg_dict}
  print('\n')
  return z



def mainAddAdGroups(client, campID, adgNAM):
  # Initialize BatchJobHelper.
  batch_job_helper = client.GetBatchJobHelper(version='v201809')

  # Create a BatchJob.
  batch_job = AddBatchJob(client)
  # Retrieve the URL used to upload the BatchJob operations.
  upload_url = batch_job['uploadUrl']['url']
  batch_job_id = batch_job['id']
  print('Created ADD AD GROUPS BatchJob with ID "%d", status "%s", and upload URL "%s"' % (
      batch_job['id'], batch_job['status'], upload_url))

  # Generate operations to upload.
  adgroup_operations = BuildAdGroupOperations(
      batch_job_helper, campID, adgNAM)

  # Upload operations.
  batch_job_helper.UploadOperations(
      upload_url, adgroup_operations)

  # Download and display results.
  download_url = GetBatchJobDownloadUrlWhenReady(client, batch_job_id)
  response = urlopen(download_url).read()
  PrintResponse(batch_job_helper, response)



def mainAddAdGroupsKeywords(client, adgids, keywds):
  # Initialize BatchJobHelper.
  batch_job_helper = client.GetBatchJobHelper(version='v201809')
  client.partial_failure = True

  # Create a BatchJob.
  batch_job = AddBatchJob(client)
  # Retrieve the URL used to upload the BatchJob operations.
  upload_url = batch_job['uploadUrl']['url']
  batch_job_id = batch_job['id']
  print('Created ADD KEYWORDS BatchJob with ID "%d", status "%s", and upload URL "%s"' % (
      batch_job['id'], batch_job['status'], upload_url))

  # Generate operations to upload.
  adgroup_criterion_operations = BuildAdGroupCriterionOperations(
      adgids, keywds)

  # Upload operations.
  batch_job_helper.UploadOperations(
      upload_url,
      adgroup_criterion_operations)

  # Download and display results.
  download_url = GetBatchJobDownloadUrlWhenReady(client, batch_job_id)
  response = urlopen(download_url).read()
  PrintResponse(batch_job_helper, response)


def mainAddAdCopies(client, adgIDs, ORG, DST, URL, HEAD1, DESC1, HEAD2, DESC2):
  # Initialize BatchJobHelper.
  batch_job_helper = client.GetBatchJobHelper(version='v201809')

  # Create a BatchJob.
  batch_job = AddBatchJob(client)
  # Retrieve the URL used to upload the BatchJob operations.
  upload_url = batch_job['uploadUrl']['url']
  batch_job_id = batch_job['id']
  print('Created ADD TEXT ADS BatchJob with ID "%d", status "%s", and upload URL "%s"' % (
      batch_job['id'], batch_job['status'], upload_url))

  # Generate operations to upload.
  adgroup_ad_operations = BuildAdGroupAdOperations(
      adgIDs, ORG, DST, URL, HEAD1, DESC1, HEAD2, DESC2)

  # Upload operations.
  batch_job_helper.UploadOperations(
      upload_url, adgroup_ad_operations)

  # Download and display results.
  download_url = GetBatchJobDownloadUrlWhenReady(client, batch_job_id)
  response = urlopen(download_url).read()
  PrintResponse(batch_job_helper, response)


def AddBatchJob(client):
  """Add a new BatchJob to upload operations to.

  Args:
    client: an instantiated AdWordsClient used to retrieve the BatchJob.

  Returns:
    The new BatchJob created by the request.
  """
  # Initialize appropriate service.
  batch_job_service = client.GetService('BatchJobService', version='v201809')
  # Create a BatchJob.
  batch_job_operations = [{
      'operand': {},
      'operator': 'ADD'
  }]
  return batch_job_service.mutate(batch_job_operations)['value'][0]


def BuildBudgetOperations(batch_job_helper):
  """Builds the operations needed to create a new Budget.

  Note: When the Budget is created, it will have a different Id than the one
  generated here as a temporary Id. This is just used to identify it in the
  BatchJobService.

  Args:
    batch_job_helper: a BatchJobHelper instance.

  Returns:
    a list containing the operation that will create a new Budget.
  """
  # A list of operations creating a Budget.
  budget_operations = [{
      # The xsi_type of the operation can usually be guessed by the API because
      # a given service only handles one type of operation. However, batch jobs
      # process operations of different types, so the xsi_type must always be
      # explicitly defined for these operations.
      'xsi_type': 'BudgetOperation',
      'operand': {
          'name': 'Batch budget (to be deleted) #%s' % uuid.uuid4(),
          # This is a temporary Id used by the BatchJobService to identify the
          # Budget for operations that require a budgetId.
          'budgetId': batch_job_helper.GetId(),
          'amount': {
              'microAmount': '5000000'
          },
          'deliveryMethod': 'STANDARD'
      },
      'operator': 'ADD'
  }]

  return budget_operations


def BuildLocationCriterionOperations(campaign_operations, location):
  """Builds the operations needed to create Location Criterion.

  Args:
    campaign_operations: a list containing the operations that will add
      Campaigns.

  Returns:
    a list containing the operations that will create a new Location
    Criterion associated with each provided Campaign.
  """
  locDict = {'fra': 2250, 'esp': 2724, 'can': 2124}

  try:
    location_operations = [
      {
          # The xsi_type of the operation can usually be guessed by the API
          # because a given service only handles one type of operation.
          # However, batch jobs process operations of different types, so
          # the xsi_type must always be explicitly defined for these
          # operations.
          'xsi_type': 'CampaignCriterionOperation',
          'operand': {
              'campaignId': campaign_operation['operand']['id'],
              'criterion': {
                  'xsi_type': 'Location',
                  'id': locDict[location]
              }
          },
          'operator': 'ADD'
      }
      for campaign_operation in campaign_operations]
  except:
    print('Are you sure you typed the country input correctly?')

  return location_operations


def BuildLanguageCriterionOperations(campaign_operations, language):

  langDict = {'French': 1002, 'Spanish': 1003, 'English': 1000}

  try:
    language_operations = [
      {
          # The xsi_type of the operation can usually be guessed by the API
          # because a given service only handles one type of operation.
          # However, batch jobs process operations of different types, so
          # the xsi_type must always be explicitly defined for these
          # operations.
          'xsi_type': 'CampaignCriterionOperation',
          'operand': {
              'campaignId': campaign_operation['operand']['id'],
              'criterion': {
                  'xsi_type': 'Language',
                  'id': langDict[language]
              }
          },
          'operator': 'ADD'
      }
      for campaign_operation in campaign_operations]
  except:
      print('Are you sure you typed the language input correctly?')

  return language_operations


def BuildCampaignOperations(batch_job_helper,
                            budget_operations, number_of_campaigns, campaign_names):
  """Builds the operations needed to create a new Campaign.

  Note: When the Campaigns are created, they will have a different Id than those
  generated here as a temporary Id. This is just used to identify them in the
  BatchJobService.

  Args:
    batch_job_helper: a BatchJobHelper instance.
    budget_operations: a list containing the operation that will add the budget
      used by these Campaigns.
    number_of_campaigns: an int number defining the number of campaigns to be
      created.

  Returns:
    a list containing the operations to create the desired number of Campaigns.
  """
  # Grab the temporary budgetId to associate with the new Campaigns.
  budget_id = budget_operations[0]['operand']['budgetId']

  campaign_operations = [
      {
          # The xsi_type of the operation can usually be guessed by the API
          # because a given service only handles one type of operation.
          # However, batch jobs process operations of different types, so
          # the xsi_type must always be explicitly defined for these
          # operations.
          'xsi_type': 'CampaignOperation',
          'operand': {
              'name': campaign_names[x],
              # Recommendation: Set the campaign to PAUSED when creating it to
              # stop the ads from immediately serving. Set to ENABLED once
              # you've added targeting and the ads are ready to serve.
              'status': 'PAUSED',
              # This is a temporary Id used by the BatchJobService to identify
              # the Campaigns for operations that require a campaignId.
              'id': batch_job_helper.GetId(),
              'advertisingChannelType': 'SEARCH',
              'trackingUrlTemplate': '{lpurl}?vr_source=google&vr_medium=cpc&vr_campaignid={campaignid}&vr_adgroupid={adgroupid}',
              # Note that only the budgetId is required
              'budget': {
                  'budgetId': budget_id
              },
              'networkSetting': {
                  'targetGoogleSearch': 'true',
                  'targetSearchNetwork': 'false',
                  'targetContentNetwork': 'false',
                  'targetPartnerSearchNetwork': 'false'
              },
              'biddingStrategyConfiguration': {
                  'biddingStrategyType': 'MANUAL_CPC'
              }
          },
          'operator': 'ADD'
      }
      for x in range(number_of_campaigns)]

  return campaign_operations


def BuildAdGroupCriterionOperations(adgroid, keywo):
  """Builds the operations adding a Keyword Criterion to each AdGroup.

  Args:
    adgroup_operations: a list containing the operations that will add AdGroups.
    number_of_keywords: an int defining the number of Keywords to be created.

  Returns:
    a list containing the operations that will create a new Keyword Criterion
    associated with each provided AdGroup.
  """
  criterion_operations = [
      {
          # The xsi_type of the operation can usually be guessed by the API
          # because a given service only handles one type of operation.
          # However, batch jobs process operations of different types, so
          # the xsi_type must always be explicitly defined for these
          # operations.
          'xsi_type': 'AdGroupCriterionOperation',
          'operand': {
              'xsi_type': 'BiddableAdGroupCriterion',
              'adGroupId': adgroid[i],
              'criterion': {
                  'xsi_type': 'Keyword',
                  # Make 50% of keywords invalid to demonstrate error handling.
                  'text': keywo[i],
                  'matchType': 'EXACT'
              }
          },
          'operator': 'ADD'
      }
      for i in range(0, len(adgroid))]

  return criterion_operations


def BuildAdGroupOperations(batch_job_helper,
                           campid, adgnam):
  """Builds the operations adding desired number of AdGroups to given Campaigns.

  Note: When the AdGroups are created, they will have a different Id than those
  generated here as a temporary Id. This is just used to identify them in the
  BatchJobService.

  Args:
    batch_job_helper: a BatchJobHelper instance.
    campaign_operations: a list containing the operations that will add
      Campaigns.
    number_of_adgroups: an int defining the number of AdGroups to be created per
      Campaign.

  Returns:
    a list containing the operations that will add the desired number of
    AdGroups to each of the provided Campaigns.
  """
  adgroup_operations = [
      {
          # The xsi_type of the operation can usually be guessed by the API
          # because a given service only handles one type of operation.
          # However, batch jobs process operations of different types, so
          # the xsi_type must always be explicitly defined for these
          # operations.
          'xsi_type': 'AdGroupOperation',
          'operand': {
              'campaignId': campid[i], # id of the campaign created by function: mainAddCampaigns
              'id': batch_job_helper.GetId(),
              'name': adgnam[i], # ad group that must be created in the above campaign id based on EC table from KL
              'biddingStrategyConfiguration': {
                  'bids': [
                      {
                          'xsi_type': 'CpcBid',
                          'bid': {
                              'microAmount': 50000 # default max CPC ad group bid: 0.05
                          }
                      }
                  ]
              }
          },
          'operator': 'ADD'
      }
      for i in range(0,len(adgnam))]

  return adgroup_operations


def BuildAdGroupAdOperations(adg_ids, origin, dest, url, head1, desc1, head2, desc2):
  """Builds the operations adding an ExpandedTextAd to each AdGroup.

  Args:
    adgroup_operations: a list containing the operations that will add AdGroups.

  Returns:
    a list containing the operations that will create a new ExpandedTextAd for
    each of the provided AdGroups.
  """

  adgroup_ad_operations = [
      {
          # The xsi_type of the operation can usually be guessed by the API
          # because a given service only handles one type of operation.
          # However, batch jobs process operations of different types, so
          # the xsi_type must always be explicitly defined for these
          # operations.
          'xsi_type': 'AdGroupAdOperation',
          'operand': {
              'adGroupId': adg_ids[i],
              'ad': {
                  'xsi_type': 'ExpandedTextAd',
                  'headlinePart1': head1[i],
                  'headlinePart2': head2,
                  'description': desc1[i],
                  'description2': desc2,
                  'path1': origin[i] if len(origin[i])<15 else None,
                  'path2': dest[i] if (len(dest[i])<15 and len(origin[i])<15) else None,
                  'finalUrls': url[i]
              }
          },
          'operator': 'ADD'
      }
      for i in range(0, len(adg_ids))]


  return adgroup_ad_operations


def GetBatchJob(client, batch_job_id):
  """Retrieves the BatchJob with the given id.

  Args:
    client: an instantiated AdWordsClient used to retrieve the BatchJob.
    batch_job_id: a long identifying the BatchJob to be retrieved.
  Returns:
    The BatchJob associated with the given id.
  """
  batch_job_service = client.GetService('BatchJobService', 'v201809')

  selector = {
      'fields': ['Id', 'Status', 'DownloadUrl'],
      'predicates': [
          {
              'field': 'Id',
              'operator': 'EQUALS',
              'values': [batch_job_id]
          }
      ]
  }

  return batch_job_service.get(selector)['entries'][0]


def GetBatchJobDownloadUrlWhenReady(client, batch_job_id,
                                    max_poll_attempts=MAX_POLL_ATTEMPTS):
  """Retrieves the downloadUrl when the BatchJob is complete.

  Args:
    client: an instantiated AdWordsClient used to poll the BatchJob.
    batch_job_id: a long identifying the BatchJob to be polled.
    max_poll_attempts: an int defining the number of times the BatchJob will be
      checked to determine whether it has completed.

  Returns:
    A str containing the downloadUrl of the completed BatchJob.

  Raises:
    Exception: If the BatchJob hasn't finished after the maximum poll attempts
      have been made.
  """
  batch_job = GetBatchJob(client, batch_job_id)
  poll_attempt = 0
  while (poll_attempt in range(max_poll_attempts) and
         batch_job['status'] in PENDING_STATUSES):
    sleep_interval = (30 * (2 ** poll_attempt) +
                      (random.randint(0, 10000) / 1000))
    print('Batch Job not ready, sleeping for %s seconds.' % sleep_interval)
    time.sleep(sleep_interval)
    batch_job = GetBatchJob(client, batch_job_id)
    print(batch_job)
    poll_attempt += 1

  if batch_job['status'] == 'DONE': # this fixes NoneType attribute error to downloadUrl
    if 'downloadUrl' in batch_job:
      url = batch_job['downloadUrl']['url']
      print('Batch Job with Id "%s", Status "%s", and DownloadUrl "%s" ready.'
            % (batch_job['id'], batch_job['status'], url))
      return url
  raise Exception('Batch Job not finished downloading. Try checking later.')


def PrintResponse(batch_job_helper, response_xml):
  """Prints the BatchJobService response.

  Args:
    batch_job_helper: a BatchJobHelper instance.
    response_xml: a string containing a response from the BatchJobService.
  """
  response = batch_job_helper.ParseResponse(response_xml)

  if 'rval' in response['mutateResponse']:
    for data in response['mutateResponse']['rval']:
      if 'errorList' in data:
        print('Operation %s - FAILURE:' % data['index'])
        print('\terrorType=%s' % data['errorList']['errors']['ApiError.Type'])
        print('\ttrigger=%s' % data['errorList']['errors']['trigger'])
        print('\terrorString=%s' % data['errorList']['errors']['errorString'])
        print('\tfieldPath=%s' % data['errorList']['errors']['fieldPath'])
        # following command was excluded because it broke the code execution when
        # policy violation of keyword was found. See relevant forum thread below:
        # https://groups.google.com/forum/#!topic/adwords-api/-FCXoeSTAK0
        #print('\treason=%s' % data['errorList']['errors']['reason'])
      if 'result' in data:
        print('Operation %s - SUCCESS.' % data['index'])
  print('\n')






if __name__ == '__main__':
  # Initialize client object.
  adwords_client = adwords.AdWordsClient.LoadFromStorage()

  # Create campaigns based on EC structure (Entities to Create temporary data table from Keyword Library)
  mainAddCampaigns(adwords_client, NUMBER_OF_CAMPAIGNS_TO_ADD, camp_nam, country, language)

  # Get campaigns created by function: mainAddCampaigns
  camp_dict = mainGetCampaigns(adwords_client)
  camp_ids = list(camp_dict.values())
  adgs_to_camp = []
  campid_to_adg = []
  adg_campid = {}
  # keep in list the campaign id that matches the ad group name
  for adg in range(0, len(adgs_nam)):
      adgs_to_camp.append(adgs_nam[adg].split('___')[0])
      campid_to_adg.append(camp_dict[adgs_to_camp[adg]])
      # keep the pairs also in a dictionary
      adg_campid[adgs_nam[adg]] = campid_to_adg[adg]

  # Create the adgroups in each campaign based on EC structure
  mainAddAdGroups(adwords_client, campid_to_adg, adgs_nam)

  # Get ad group names and generated ids and map the keywords to be created in them
  # based on the EC structure
  adg_dict = {}
  it = 0
  for i in range(0, len(camp_ids)):
      if it == 0:
          adg_nam_id = mainGetAdGroups(adwords_client, camp_ids[i], adg_dict)
          it += 1
      else:
          adg_nam_id = mainGetAdGroups(adwords_client, camp_ids[i], adg_nam_id)
          it += 1
  adg_kew['adg_id'] = adg_kew['adg_name'].map(adg_nam_id)
  keywords = adg_kew['keyword'].to_list()
  adg_ids = adg_kew['adg_id'].to_list()

  # Create the adgroup criteria (keywords) in each ad group based on EC structure
  mainAddAdGroupsKeywords(adwords_client, adg_ids, keywords)

  # Build a csv with info for ad texts
  theme = []
  route = []
  orig = []
  dest = []
  tranTy = []
  for i in range(0, len(adg_kew['adg_name'])):
      th = adg_kew.iloc[i, 0].split('__')[1]
      th = th.replace('_', ' ').capitalize()
      rt = adg_kew.iloc[i, 0].split('___')[1]
      origin = rt.split('-')[0].capitalize()
      origin = " ".join([
          word.capitalize()
          for word in origin.split("_")
      ])
      destin = rt.split('-')[1].capitalize()
      destin = " ".join([
          word.capitalize()
          for word in destin.split("_")
      ])
      theme.append(th)
      route.append(rt)
      orig.append(origin)
      dest.append(destin)
      tranTy.append(transType.capitalize())
  adg_kew['theme'] = theme
  adg_kew['origin'] = orig
  adg_kew['destination'] = dest
  adg_kew['trans_type'] = tranTy
  adg_ids = adg_kew['adg_id'].to_list()
  urls = adg_kew['url'].to_list()



  # Let the user define some parts of the text ad
  print('Now please take the time to help the script create the ad text')
  print('\n')
  head2 = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
  desc = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
  desc2 = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' \
          'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' \
          'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
  while len(head2) > 30:
      head2 = input('Please provide the second headline (character limit: 30): ')
      print('You provided a second headline with character length equal to {}'.format(len(head2)))
  while len(desc) > 30:
      desc = input('Please provide a phrase to be added in description 1 (character limit: 30): ')
      print('You provided a phrase to be added in description 1 with character length equal to {}'.format(len(desc)))
  while len(desc2) > 90:
      desc2 = input('Please provide the second description (character limit: 90): ')
      print('You provided a second description with character length equal to {}'.format(len(desc2)))
      print('\n')

  # Check if headline 1 is too long and keep result in csv
  headline1_long = []
  for i in range(0, len(adg_kew['adg_name'])):
      # account in len 1 for one space between theme and origin
      # and 3 for " = " between origin and destination
      head1_check = len(adg_kew['theme'].iloc[i]) + len(adg_kew['origin'].iloc[i]) + len(adg_kew['destination'].iloc[i]) + 4
      if head1_check > 30:
          headline1_long.append('true')
      else:
          headline1_long.append('false')
  adg_kew['head1_long'] = headline1_long

  # Check if description 1 is too long and keep result in csv
  description1_long = []
  for i in range(0, len(adg_kew['adg_name'])):
      # account in len 4 for spaces between theme, origin and destination
      # and 3 for " | " between destination and description
      desc1_check = len(adg_kew['theme'].iloc[i]) + len(adg_kew['origin'].iloc[i]) + len(adg_kew['destination'].iloc[i]) + len(desc) + 4 + 3
      if desc1_check > 90:
          description1_long.append('true')
      else:
          description1_long.append('false')
  adg_kew['desc1_long'] = description1_long
  adg_kew.to_csv('KeywordMapping.csv')

  adg_ads = pd.read_csv('KeywordMapping.csv')
  adg_ads = adg_ads.drop_duplicates(subset='adg_name', keep="first")
  adg_ads = adg_ads.iloc[:,1:]
  adg_ads_ids = adg_ads['adg_id'].tolist()
  adg_ads_origin = adg_ads['origin'].tolist()
  adg_ads_dest = adg_ads['destination'].tolist()
  adg_ads_url = adg_ads['url'].tolist()
  adg_ads_theme = adg_ads['theme'].tolist()
  prot_urls = adg_ads_url

  # Provide alternative headline 1 if existing is too long
  final_head1 = []
  for i in range(0, len(adg_ads['adg_name'])):
      if adg_ads['head1_long'].iloc[i] == True:
          head1_text = transType.capitalize() + ' ' + adg_ads_origin[i] + ' - ' + adg_ads_dest[i]
          if len(head1_text) > 30:
              head1_text = transType.capitalize() + ' to ' + adg_ads_dest[i]
              if len(head1_text) > 30:
                  head1_text = adg_ads_origin[i] + ' - ' + adg_ads_dest[i]
                  if len(head1_text) > 30:
                      w1 = adg_ads_origin[i]
                      w2 = adg_ads_dest[i]
                      head1_text = w1.split('_')[0] + ' - ' + w2.split('_')[0]
          final_head1.append(head1_text)
      else:
          head1_text = adg_ads_theme[i] + ' ' + adg_ads_origin[i] + ' - ' + adg_ads_dest[i]
          final_head1.append(head1_text)

  # Provide alternative description 1 if existing is too long
  final_desc1 = []
  for i in range(0, len(adg_ads['adg_name'])):
      if adg_ads['desc1_long'].iloc[i] == True:
          desc1_text = desc
          final_desc1.append(desc1_text)
      else:
          desc1_text = adg_ads_theme[i] + ' ' + adg_ads_origin[i] + ' - ' + adg_ads_dest[i] + ' | ' + desc
          final_desc1.append(desc1_text)

  adg_ads['head1'] = final_head1
  adg_ads['desc1'] = final_desc1
  adg_ads.to_csv('textAdMapping.csv')

  # Create text ad in each ad group based on EC structure
  mainAddAdCopies(adwords_client, adg_ads_ids, adg_ads_origin, adg_ads_dest, prot_urls, final_head1, final_desc1, head2, desc2)
