#==============================================================================
# Zoho Writer
#==============================================================================

  
#============================ Import libraries ================================
import pandas as pd
import logging
import logging_gelf.formatters
import logging_gelf.handlers
import zoho.ApiClient as zohoApi
import os
from keboola import docker



# # Config Keys
KEY_CONTACT_RELATION = 'contactRelations'

DATA_PATH = os.environ['KBC_DATADIR']
REDIRECT_URL = "https://syrup.keboola.com/oauth-v2/authorize/esnerda.wr-zoho-crm/callback"
TOKEN_FILE_PATH = os.path.join(DATA_PATH, 'temp', 'tokens')
# just for storing the keys
KBC_USER_EMAIL = 'zoho@keboola.com'

CAMPAIGN_TABLE = "campaigns.csv"
CONTACT_TABLE = "contacts.csv"
ACCOUNTS_TABLE = "accounts.csv"
CAMPAIGN_REL_TABLE = "campaignRelations.csv"
ACCOUNT_REL_TABLE = "accountRelations.csv"





#==============================================================================


#================================= Logging ====================================

def setLogging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt="%Y-%m-%d %H:%M:%S")
    
    logger = logging.getLogger()
    # logging_gelf_handler = logging_gelf.handlers.GELFTCPSocketHandler(
    #    host=os.getenv('KBC_LOGGER_ADDR'),
    #    port=int(os.getenv('KBC_LOGGER_PORT'))
    #    )
    # logging_gelf_handler.setFormatter(logging_gelf.formatters.GELFFormatter(null_character=True))
    # logger.addHandler(logging_gelf_handler)
    
    # removes the initial stdout logging
    # logger.removeHandler(logger.handlers[0])
    return logger
#==============================================================================


#============================= Initialise app =================================

def initClient(cfg):
    refresh_token = cfg.get_oauthapi_data().get('refresh_token')
    client_id = cfg.get_oauthapi_appkey()
    client_secret = cfg.get_oauthapi_appsecret()
   
    if refresh_token == '' or client_id == '' or client_secret == '':
        raise Exception ("Please enter your Client ID, Client Secret and Refresh Token.")

    oAuthProps = {'client_id' : client_id,
            'client_secret' : client_secret,
            'redirect_uri' : REDIRECT_URL,
            'token_persistence_path' : TOKEN_FILE_PATH,
            'access_type' : 'offline',
            'accounts_url':'https://accounts.zoho.eu'}    
    # #general props
    cfgPars = cfg.get_parameters()
    configProps = {'apiBaseUrl' : cfgPars.get("baseUrl"),
            'apiVersion' : 'v2',
            'currentUserEmail' : KBC_USER_EMAIL,
            'token_persistence_path' : TOKEN_FILE_PATH,
            'applicationLogFilePath':'',
            'sandbox':'false'}
    zcrmClient = zohoApi.ApiClient(oAuthProps, configProps, logging)
    zcrmClient.initClient(refresh_token)
    
    return zcrmClient
    




#=========================== GET list of tables ===============================

def getTable(tableName, cfg):
    inTables = cfg.get_input_tables()
    for table in inTables:
        if table.get('destination') == tableName:
            return table
        
    return None    


#==============================================================================


#============================= functions =================================


def buildModuleRels(relRecords, moduleObjResultIds, relObjResultIds):
    if moduleObjResultIds is None or moduleObjResultIds.empty:
        logging.warn("There is no module objects updated! Can't build relations")
        return None
    if relObjResultIds is None or relObjResultIds.empty:
        logging.warn("There is no Contact objects updated! Can't build relations")
        return None
    

    relObjResultIds = relObjResultIds.applymap(str)
    moduleObjResultIds = moduleObjResultIds.applymap(str)
    
    relObjResultIds.set_index(zohoApi.ApiClient.KEY_RES_UID, inplace=True, drop=False)
    moduleObjResultIds.set_index(zohoApi.ApiClient.KEY_RES_UID, inplace=True, drop=False)
    relObjResultIds.drop_duplicates(inplace=True)
    moduleObjResultIds.drop_duplicates(inplace=True)
    
    # join in module object Ids
    res = relRecords.join(moduleObjResultIds, on=zohoApi.ApiClient.KEY_REL_MODULE_OBJ_ID)
    notFoundModuleObjects = res[pd.isnull(res[zohoApi.ApiClient.KEY_RES_UID])][zohoApi.ApiClient.KEY_REL_MODULE_OBJ_ID]
    
    # assign module object id to module_id
    res[zohoApi.ApiClient.KEY_REL_MODULE_OBJ_ID] = res[zohoApi.ApiClient.KEY_ENTITY_ID]
    res.drop(columns=moduleObjResultIds.columns.values.tolist(), inplace=True)
    # join in module ids
    res = res.join(relObjResultIds, on=zohoApi.ApiClient.KEY_REL_ENT_ID)
    notFoundRelObjects = res[pd.isnull(res[zohoApi.ApiClient.KEY_RES_UID])][zohoApi.ApiClient.KEY_REL_ENT_ID]
    
    # assign related object id to ent_id
    res[zohoApi.ApiClient.KEY_REL_ENT_ID] = res[zohoApi.ApiClient.KEY_ENTITY_ID]
    # drop NaN values
    res = res[pd.notnull(res[zohoApi.ApiClient.KEY_REL_MODULE_OBJ_ID]) & pd.notnull(res[zohoApi.ApiClient.KEY_REL_ENT_ID])][['module_obj_id', 'ent_id']]
    return res, notFoundModuleObjects, notFoundRelObjects

def checkRelBuildResults(invalidModuleIds, modName, invalidObjectIds, objName):
    if invalidModuleIds is not None and not invalidModuleIds.empty:
        logging.warning("Some " + modName + " module ids were not found in " + objName + " input table! \n"+invalidModuleIds.to_string())
        
    if invalidObjectIds is not None and not invalidObjectIds.empty:
        logging.warning("Some " + objName + " object ids were not found in " + modName + " input table! \n"+invalidObjectIds.to_string())
        
        
    
#==============================================================================


#============================== Write data ====================================  
cfg = docker.Config(os.environ['KBC_DATADIR'])
params = cfg.get_parameters()


setLogging()
zcrmClient = initClient(cfg)

contacts = getTable(CONTACT_TABLE, cfg)
campaigns = getTable(CAMPAIGN_TABLE, cfg)
accounts = getTable(ACCOUNTS_TABLE, cfg)

# #relations
campaignRels = getTable(CAMPAIGN_REL_TABLE, cfg)
accountnRels = getTable(ACCOUNT_REL_TABLE, cfg)

# # upsert campaigns
if campaigns:
    campaignsDf = pd.read_csv(campaigns.get('full_path'), dtype=str)
    campaignResultIds, campaignFailedRecords = zcrmClient.Upsert(zcrmClient.KEY_CAMPAIGNS_MOD_NAME, campaignsDf, zcrmClient.DEF_CAMPAIGN_DUP_CHECK_FIELD);

# # upsert accounts
accountsDf = None
if accounts:
    accountsDf = pd.read_csv(accounts.get('full_path'), dtype=str)

accountsResultIds, accountsFailedRecords = zcrmClient.Upsert(zcrmClient.KEY_ACCOUNTS_MOD_NAME, accountsDf, zcrmClient.DEF_ACCOUNT_DUP_CHECK_FIELD);

# # upsert contacts
contactsDf = None
if contacts:
    contactsDf = pd.read_csv(contacts.get('full_path'), dtype=str)

contactResultIds, contactFailedRecords = zcrmClient.Upsert(zcrmClient.KEY_CONTACTS_MOD_NAME, contactsDf, zcrmClient.DEF_CONTACT_DUP_CHECK_FIELD);


# # update Campaign relations if any
relDatasets = params.get(KEY_CONTACT_RELATION)
modRelsData = None
if campaignRels:
    campaignRelsDf = pd.read_csv(campaignRels.get('full_path'), dtype=str)    
    if zcrmClient.KEY_CAMPAIGNS_MOD_NAME in relDatasets:
        modRelsData, notFoundModuleObjects, notFoundRelObjects = buildModuleRels(campaignRelsDf, campaignResultIds, contactResultIds)
        checkRelBuildResults(notFoundModuleObjects, zcrmClient.KEY_CAMPAIGNS_MOD_NAME, notFoundRelObjects, zcrmClient.KEY_CONTACTS_MOD_NAME)
    else:
        modRelsData = campaignRelsDf
    
    if modRelsData is not None or not modRelsData.empty:
       zcrmClient.UpdateRelations(zcrmClient.KEY_CAMPAIGNS_MOD_NAME, zcrmClient.KEY_CONTACTS_MOD_NAME, zcrmClient.DEF_CAMPAIGN_DUP_CHECK_FIELD, modRelsData)
    else:
        logging.warn("No " + zcrmClient.KEY_CAMPAIGNS_MOD_NAME + " module relations were found and updated!")
    
# # update Account relations if any

if accountnRels:
    modRelsData = None
    accountRelsDf = pd.read_csv(accountnRels.get('full_path'), dtype=str)
    
    modRelsData = None
    if zcrmClient.KEY_ACCOUNTS_MOD_NAME in relDatasets:
        modRelsData = buildModuleRels(accountRelsDf, accountsResultIds, contactResultIds)
        checkRelBuildResults(notFoundModuleObjects, zcrmClient.KEY_ACCOUNTS_MOD_NAME, notFoundRelObjects, zcrmClient.KEY_CONTACTS_MOD_NAME)
    else:
        modRelsData = accountnRels
     
    if modRelsData is not None or not modRelsData.empty:       
        zcrmClient.UpdateRelations(zcrmClient.KEY_ACCOUNTS_MOD_NAME, zcrmClient.KEY_CONTACTS_MOD_NAME, zcrmClient.DEF_ACCOUNT_DUP_CHECK_FIELD, modRelsData)
    else:
        logging.warn("No " + zcrmClient.KEY_CAMPAIGNS_MOD_NAME + " module relations were found and updated!")


logging.info("Write finished successfully!")








#==============================================================================
