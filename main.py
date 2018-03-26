#==============================================================================
# Zoho Writer
#==============================================================================

  
#============================ Import libraries ================================
import pandas as pd
import logging
import logging_gelf.formatters
import logging_gelf.handlers
import sys
import numpy as np
import zoho.ApiClient as zohoApi
import os
from keboola import docker


GIT_VERSION = '1.0.6.2'

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

CONST_MAX_CHUNK_SIZE = 100

KEY_PAR_BASEURL = "baseUrl"
KEY_PAR_ACCOUNTS_URL = "accountsUrl"
KEY_PAR_MODULES = "modules"
KEY_PAR_RELATED_LISTS = "relatedLists"

KEY_PAR_MOD_NAME = "moduleName"
KEY_PAR_TABLE_NAME = "tableName"
KEY_PAR_MOD_DUP_CHECK = "moduleDupCheckCol"
# # rel lists par
KEY_PAR_REL_GET_FROM_INPUT = "getFromModuleInput"
KEY_PAR_REL_MOD_NAME = "relatedModuleName"


MANDATORY_PARAMS = [KEY_PAR_BASEURL, KEY_PAR_ACCOUNTS_URL]


#==============================================================================



def validateConfig(dockerConfig):
    parameters = dockerConfig.get_parameters()
    for field in MANDATORY_PARAMS:
        if not parameters[field]:
            raise Exception('Missing mandatory configuration field: ' + field)



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
    cfgPars = cfg.get_parameters()
    refresh_token = cfg.get_oauthapi_data().get('refresh_token')
    client_id = cfg.get_oauthapi_appkey()
    client_secret = cfg.get_oauthapi_appsecret()
   
    if not refresh_token :
        raise Exception ("Refresh Token not provided!")
    elif not client_id:
        raise Exception ("Client ID not provided!.")
    elif not client_secret:
        raise Exception ("Client Secret not provided!.")

    oAuthProps = {'client_id' : client_id,
            'client_secret' : client_secret,
            'redirect_uri' : REDIRECT_URL,
            'token_persistence_path' : TOKEN_FILE_PATH,
            'access_type' : 'offline',
            'accounts_url':cfgPars.get(KEY_PAR_ACCOUNTS_URL)}
    # #general props
    
    configProps = {'apiBaseUrl' : cfgPars.get(KEY_PAR_BASEURL),
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
        logging.warning("Some " + modName + " module ids were not found in " + objName + " input table for " + modName 
                        + ":"+ objName + " relation! \n" + invalidModuleIds.to_string())
        
    if invalidObjectIds is not None and not invalidObjectIds.empty:
        logging.warning("Some " + objName + " object ids were not found in " + modName + " input table for " + modName 
                        + ":" + objName + " relation! \n" + invalidObjectIds.to_string())
        
def checkIfContansRel(relation, moduleResults):
    modName = relation.get(KEY_PAR_MOD_NAME)
    relModName = relation.get(KEY_PAR_REL_MOD_NAME)
    
    if not any(modName in d for d in moduleResults):
        logging.error("Cannot update relation: " + modName + ":" + relModName + " There is no module data imported for module " 
                      + modName + ". Please make sure you imported all the related records when choosing GetFromModuleInput option!")
        sys.exit(1)
    if not any(relModName in d for d in moduleResults):
        logging.error("Cannot update relation: " + modName + ":" + relModName + " There is no module data imported for module " 
                      + relModName + ". Please make sure you imported all the related records when choosing GetFromModuleInput option!")
        sys.exit(1)
    return moduleResults[modName], moduleResults[relModName]

        
def updateRelations(rel, relDf, moduleResults):
        modRelsData = None
        modName = rel.get(KEY_PAR_MOD_NAME)
        relModName = rel.get(KEY_PAR_REL_MOD_NAME)
        if rel.get(KEY_PAR_REL_GET_FROM_INPUT):
            modRes, relModRes = checkIfContansRel(rel, moduleResults)            
            modRelsData, notFoundModuleObjects, notFoundRelObjects = buildModuleRels(relDf, modRes, relModRes)
            checkRelBuildResults(notFoundModuleObjects, modName, notFoundRelObjects, relModName)
        else:
            modRelsData = relDf
        
        if modRelsData is not None or not modRelsData.empty:
           zcrmClient.UpdateRelations(modName, relModName, modRelsData)
        else:
            logging.warn("No " + zcrmClient.KEY_CAMPAIGNS_MOD_NAME + " module relations were found and updated!")

def validateModuleFields(modName, fields):
    invalidFields = zcrmClient.validateModuleFieldNames(modName,fields)
    if invalidFields:
        logging.warning("Some fields defined in the " + modName + " module input table are invalid! \n" + str(invalidFields))
        

def index_marks(nrows, chunk_size):
    return range(1 * chunk_size, (nrows // chunk_size + 1) * chunk_size, chunk_size)

def upsertRecordsInChunks(mdNam, moduleDf, dupCheckField):
    indices = index_marks(moduleDf.shape[0], CONST_MAX_CHUNK_SIZE)
    chunks =  np.split(moduleDf, indices)
    modResultIdsAll = None
    modFailedRecordsAll = None
    for chunk in chunks:
        modResultIds, modFailedRecords = zcrmClient.Upsert(mdNam, chunk, dupCheckField);
        if modResultIdsAll is None and modFailedRecordsAll is None:
            modResultIdsAll = modResultIds
            modFailedRecordsAll = modFailedRecords
        else:
            modResultIdsAll = modResultIdsAll.append(modResultIds)
            modFailedRecordsAll = modFailedRecordsAll.append(modFailedRecords)
    return modResultIdsAll, modFailedRecordsAll
#==============================================================================


#============================== Write data ====================================  
cfg = docker.Config(os.environ['KBC_DATADIR'])
params = cfg.get_parameters()
try:
    validateConfig(cfg)
except Exception as e:
    logging.error(str(e))
    sys.exit(1)

setLogging()
logging.info("Running version " + GIT_VERSION)
try:
    zcrmClient = initClient(cfg)
except Exception as e:
    logging.error("Failed to init client" + str(e))
    sys.exit(1)


modules = params.get(KEY_PAR_MODULES)
moduleResults = {}
moduleFailedRecords = {}

logging.info('Uploading modules...')
if not modules:
    logging.info("No modules specified!")

try:
    for module in modules:
        moduleTab = getTable(module.get(KEY_PAR_TABLE_NAME), cfg)
        mdNam = module.get(KEY_PAR_MOD_NAME)
        logging.info("Uploading "+mdNam+ " module data..")
        if moduleTab is not None:            
            moduleDf = pd.read_csv(moduleTab.get('full_path'), dtype=str)
            validateModuleFields(mdNam, moduleDf.columns.values.tolist())
            
           
            modResultIds, modFailedRecords =  upsertRecordsInChunks(mdNam, moduleDf, module.get(KEY_PAR_MOD_DUP_CHECK));
            moduleResults[mdNam] = modResultIds
            moduleFailedRecords[mdNam] = modFailedRecords
        else:
            logging.error("Specified table name: " + module.get(KEY_PAR_TABLE_NAME) + " not found in input mapping for module: " + mdNam)
            sys.exit(0)
except Exception as e:
    logging.error("Failed to upload modules!" + str(e))
    sys.exit(1)


relations = params.get(KEY_PAR_RELATED_LISTS)
try:
    for rel in relations:
        relTab = getTable(rel.get(KEY_PAR_TABLE_NAME), cfg) 
        moduleName = rel.get(KEY_PAR_MOD_NAME)
        relModuleName = rel.get(KEY_PAR_REL_MOD_NAME)
        logging.info("Uploading "+moduleName+":"+ relModuleName + " module relations..")
        if relTab:
            relDf = pd.read_csv(relTab.get('full_path'), dtype=str)        
            updateRelations(rel, relDf, moduleResults)
except Exception as e:
    logging.error("Failed to upload module relations!" + str(e))
    sys.exit(1)

logging.info("Write finished successfully!")








#==============================================================================
