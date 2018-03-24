'''
Created on 23. 3. 2018

@author: David Esner
'''
import os
import zcrmsdk
import pandas as pd

class ApiClient(object):

    # CONSTANTS
    CAMPAING_JOIN_UID = "relUid"
    KEY_RES_UID = 'uiD'
    KEY_ENTITY_ID = 'entId'
    
    KEY_ACCOUNTS_MOD_NAME = 'Accounts'
    KEY_CAMPAIGNS_MOD_NAME = 'Campaigns'
    KEY_CONTACTS_MOD_NAME = 'Contacts'

    KEY_REL_MODULE_OBJ_ID = 'module_obj_id'
    KEY_REL_ENT_ID = 'ent_id'
    
    ###hardcoded params
    DEF_CAMPAIGN_DUP_CHECK_FIELD = "Campaign_Name"
    DEF_CONTACT_DUP_CHECK_FIELD = "Email"
    DEF_ACCOUNT_DUP_CHECK_FIELD = "Account_Name"
    
    
    '''
    Provide ZCRM SDK configuration files:
    oAuthProperties:
    e.g.:
    {'client_id' : client_id, 
            'client_secret' : client_secret, 
            'redirect_uri' : REDIRECT_URL,
            'token_persistence_path' : TOKEN_FILE_PATH,
            'access_type' : 'offline',
            'accounts_url':'https://accounts.zoho.eu'}
            
    configProps:
    e.g.
    {'apiBaseUrl' : "www.zohoapis.eu", 
            'apiVersion' : 'v2', 
            'currentUserEmail' : KBC_USER_EMAIL,
            'token_persistence_path' : TOKEN_FILE_PATH,
            'applicationLogFilePath':'',
            'sandbox':'false'}
    '''    
    def __init__(self, oauthProperties, configProps, logging):
        self.__setProperties(oauthProperties, configProps)
        self.CLIENT_EMAIL = configProps.get('currentUserEmail')
        self.logging = logging


    #============================= Init client ==================================
    def __writePropertiesToFile(self, filePath, props):
        file = open(filePath, 'w') 
        for key, val in props.items():
            file.write(key + "=" + val)    
            file.write("\n") 
        file.close() 
    
    def __setProperties(self, oauthProperties, configProps):      
        
        resources_path = os.path.join(os.path.dirname(zcrmsdk.__file__), 'resources', 'oauth_configuration.properties')        
        self.__writePropertiesToFile(resources_path, oauthProperties)
        
        # #general props            
        generalPropsPath = os.path.join(os.path.dirname(zcrmsdk.__file__), 'resources', 'configuration.properties')
        self.__writePropertiesToFile(generalPropsPath, configProps)
         
         
        # # create token persist file
        tokenFilePath = configProps.get('token_persistence_path')
        if not os.path.exists(tokenFilePath):
            os.makedirs(tokenFilePath)
        open(os.path.join(tokenFilePath, 'zcrm_oauthtokens.pkl'), 'a').close()


    def initClient(self, refreshToken):
        zcrmClient = zcrmsdk.ZCRMRestClient()
        zcrmClient.initialize()
        
        oauth_client = zcrmsdk.ZohoOAuth().get_client_instance()
        oauth_client.generate_access_token_from_refresh_token(refreshToken, self.CLIENT_EMAIL) 
        return zcrmClient
        
    
    
    #==============================================================================
    
    
    #============================= Writer methods ==================================
    def buildRecords(self, module, df):
        record_ins_list = list()
        fieldNames = df.columns.values.tolist()
        for index, row in df.iterrows():
            record = zcrmsdk.ZCRMRecord.get_instance(module)
            for column in fieldNames:            
                record.set_field_value(column, row[column])
            record_ins_list.append(record)
        return record_ins_list
    
    def geEntityIds(self, entityResponses, uniqueFieldName):
        data = [] 
        for entity_response in entityResponses:  
            entId = ''
            uid = ''
            if entity_response.status == 'success':            
                entId = entity_response.data.entity_id 
                uid = entity_response.data.field_data.get(uniqueFieldName)
            status = entity_response.status  
            message = entity_response.message
            data.append([entId, uid, status, message])   
        res = pd.DataFrame(data, columns=[self.KEY_ENTITY_ID, self.KEY_RES_UID, 'status', 'message']) 
        return res


    def checkResultStatus(self, resultIds):
        failedRecords = resultIds[(resultIds.status <> 'success')]
        if not failedRecords.empty:
            self.logging.warn("Some records failed to be uploaded: " + failedRecords.to_string())
        return failedRecords
    
    # Insert or update records
    def Upsert(self, module, df, uniqueFieldName):
            if df is None:
                return None, None
            recordLists = self.buildRecords(module, df)
            resp = zcrmsdk.ZCRMModule.get_instance(module).upsert_records(recordLists)
            
            if 200 < resp.status_code > 299:
                raise Exception("Api call failed with code: " + resp.code + ", message:" + resp.message, resp.details)    
            else:
                resultIds = self.geEntityIds(resp.bulk_entity_response, uniqueFieldName)
                failedRecords = self.checkResultStatus(resultIds)
                return resultIds, failedRecords
    
    # Insert or update records
    def UpdateRelations(self, module, relType, moduleUID, df):
        if df is None:
            return None, None
        failedRecords = []
        for index, row in df.iterrows():
            record=zcrmsdk.ZCRMRecord.get_instance(module,row[self.KEY_REL_MODULE_OBJ_ID]) #module API Name, entityId
            junction_record=zcrmsdk.ZCRMJunctionRecord.get_instance(relType, row[self.KEY_REL_ENT_ID]) #module API Name, entityId
            resp=record.add_relation(junction_record)            
            if 200 > resp.status_code > 299:
                raise Exception("Api call failed with code: " + resp.code + ", message:" + resp.message, resp.details)    
            else:               
                failedRecords.append([row[self.KEY_REL_MODULE_OBJ_ID],row[self.KEY_REL_MODULE_OBJ_ID]])
        return failedRecords







