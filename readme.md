## ZOHO CRM Writer for KBC
Writer component for Keboola Connection allowing to upsert Modules in ZOHO CRM and relations among them.

## Funcionality

Component is capable of writing two types of data:

###1. Module data

Component takes input tables that needs to contains all fields to be updated for required Zoho module. Note that the column names **must exactly match the Zoho module api names!** These are usually field names separated by `_` For instance `Campaign_Name`. If some field in the source table is not matching the one in Zoho a warning is produced, but the writer does not fail. The process fails only if some of the Zoho required fields are misssing.  

###2. Module relations

This allows to upload relations between modules, i.e. link modules to a parent module list. For example link all `Contacts` with `Campaigns`.

The input table header **must have following structure:** `module_obj_id, ent_id` 
`module_obj_id` should contain parent Zoho ID, for instance of a Campaign
`ent_id` should contain related records Zoho ID, for instance of a Contact

The module relations can work in two regimes:

**Standard mode**

When the `Related records upload mode` parameter is set to `Standard mode`. The component expects the proper Zoho IDs to be present in the table.

**Insert and Update mode** 

In this mode all records specified in the relations have to be present in the Module input data. This is useful in situations when you don't know the Zoho IDs upfront and some of the records do not exist in the Zoho yet. All the Parent objects like Campaings and all the Related objects like Contacts needs to be part of the import in `Modules` section. If they are not, the process will produce a warning.

The content of the input table is in this case different
- the `module_obj_id` should contain duplicate check column of the parent, e.g. `Campaign Name` for campaigns
- the `ent_id` should contain duplicate check column of the related object, e.g. `Email` for contacts


### Configuration parameters

- **Zoho accounts URL** – (REQ) Accounts URL may differ if you use another domain than eu. e.g. `https://accounts.zoho.eu`
- **Zoho base URL** - Accounts URL may differ if you use another domain than eu. e.g. `www.zohoapis.com`
- **Modules to upsert** – (REQ) Modules that will be updated/inserted.
	- **Module name** - ZOHO module name as defined in Zoho. e.g. Contacts. NOTE: (case sensitive!)
	- **Table name** - Input table name that contains module data as defined in the input mapping. e.g. contacts
	- **Module duplicate check column** - Column name which is used as a duplicate check for the given module. For example for `Contacts` it may be `Email`. **Case sensitive!** For more info refer to [dup_check](https://www.zoho.com/crm/help/api/v2/#sys_def_dup_chk_flds) 
- **Relations to upsert** - List of relations to be updated
	- **Module name** - Name of the parent module as defined in Zoho. e.g. `Campaigns`. **NOTE:** (case sensitive!)
	- **Related module name** - Name of the Module which is to be linked to the parent Module. e.g. `Contacts`
	- **Table name** - ZOHO module name as defined in Zoho. e.g. Contacts. NOTE: (case sensitive!)Input table name that contains module data as defined in the input mapping. e.g. campaignContactRels. **Caution:** the columns of that table must contain following fields: `module_obj_id, ent_id`. Where `module_obj_id` => Zoho ID of parent module (e.g. `campaigns`), `ent_id` = Zoho ID of related object (e.g. Contact) 
	- **Related records upload mode** - Flag whether relation table contains Zoho ids, or Duplicate check fields values. If set to `Insert and Update mode`, the writer excepts duplicate check field values instead IDs and also all these values to have matching records in imported Module data. I.e. all Campaing and Contact values specified must be contained within Campaign and Contact data specified in Modules section.
