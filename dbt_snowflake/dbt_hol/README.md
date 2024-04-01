# dbt Core & Snowflake Tutorial Guide
References:
- [Accelerating Data Teams with dbt Core & Snowflake](https://quickstarts.snowflake.com/guide/data_teams_with_dbt_core/index.html#0)

## Table of Contents

## Introduction
### Prerequisites
To complete this tutorial, the following conditions need to be met:
- A snowflake account with `ACCOUNTADMIN` access.
- A good understanding on the Snowflake architecture and Snowflake objects.
- Familiarity with SQL.
- Having the dbt CLI installed. In this tutorial, we will be using the `dbt-snowflake`.

### Objectives
- To leverage data in Snowflake's Data Marketplace.
- To build scalable pipelines using dbt & Snowflake.
- To upload data to Snowflake using `dbt seed`.
- To implement data quality tests using `dbt test`.

## Snowflake Configuration
For this section, you need to follow the steps below to configure the Snowflake environment for this tutorial.

1. Create a Snowflake trial account from this [link](https://signup.snowflake.com/?utm_source=google&utm_medium=paidsearch&utm_campaign=ap-my-en-brand-core-exact&utm_content=go-eta-evg-ss-free-trial&utm_term=c-g-snowflake-e&_bt=562090225371&_bk=snowflake&_bm=e&_bn=g&_bg=125204661502&gclsrc=aw.ds&gad_source=1&gclid=CjwKCAjwtqmwBhBVEiwAL-WAYXMA5rn7cdhaMAmGzWOnNEf384kcSdwulwvz12It1oVI0MrMm8xECBoCsKQQAvD_BwE).
2. Login to the newly created snowflake trial account.
3. Create a new folder by clicking the "Add" button on the right hand side. This folder will be used to store all the related Snowflake worksheets for this tutorial.
![image](/dbt_snowflake/dbt_hol/images/Pasted%20image%2020240321180231.png)
4. Within the folder, create a new worksheet and include the following codes:
```sql
---------------------------------------
-- Create Snowflake user for dbt
---------------------------------------
USE ROLE accountadmin;
CREATE USER IF NOT EXISTS dbt_user
    PASSWORD = 'password'
    LOGIN_NAME = dbt_user
    DISPLAY_NAME = dbt_user
    COMMENT = 'User for handling dbt development and production';
    -- There are other configuration parameters available

---------------------------------------
-- dbt credentials
---------------------------------------
USE ROLE securityadmin;
-- Create new roles for dbt
CREATE OR REPLACE ROLE dbt_dev_role;
CREATE OR REPLACE ROLE dbt_prod_role;

-- Grant the specific roles to the selected users
GRANT ROLE dbt_dev_role,dbt_prod_role TO USER dbt_user;
GRANT ROLE dbt_dev_role,dbt_prod_role TO ROLE sysadmin;

---------------------------------------
-- dbt objects
---------------------------------------
USE ROLE sysadmin;
-- Create multiple types of snowflake virtual warehouse
CREATE OR REPLACE WAREHOUSE dbt_dev_wh WITH 
	WAREHOUSE_SIZE = 'XSMALL' 
	AUTO_SUSPEND = 60 
	AUTO_RESUME = TRUE 
	MIN_CLUSTER_COUNT = 1
	MAX_CLUSTER_COUNT = 1 
	INITIALLY_SUSPENDED = TRUE;

CREATE OR REPLACE WAREHOUSE dbt_dev_heavy_wh WITH 
	WAREHOUSE_SIZE = 'LARGE' 
	AUTO_SUSPEND = 60 
	AUTO_RESUME = TRUE 
	MIN_CLUSTER_COUNT = 1 
	MAX_CLUSTER_COUNT = 1 
	INITIALLY_SUSPENDED = TRUE;
	
CREATE OR REPLACE WAREHOUSE dbt_prod_wh WITH 
	WAREHOUSE_SIZE = 'XSMALL' 
	AUTO_SUSPEND = 60 
	AUTO_RESUME = TRUE 
	MIN_CLUSTER_COUNT = 1 
	MAX_CLUSTER_COUNT = 1 
	INITIALLY_SUSPENDED = TRUE;
	
CREATE OR REPLACE WAREHOUSE dbt_prod_heavy_wh
	WITH WAREHOUSE_SIZE = 'LARGE' 
	AUTO_SUSPEND = 60 
	AUTO_RESUME = TRUE 
	MIN_CLUSTER_COUNT = 1 
	MAX_CLUSTER_COUNT = 1 
	INITIALLY_SUSPENDED = TRUE;

-- Grant the permissions to fully access the warehouse to selected role
GRANT ALL ON WAREHOUSE dbt_dev_wh TO ROLE dbt_dev_role;
GRANT ALL ON WAREHOUSE dbt_dev_heavy_wh TO ROLE dbt_dev_role;
GRANT ALL ON WAREHOUSE dbt_prod_wh TO ROLE dbt_prod_role;
GRANT ALL ON WAREHOUSE dbt_prod_heavy_wh TO ROLE dbt_prod_role;

-- Create new database
CREATE OR REPLACE DATABASE dbt_hol_dev; 
CREATE OR REPLACE DATABASE dbt_hol_prod;

-- Grant the permissions to fully access the database to selected role
GRANT ALL ON DATABASE dbt_hol_dev TO ROLE dbt_dev_role;
GRANT ALL ON DATABASE dbt_hol_prod TO ROLE dbt_prod_role;
GRANT ALL ON ALL SCHEMAS IN DATABASE dbt_hol_dev TO ROLE dbt_dev_role;
GRANT ALL ON ALL SCHEMAS IN DATABASE dbt_hol_prod TO ROLE dbt_prod_role;
```

The functionality of the codes are to:
- Create a user for dbt usage.
- Create a role for dbt development and dbt production in Snowflake.
- Grant the role to the user.
- Create multiple types of Snowflake virtual warehouse.
- Grant the privileges/permissions to the created roles.

***Remarks***: *In this tutorial, full permissions are granted to the roles. However, in reality, it is necessary to grant only the minimal permissions or privileges that allow the tasks to be completed for each role*

### Outcomes
By the end of this part, we should have:
- Two empty databases: "DBT_HOL_DEV" and "DBT_HOL_PROD"
- Each created role being assigned with two different virtual warehouse sizes: "XSMALL" and "LARGE".
- A role for each development and production phase, and one defined user.