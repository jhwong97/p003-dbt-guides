# dbt Core & Snowflake Tutorial Guide
References:
- [Accelerating Data Teams with dbt Core & Snowflake](https://quickstarts.snowflake.com/guide/data_teams_with_dbt_core/index.html#0)

## Table of Contents
1. [Introduction](#introduction)
2. [Step 1: Snowflake Configuration](#step-1-snowflake-configuration)
3. [Step 2: DBT Configuration](#step-2-dbt-configuration)
4. [Step 3: Connect to Data Source](#step-3-connect-to-data-sources)

## Introduction
### Architecture and Use Case Overview
In this tutorial, we are going to analyze historical trading performance of a company that has trading desks spread across different regions.

In the original version (Refer to the reference), the tutorial will leverage the datasets of Knoema Economy Data Atlas that is available in Snowflake Data Marketplace. However, the Knoema Economy Data Atlas is no more provided in the data marketplace. Therefore, an alternative - **Financial & Economic Essentials Data from Cybersyn** will be used as a replacement to it.

The source data used is stated below:
- Dataset A: Stock Price History
- Dataset B: FX Rates
- Dataset C: Trading books

The overall tutorial workflow is shown in the image below.
![tutorial_workflow](/dbt_snowflake/dbt_hol/images/tutorial_workflow.png)

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

## Step 1: Snowflake Configuration
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

## Step 2: dbt Configuration
### Initialize the dbt project
For this tutorial, the following steps are required to set up the dbt environment to connect to the Snowflake.
1. Set up an virtual environment and install the `dbt-snowflake==1.7.2` package.
2. After the installation is completed, initialize a new dbt project by running the following commands:
    ```cmd
    dbt init dbt_hol
    ```
3. After initializing the dbt, you are prompted to fill up some information to configure the dbt profile for connecting to your Snowflake account. The whole process is outlined as below:
    -  **Info 1**:
        ```cmd
        01:30:24  Setting up your profile.
        Which database would you like to use?
        [1] bigquery
        [2] postgres
        [3] snowflake

        (Don't see the one you want? https://docs.getdbt.com/docs/available-adapters)

        Enter a number:
        ```
        As in my virtual environment, I've the `dbt-postgres` and `dbt-bigquery` packages installed, and thus additional options for setting up the connection to the bigquery and postgres are available. In this tutorial, we will only select the snowflake or entering the number `3`.

    - **Info 2**:
        ```cmd
        account (https://<this_value>.snowflakecomputing.com): 
        ```
        For this part, you need to replace `<this_value>` with your account value. The account value is in this format `<organization_name>-<account_name>` and you need to replace the `<organization_name>` and `<account_name>` with your own value. You can find the respective value by running this commands in snowflake:
        ```sql
        -- Check your organization name and account name
        SELECT CURRENT_ORGANIZATION_NAME();
        SELECT CURRENT_ACCOUNT_NAME();
        ```

    - **Info 3**:
        ```cmd
        user (dev username):
        ```
        You need to enter your username which is `dbt_user` that created earlier.
    
    - **Info 4**:
        ```cmd
        [1] password
        [2] keypair
        [3] sso
        Desired authentication type option (enter a number):
        ```
        In this section, you are required to select the desired authentication type option. There are multiple options, but we will go for password by entering the number `1` for this tutorial.
    
    - **Info 5**:
        ```cmd
        password (dev password):
        ```
        Enter the password to authenticate the usage of that user which is `password` in this tutorial.
    
    - **Info 6**:
        ```cmd
        role (dev role):
        ```
        Enter the role that bind with the user which is `dbt_dev_role`.

    - **Info 7**:
        ```cmd
        warehouse (warehouse name):
        ```
        Enter the warehouse to be used which is `dbt_dev_wh`.
    
    - **Info 8**:
        ```cmd
        database (default database that dbt will build objects in):
        ```
        Enter the database that dbt will build objects into, which is `dbt_hol_dev`.
    
    - **Info 9**:
        ```cmd
        schema (default schema that dbt will build objects in):
        ```
        Enter the schema that dbt will build objects into, which is `public`.
    
    - **Info 10**:
        ```cmd
        threads (1 or more) [1]:
        ```
        The `threads` refers to the number of concurrent models dbt should build. Set this to a higher number if using a bigger warehouse. Default = 1.
    
    The above steps help to set up the connection between the dbt and the Snowflake objects used in the development environment.

4. In this tutorial, we will be adding one more connection to the Snowflake objects used in the production environment. However it is not possible to add additional connection via the `dbt init dbt_hot` again as it will return the following messages:
    ```cmd
    02:02:06  Running with dbt=1.7.9
    02:02:06  A project called dbt_hol already exists here.
    ```
    - To add the new connection, we need to manually update the configuration in `profiles.yml` which can be found in `~/.dbt/profiles.yml`.
    - Open the file and add the following section:
    ```yml
    dbt_hol:
        target: dev
        outputs:
            dev:
                type: snowflake
                account: <your_snowflake_trial_account>
                user: dbt_user
                password: password
                role: dbt_dev_role
                database: dbt_hol_dev
                warehouse: dbt_dev_wh
                schema: public
                threads: 200
            
            # Manual Update
            prod:
                type: snowflake
                ######## Please replace with your Snowflake account name
                account: <your_snowflake_trial_account>
                user: dbt_user
                ######## Please replace with your Snowflake dbt user password
                password: <mysecretpassword>
                role: dbt_prod_role
                database: dbt_hol_prod
                warehouse: dbt_prod_wh
                schema: public
                threads: 200
    ```

5. Validate the configuration or the connection between dbt and Snowflake. To perform this action, change the directory into the `dbt_hol` folder and then run the following command:
    ```cmd
    dbt debug
    ```

    The expected output in the CLI should look similar to this, confirming that dbt was able to connect to the Snowflake database:
    ```cmd
    03:20:07  Running with dbt=1.7.9
    03:20:07  dbt version: 1.7.9
    03:20:07  python version: 3.11.3
    03:20:07  python path: D:\OneDrive\0_Project\e001-dbt-guides\venv\Scripts\python.exe
    03:20:07  os info: Windows-10-10.0.22631-SP0
    03:20:08  Using profiles dir at C:\Users\Albert\.dbt
    03:20:08  Using profiles.yml file at C:\Users\Albert\.dbt\profiles.yml
    03:20:08  Using dbt_project.yml file at D:\OneDrive\0_Project\e001-dbt-guides\dbt_snowflake\dbt_hol\dbt_project.yml
    03:20:08  adapter type: snowflake
    03:20:08  adapter version: 1.7.2
    03:20:08  Configuration:
    03:20:08    profiles.yml file [OK found and valid]
    03:20:08    dbt_project.yml file [OK found and valid]
    03:20:08  Required dependencies:
    03:20:08   - git [OK found]

    03:20:08  Connection:
    03:20:08    account: <account_name>
    03:20:08    user: dbt_user
    03:20:08    database: dbt_hol_dev
    03:20:08    warehouse: dbt_dev_wh
    03:20:08    role: dbt_dev_role
    03:20:08    schema: public
    03:20:08    authenticator: None
    03:20:08    private_key_path: None
    03:20:08    oauth_client_id: None
    03:20:08    query_tag: None
    03:20:08    client_session_keep_alive: False
    03:20:08    host: None
    03:20:08    port: None
    03:20:08    proxy_host: None
    03:20:08    proxy_port: None
    03:20:08    protocol: None
    03:20:08    connect_retries: 1
    03:20:08    connect_timeout: None
    03:20:08    retry_on_database_errors: False
    03:20:08    retry_all: False
    03:20:08    insecure_mode: False
    03:20:08    reuse_connections: None
    03:20:08  Registered adapter: snowflake=1.7.2
    03:20:09    Connection test: [OK connection ok]

    03:20:09  All checks passed!
    ```
    If the connection failed, you might need to recheck the configurations in the `profiles.yml` file.

6. Finally, let's run the sample models located in the `~/models` that comes with dbt templates by default to validate everything is set up correctly. For this, please run the following command in the dbt root folder:
    ```cmd
    dbt run
    ```

    The expected output is shown below:
    ```cmd
    07:30:14  Running with dbt=1.7.9
    07:30:15  Registered adapter: snowflake=1.7.2
    07:30:16  Unable to do partial parsing because profile has changed
    07:30:17  Found 2 models, 4 tests, 0 sources, 0 exposures, 0 metrics, 430 macros, 0 groups, 0 semantic models
    07:30:17
    07:30:19  Concurrency: 200 threads (target='dev')
    07:30:19
    07:30:19  1 of 2 START sql table model public.my_first_dbt_model ......................... [RUN]
    07:30:21  1 of 2 OK created sql table model public.my_first_dbt_model .................... [SUCCESS 1 in 2.66s]
    07:30:21  2 of 2 START sql view model public.my_second_dbt_model ......................... [RUN]
    07:30:22  2 of 2 OK created sql view model public.my_second_dbt_model .................... [SUCCESS 1 in 0.83s]
    07:30:22
    07:30:22  Finished running 1 table model, 1 view model in 0 hours 0 minutes and 5.07 seconds (5.07s).
    07:30:22  
    07:30:22  Completed successfully
    07:30:22
    07:30:22  Done. PASS=2 WARN=0 ERROR=0 SKIP=0 TOTAL=2
    ```

7. We can then refresh the Snowflake UI to see the newly created table and views as illustrated in the image below.
![image](/dbt_snowflake/dbt_hol/images/Pasted%20image%2020240322153104.png)

## Step 3: Connect to Data Sources
To obtain the data for this tutorial, please follow the steps shown here [Connect to Data Sources](https://quickstarts.snowflake.com/guide/data_teams_with_dbt_core/index.html#4). However, in this tutorial, we will be getting the **Financial & Economic Essentials** data from **Cybersyn** as the Knoema economy data is not available in the current marketplace.

![cybersyn_data_source](/dbt_snowflake/dbt_hol/images/cybersyn_data_source.png)

After getting the data, let's go back to the Snowflake worksheets and then refresh the database browser. We will notice a new shared database called `FINANCIAL__ECONOMIC_ESSENTIALS` is appeared on the list of databases (left-hand side). By expanding the databases,we'll see multiple views under the **CYBERSYN** schema. We'll use two of the views for this tutorial:
- `STOCK_PRICE_TIMESERIES`
- `FX_RATES_TIMESERIES`

![image](/dbt_snowflake/dbt_hol/images/new_shared_databases.png)

After successfully connecting to the shared database, we can try to perform query from the datasets via the Snowflake worksheet.
```sql
-- Query data for the exchange rate
SELECT * 
FROM FINANCIAL__ECONOMIC_ESSENTIALS.CYBERSYN.FX_RATES_TIMESERIES
WHERE "DATE" = '2024-03-21';

-- Query data for the stocks
SELECT *
FROM FINANCIAL__ECONOMIC_ESSENTIALS.CYBERSYN.STOCK_PRICE_TIMESERIES
WHERE
    "DATE" = '2024-03-21' AND
    "TICKER" = 'AAPL';
```

![image](/dbt_snowflake/dbt_hol/images/3_query_result.png)

## Step 4: Building dbt Data Pipelines
For this part, we will be building dbt pipelines for:
- Stock trading history
- Currency exchange rates
- Trading books
- Profit & Loss calculation

Before we start to build the dbt pipelines, we would need to do some simple set up on the dbt project directory.

1. We need to create multiples sub-folders inside the `models` folder from our dbt root directory. The purpose of the sub-folders help us to improve the maintainability of our project. Run the below commands to create the required sub-folders, representing different logical levels in the pipeline:

    ```cmd
    mkdir models/l10_staging
    mkdir models/l20_transform
    mkdir models/l30_mart
    mkdir models/tests
    ```

2. Next, we need to update the `dbt_project.yml` by modifying it to reflect the model structure. This step allows you to configure multiple parameters on the later level (like materialization in this tutorial).

    ```yml
    models:
    dbt_hol:
        # Applies to all files under models/example/
        example:
            materialized: view
            +enabled: false
        l10_staging:
            schema: l10_staging
            materialized: view
        l20_transform:
            schema: l20_transform
            materialized: view
        l30_mart:
            schema: l30_mart
            materialized: view
    ```
    ***Remarks***: A param called **+enabled: false** is added to the *example* section as we won't need to run those sample models. Alternatively, you can just remove the *example* section from the `dbt_project.yml`.

3. **Custom schema naming macros**. By default, [generating a schema name](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/using-custom-schemas) by appending it to the target schema environment name(dev, prod). In this tutorial, we are going to override this macro, making our schema names to look exactly the same between dev and prod databases. For this, let's create a file `macros\call_me_anything_you_want.sql` with the following content:

    ```sql
    {% macro generate_schema_name(custom_schema_name, mode) -%}
        {%- set default_schema = target.schema -%}
        {%- if custom_schema_name is none -%}
            {{default_schema}}
        {%- else -%}
            {{custom_schema_name | trim}}
        {%- endif -%}

    {% macro set_query_tag() -%}
        {% set new_query_tag = model.name %} {# always use model name #}
        {% if new_query_tag %}
            {% set original_query_tag = get_current_query_tag() %}
            {{ log("Setting query_tag to '" ~ new_query_tag ~ "'. Will reset to '" ~ original_query_tag ~ "' after materialization.") }}
            {% do run_query("alter session set query_tag = '{}'".format(new_query_tag)) %}
            {{ return(original_query_tag)}}
        {% endif %}
        {{ return(none)}}
    {% endmacro %}
    ```

    In the code above, there is another macro written in the file called **set_query_tag()**. This one provides the ability to add additional level of transparency by automatically setting Snowflake "query_tag" to the name of the model it associated with.

    So if we go to Snowflake UI and navigate to the **Query History** under the Monitoring tab, we are going to see all SQL queries run on the chosen Snowflake account(successful, failed, running etc) and clearly see what dbt model this particular query is related to.

    ![image](/dbt_snowflake/dbt_hol/images/query_history.png)

4. **dbt plugins**. Alongside functionality coming out of the box with dbt core, dbt also provide capability to plug-in additional packages which can be found in the [dbt Hub](https://hub.getdbt.com/) or straight out of GitHub repository. In this tutorial, we will be using some automation that the [dbt_utils](https://hub.getdbt.com/dbt-labs/dbt_utils/latest/) package provides. To set up the packages, create a new file called `packages.yml` in the root of your dbt project folder and add the following lines:

    ```yml
    packages:
    - package: dbt-labs/dbt_utils
      version: 1.1.1
    ```

    Once the new file created, run this command to install the defined package.
    ```cmd
    dbt deps
    ```

    The expected output is as below:
    ```cmd
    05:53:45  Running with dbt=1.7.9
    05:53:45  Updating lock file in file path: D:\OneDrive\0_Project\e001-dbt-guides\dbt_snowflake\dbt_hol/package-lock.yml
    05:53:45  Installing dbt-labs/dbt_utils
    05:53:46  Installed from version 1.1.1
    05:53:46  Up to date
    ```

### dbt Pipelines for Stock Trading History
1. The first stage of building the dbt pipeline is to declare the [dbt sources](https://docs.getdbt.com/docs/building-a-dbt-project/using-sources). Let's create a `models/l10_staging/sources.yml` file and add the following configuration:

    ```yml
    version: 2

    sources:
    - name: FINANCIAL__ECONOMIC_ESSENTIALS
      database: FINANCIAL__ECONOMIC_ESSENTIALS
      schema: CYBERSYN
      tables:
        - name: FX_RATES_TIMESERIES
        - name: STOCK_PRICE_TIMESERIES
    ```

    This configuration file defines two tables ('**FX_RATES_TIMESERIES**' and '**STOCK_PRICE_TIMESERIES**') from the '**CYBERSYN**' schema in the '**FINANCIAL__ECONOMIC_ESSENTIALS**' database as sources for this tutorial.

2. After setting up the dbt sources, we need to create the staging models which act as a first-level transformation. While not mandatory, these could act as a level of abstraction, separating ultimate source structure from the entry point of dbt pipeline. Providing your project more options to react to an upstream structure change. To create the models, you need to follow these few steps:
    - Create a new file `models/l10_staging/stg_fx_rates_time_series.sql` and include the following codes:

        ```sql
        SELECT
            VARIABLE,
            BASE_CURRENCY_ID,
            QUOTE_CURRENCY_ID,
            BASE_CURRENCY_NAME,
            QUOTE_CURRENCY_NAME,
            DATE,
            VALUE,
            'CYBERSYN.FX_RATES_TIMESERIES' data_source_name
        FROM {{source('FINANCIAL__ECONOMIC_ESSENTIALS', 'FX_RATES_TIMESERIES')}} src
        ```

    - Create a new file `models/l10_staging/stg_stock_price_time_series.sql` and include the following codes:

        ```sql
        SELECT
            TICKER,
            PRIMARY_EXCHANGE_NAME,
            VARIABLE,
            VARIABLE_NAME,
            DATE,
            VALUE,
            'CYBERSYN.STOCK_PRICE_TIMESERIES' data_source_name
        FROM {{source('FINANCIAL__ECONOMIC_ESSENTIALS', 'STOCK_PRICE_TIMESERIES')}} src
        ```

    I am sure you noticed, this looks like SQL with the exception of macro **{{source()}}** that is used in "FROM" part of the query instead of fully qualified path (database.schema.table). This is one of the key concepts that is allowing dbt during compilation to replace this with target-specific name. As result, you as a developer, can promote **same** pipeline code to DEV, PROD and any other environments without any changes.

3. **Run the models** to create the views inside the Snowflake. There are multiple ways of running or executing the models:
    - Run `dbt run` to run all models
	- Run `dbt run --model l10_staging` to run all models that are located in **models/l10_staging**
	- Run `dbt run -s stg_fx_rates_time_series` to select a specific model to run. More details are in [documentation](https://quickstarts.snowflake.com/guide/data_teams_with_dbt_core/(https://docs.getdbt.com/reference/node-selection/syntax)).

    The expected outcome is shown below:
    ```cmd
    09:22:48  Running with dbt=1.7.9
    09:22:50  Registered adapter: snowflake=1.7.2
    09:22:50  [WARNING]: Configuration paths exist in your dbt_project.yml file which do not apply to any resources.
    There are 2 unused configuration paths:
    - models.dbt_hol.l30_mart
    - models.dbt_hol.l20_transform
    09:22:50  Found 2 models, 2 sources, 0 exposures, 0 metrics, 546 macros, 0 groups, 0 semantic models
    09:22:50
    09:22:52  Concurrency: 200 threads (target='dev')
    09:22:52
    model in 0 hours 0 minutes and 3.51 seconds (3.51s).
    09:22:54
    09:22:54  Completed successfully
    09:22:54
    09:22:54  Done. PASS=1 WARN=0 ERROR=0 SKIP=0 TOTAL=1
    ```

    After completing the above step, we should be able to see two new views created inside the `dbt_hol_dev` database through the Snowflake web UI. We can then start writing SQL to query data from the view. For example, executing this SQL:

    ```sql
    -- staging stage
    SELECT * 
    FROM dbt_hol_dev.l10_staging.stg_stock_price_time_series
    WHERE ticker ='AAPL' 
    AND date ='2024-03-21';
    ```
    It will return the following results:

    ![image](/dbt_snowflake/dbt_hol/images/staging_results.png)

    Based on the above image, we can see that the variable_name like Post-Market Close, All-Day High, Pre-Market Open, All-Day Low and Nasdaq Volume are represented as different individual rows. However, for this tutorial, we need to simplify it by transposing the data into columns as shown in the image below. To achieve this, we will need to create few more models for the data transformation.

    ![image](/dbt_snowflake/dbt_hol/images/wanted_columns.png)

4. There are multiple ways to achieve the data transformation.
    - **Option 1: Using typical SQL method**:
        - We first create a new file `~/models/l20_transform/tfm_stock_price_time_series.sql` and then include the following codes:
        ```sql
        WITH cte AS(
            SELECT 
                TICKER,
                PRIMARY_EXCHANGE_NAME,
                VARIABLE_NAME,
                DATE,
                VALUE,
                data_source_name
            FROM 
                {{ref('stg_stock_price_time_series')}} src
            WHERE
                VARIABLE_NAME IN ('Post-Market Close', 'Pre-Market Open', 'All-Day High', 'All-Day Low', 'Nasdaq Volume')
        )
        SELECT *
        FROM cte
        PIVOT(SUM(Value) FOR VARIABLE_NAME IN ('Post-Market Close', 'Pre-Market Open', 'All-Day High', 'All-Day Low', 'Nasdaq Volume'))
        AS p(TICKER,PRIMARY_EXCHANGE_NAME,DATE,data_source_name,close, open, high, low, volume)
        ```
    
    - **Option 2: Using the dbt_utils method**:
        - We create another file `~/models/l20_transform/tfm_stock_price_time_series_alt.sql` and then include the following codes:
        ```sql
        SELECT
            TICKER,
            PRIMARY_EXCHANGE_NAME,
            DATE,
            VALUE,
            data_source_name,
            {{dbt_utils.pivot(
                column = 'VARIABLE_NAME',
                values = dbt_utils.get_column_values(ref('stg_stock_price_time_series'), 'VARIABLE_NAME'),
                then_value = 'value'
            )}}
        FROM {{ref('stg_stock_price_time_series')}}
        GROUP BY TICKER,PRIMARY_EXCHANGE_NAME,DATE,VALUE,data_source_name
        ```

5. After the creation of those dbt transformation models, we then run the following codes:
    - `dbt run -m tfm_stock_price_time_series.sql`
    - `dbt run -m tfm_stock_price_time_series_alt.sql`
    
   If encounter any errors, you need to resolve the issues by checking either the SQL codes, connection to Snowflake, environments, etc.

6. In addition, we will create one more file `~/models/l20_transform/tfm_stock_history.sql` with the following codes:
    ```sql
    SELECT src.*
    FROM {{ref('tfm_stock_price_time_series')}} src
    ```

    It serves as another model that abstracts source-specific transformations into a business view. In case there were multiple feeds providing datasets of the same class (stock history in this case), this view would be able to consolidate (UNION ALL) data from all of them. Thus becoming a one-stop-shop for all stock_history data.

7. **Deploy** the transformation model including all of its ancestors by running this command:
    ```cmd
    dbt run --model +tfm_stock_history
    ```

    The expected output will be as below:
    ```cmd
    13:41:38  Running with dbt=1.7.9
    13:41:39  Registered adapter: snowflake=1.7.2
    13:41:40  [WARNING]: Configuration paths exist in your dbt_project.yml file which do not apply to any resources.
    There are 1 unused configuration paths:
    - models.dbt_hol.l30_mart
    13:41:40  Found 5 models, 2 sources, 0 exposures, 0 metrics, 546 macros, 0 groups, 0 semantic models
    13:41:40  
    13:41:42  Concurrency: 200 threads (target='dev')
    13:41:42
    13:41:42  1 of 3 START sql view model l10_staging.stg_stock_price_time_series ............ [RUN]
    13:41:45  Finished running 3 view models in 0 hours 0 minutes and 5.11 seconds (5.11s).
    13:41:45
    13:41:45  Completed successfully
    13:41:45
    13:41:45  Done. PASS=3 WARN=0 ERROR=0 SKIP=0 TOTAL=3   
    ```

8. Check the existence of the transformed models in the Snowflake Web UI.