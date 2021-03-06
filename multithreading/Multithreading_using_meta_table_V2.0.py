 
import snowflake.connector
import threading
import pandas as pd

def sfConnect (
    sfPswd = '',
    sfUser = '',
    sfAccount = ''
) :
    # ### This script creates a function that establishes a connection to a Snowflake instance
    sfConnection = snowflake.connector.connect(
    user=sfUser,
    password=sfPswd,
    account=sfAccount
    )
    cs = sfConnection.cursor()
    try:
        results = cs.execute('select current_version()').fetchone()
    except:
        print('Connection failed, check credentials')
        return
    finally:
        print('Connection established')
        print("Snowflake Version: " + results[0])
        cs.close()
    return sfConnection





class sfExecutionThread (threading.Thread):
    def __init__(self, threadID, sqlQuery):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.sqlQuery = sqlQuery
    def run(self):
        print('Starting {0}: {1}'.format(self.threadID, self.sqlQuery))
        sfExecuteInSnowflake(self.sqlQuery)
        print('Exiting {0}: {1}'.format(self.threadID, self.sqlQuery))

        
        
        
# Define the function that will be executed within each thread
def sfExecuteInSnowflake (
    sfQuery,
    sfRole = 'SYSADMIN',
    sfWarehouse = 'MULTI_THREADING_TEST') :
    sfConnection.cursor().execute(sfQuery)

    
    
# Define the list of variables which determine the data that will be loaded
def gettabledetails (
    sfPswd = '',
    sfUser = '',
    sfAccount = ''
) :
    # ### This script creates a function that establishes a connection to a Snowflake instance
    sfConnection = snowflake.connector.connect(
    user=sfUser,
    password=sfPswd,
    account=sfAccount
    )
    cs = sfConnection.cursor()
    try:
        results = cs.execute('select * from "OUR_FIRST_DATABASE"."PUBLIC"."MET_TABLE"').fetchall()
        print('Connection established')
        result = pd.DataFrame(results)
        print (result)
        return result
    except:
        print('Connection failed, check credentials')
        return 0
    finally:
        cs.close()
        sfConnection.close()

        

application = input('Enter application name : ')
details_table = gettabledetails(sfPswd = 'Minipandu@0',sfUser = 'anirudhtcs',sfAccount = 'sx69711.ap-south-1.aws')
details_table.columns = ['File_Name','SF_Table','File_Format','Application_Name',
                         'Application_ID','Active_Flag']
if application in details_table['Application_Name'].values:
    details_table_app_filtered = details_table[details_table['Application_Name']==application]
    #print(details_table_app_filtered)
    split_details = details_table_app_filtered["File_Name"].str.split("/",n=1,expand =True)
    split_dbschma = split_details[0].str.split(".", n=2, expand=True)
    
# Define an empty list to populate with COPY INTO statements
    copyIntoStatements = []
# Loop through the members of variablesList and construct the COPY INTO statements
# Use .format() to replace the {0} and {1} with variables destinationTable and sourceLocation
    #sfC = sfConnect(sfPswd = 'Minipandu@0',sfUser = 'anirudhtcs',sfAccount = 'sx69711.ap-south-1.aws')
    for member in details_table_app_filtered.index:
        #sfC.cursor().execute("USE DATABASE "+split_dbschma[0])
        #sfC.cursor().execute("USE SCHEMA "+split_dbschma[1])
        copyIntoStatements.append(
        '''
        COPY INTO {0}
        FROM {1}
        FILES = ('{2}')
        FILE_FORMAT = (FORMAT_NAME = {3})
        ;
        '''.format(details_table_app_filtered['SF_Table'][member],split_dbschma[2][member],split_details[1][member],details_table_app_filtered['File_Format'][member]))
    

# Create the empty list of threads
    threads = []
# Define a counter which will be used as the threadID
    counter = 0
# Loop through each statement in the copyIntoStatements list,
# adding the sfExecutionThread thread to the list of threads
# and incrementing the counter by 1 each time.

#########################
    # Establish connection
    ## Make sure you insert the right login credentials below.
    sfConnection = sfConnect(sfPswd = 'Minipandu@0',sfUser = 'anirudhtcs',sfAccount = 'sx69711.ap-south-1.aws')
    # Use role defined in function input
    sfConnection.cursor().execute('USE ROLE SYSADMIN')
    sfConnection.cursor().execute('USE DATABASE OUR_FIRST_DATABASE')
       
    # Use warehouse defined in function input
    sfConnection.cursor().execute('USE WAREHOUSE MULTI_THREADING_TEST')
    # Increase the session timeout if desiredabs
    sfConnection.cursor().execute('ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 12000')
    # Execute the query sfQuery in Snowflake
#####################################
    for statement in copyIntoStatements:
        threads.append(sfExecutionThread(counter, statement))
        counter += 1
# Execute the threads
    for thread in threads:
        thread.start()
    print('All threads started')
else:
    print('invalid Application name')
