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
        results = cs.execute('select * from "OUR_FIRST_DATABASE"."PUBLIC"."LOAD_DETAILS"').fetchall()
        print('Connection established')
        result = pd.DataFrame(results)
        #print (type(result))
        return result
    except:
        print('Connection failed, check credentials')
        return 0
    finally:
        cs.close()
        sfConnection.close()

        

application = input('Enter application name : ')
details_table = gettabledetails(sfPswd = 'Minipandu@0',sfUser = 'anirudhtcs',sfAccount = 'sx69711.ap-south-1.aws')
details_table.columns = ['application','source_location','target_location','delimiter_used',
                         'enclosing_char','escape_char']
if application in details_table['application'].values:
    
#details_table_app_filtered = details_table[details_table['application']==application]

# Define an empty list to populate with COPY INTO statements
    copyIntoStatements = []
# Loop through the members of variablesList and construct the COPY INTO statements
# Use .format() to replace the {0} and {1} with variables destinationTable and sourceLocation
    for member in details_table_app_filtered.index:
        copyIntoStatements.append(
        '''
        create or replace table {0}
        as select * from {1}
        ;
        '''.format(details_table_app_filtered['target_location'][member],details_table_app_filtered['source_location'][member]))
    

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
