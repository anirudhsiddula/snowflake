import snowflake.connector
import threading

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
    # Establish connection
    ## Make sure you insert the right login credentials below.
    #sfConnection = sfConnect(sfPswd = 'Minipandu@0',sfUser = 'anirudhtcs',sfAccount = 'sx69711.ap-south-1.aws')
    # Use role defined in function input
    #sfConnection.cursor().execute('USE ROLE {0}'.format(sfRole))
    # Use warehouse defined in function input
    #sfConnection.cursor().execute('USE WAREHOUSE {0}'.format(sfWarehouse))
    # Increase the session timeout if desired
    #sfConnection.cursor().execute('ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 12000')
    # Execute the query sfQuery in Snowflake
    sfConnection.cursor().execute(sfQuery)
# Define the list of variables which determine the data that will be loaded
variablesList = [
    {
      'sourceLocation': '"SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF10TCL"."CUSTOMER"',
      'destinationTable': '"OUR_FIRST_DATABASE"."PUBLIC"."SECOND_TABLE"'
    },
    {
      'sourceLocation': '"SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."JLINEITEM"',
      'destinationTable': '"OUR_FIRST_DATABASE"."PUBLIC"."ZERO_TABLE"'
    },
    {
      'sourceLocation': '"SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER_ADDRESS"',
      'destinationTable': 'OUR_FIRST_DATABASE.PUBLIC.third_table'
    },
    {
      'sourceLocation': '"SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER_DEMOGRAPHICS"',
      'destinationTable': 'OUR_FIRST_DATABASE.PUBLIC.fourth_table'
    }
        ]
       
# Define an empty list to populate with COPY INTO statements
copyIntoStatements = []
# Loop through the members of variablesList and construct the COPY INTO statements
# Use .format() to replace the {0} and {1} with variables destinationTable and sourceLocation
#for member in variablesList:
#    copyIntoStatements.append(
#    '''
#    COPY INTO {0}
#    FROM @{1}
#    FILE_FORMAT = (type =csv field_delimiter = '|' skip_header = 1)
#    ;
#    '''.format(member['destinationTable'], member['sourceLocation'])
#  )
        
for member in variablesList:
        copyIntoStatements.append(
        '''
        create or replace table {0}
        as select * from {1}
          ;
        '''.format(member['destinationTable'], member['sourceLocation']))
        
#for member in variablesList:
#    for i in range(10):
#        copyIntoStatements.append(
#        '''
#        COPY INTO {0}
#        FROM @{1}
#          FILE_FORMAT = (type =csv field_delimiter = '|' skip_header = 1)
#         ;
#        '''.format(member['destinationTable'], member['sourceLocation'])
#  )
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
    # Increase the session timeout if desired
sfConnection.cursor().execute('ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 12000')
    # Execute the query sfQuery in Snowflake
#####################################
for statement in copyIntoStatements:
    threads.append(sfExecutionThread(counter, statement))
    counter += 1
# Execute the threads
for thread in threads:
    thread.start()
