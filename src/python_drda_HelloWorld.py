##
# Python Sample Application: Connection to Informix using DRDA
##

##
# Topics
# 1 Create table
# 2 Inserts
# 2.1 Insert a single document into a table
# 2.2 Insert multiple documents into a table
# 3 Queries
# 3.1 Find one document in a table that matches a query condition  
# 3.2 Find documents in a table that match a query condition
# 3.3 Find all documents in a table
# 4 Update documents in a table
# 5 Delete documents in a table
# 6 Drop a table
##

import ibm_db
import json
import logging
import os
from flask import Flask, render_template

app = Flask(__name__)

# To run locally, set URL and DATABASE information.
# Otherwise, url and database information will be parsed from 
# the Bluemix VCAP_SERVICES.
URL = ""

USE_SSL = False     # Set to True to use SSL url from VCAP_SERVICES
SERVICE_NAME = os.getenv('SERVICE_NAME', 'timeseriesdatabase')
port = int(os.getenv('VCAP_APP_PORT', 8080))

class DataFormat:
    def __init__(self, name, value):
        self.name = name
        self.value = value

def getDatabaseInfo():
    """
    Get database url information
    
    :returns: (url)
    """
    
    # Use defaults
    if URL:
        return URL
    
    # Parse database info from VCAP_SERVICES
    if (os.getenv('VCAP_SERVICES') is None):
        raise Exception("VCAP_SERVICES not set in the environment")
    vcap_services = json.loads(os.environ['VCAP_SERVICES'])
    try:
        tsdb = vcap_services[SERVICE_NAME][0]
        credentials = tsdb['credentials']
        database = credentials['db']
        host = credentials['host']
        username = credentials['username']
        password = credentials['password']  
        if USE_SSL:
            port = credentials['drda_port_ssl']
        else:
            port = credentials['drda_port']
        url = "HOSTNAME=" + host + ";PORT=" + str(port) + ";DATABASE="+ database + ";PROTOCOL=TCPIP;UID=" + username +";PWD="+ password + ";"
        if USE_SSL:
            url += "Security=ssl;"
        return url
    
    except KeyError as e:
        logging.error(e)
        raise Exception("Error parsing VCAP_SERVICES. Key " + str(e) + " not found.")


def doEverything(): 
    output = []
    
    # Get database connectivity information
    url = getDatabaseInfo()
    
    # connect to database
    output.append("Connecting to " + url)
    try: 
        conn = ibm_db.connect(url, '', '')
    except: 
        output.append("Could not establish connection to database: " + ibm_db.conn_errormsg())
        return output
    output.append("Connection successful")
    
    # set up variables and data
    tableName = "pythonDRDATest"
    user1 = DataFormat("test1", 1)
    user2 = DataFormat("test2", 2)
    user3 = DataFormat("test3", 3)
    
    try: 
        # 1 Create table
        output.append("\n#1 Create table")
        
        sql = "create table " + tableName + "(name varchar(255),  value integer)"
        ibm_db.exec_immediate(conn, sql)
             
        output.append( "\tCreate a table named: " + tableName)
        output.append("\tCreate Table SQL: " + sql)
        
        # 2 Inserts
        output.append("\n#2 Inserts")
        # 2.1 Insert a single document into a table
        output.append("#2.1 Insert a single document into a table")
        
        sql = "insert into " + tableName + " values(?,?)"
        statement = ibm_db.prepare(conn, sql)
        ibm_db.bind_param(statement, 1, user1.name)
        ibm_db.bind_param(statement, 2, user1.value)
        ibm_db.execute(statement)
        
        output.append("\tCreate Document -> " + user1.name + " : " + str(user1.value))
        output.append("\tSingle Insert SQL: " + sql)
        
        sql = "insert into " + tableName + " values(?,?)"
        statement = ibm_db.prepare(conn, sql)
        ibm_db.bind_param(statement, 1, user2.name)
        ibm_db.bind_param(statement, 2, user2.value)
        ibm_db.execute(statement)
        
        output.append("\tCreate Document -> " + user2.name + " : " + str(user2.value))
        output.append("\tSingle Insert SQL: " + sql)
        
        sql = "insert into " + tableName + " values(?,?)"
        statement = ibm_db.prepare(conn, sql)
        ibm_db.bind_param(statement, 1, user3.name)
        ibm_db.bind_param(statement, 2, user3.value)
        ibm_db.execute(statement)
        
        output.append("\tCreate Document -> " + user3.name + " : " + str(user3.value))
        output.append("\tSingle Insert SQL: " + sql)
        
        # 2.2 Insert multiple documents into a table
        # Currently there is no support for batch inserts with ibm_db
        output.append("#2.2: Insert multiple documents into a table. \n\tCurrently there is no support batch inserts")
        
        # 3 Queries
        output.append("\n#3 Queries")
        
        # 3.1 Find one document in a table that matches a query condition 
        output.append("#3.1 Find one document in a table that matches a query condition")
        
        sql = "select * from " + tableName + " where name LIKE '" + user1.name + "'"
        stmt = ibm_db.exec_immediate(conn, sql)
        dictionary = ibm_db.fetch_both(stmt)
        
        output.append("\tFind document with name: " + user1.name)
        output.append("\tFirst document with name -> name: " +  str(dictionary[0]) + " value: " + str(dictionary[1]))
        output.append("\tQuery By name SQL: " + sql)
        
        # 3.2 Find documents in a table that match a query condition
        output.append("#3.2 Find documents in a table that match a query condition")
        
        sql = "select * from " + tableName + " where name LIKE '" + user1.name + "'"
        stmt = ibm_db.exec_immediate(conn, sql)
        dictionary = ibm_db.fetch_both(stmt)
        
        output.append("\tFind all documents with name: " + user1.name)
        while dictionary != False:
            output.append("\tFound Document -> name: " + str(dictionary[0]) + " value: " + str(dictionary[1]))
            dictionary = ibm_db.fetch_both(stmt)
        output.append( "\tQuery All By name SQL: " + sql)
        
        # 3.3 Find all documents in a table
        output.append("#3.3 Find all documents in a table")
        
        sql = "select * from " + tableName
        stmt = ibm_db.exec_immediate(conn, sql)
        dictionary = ibm_db.fetch_both(stmt)
        
        output.append( "\tFind all documents in table: " + tableName)
        while dictionary != False:
            output.append("\tFound Document -> name: " + str(dictionary[0]) + " value: " + str(dictionary[1]))
            dictionary = ibm_db.fetch_both(stmt)
        output.append("\tFind All Documents SQL: " + sql)
        
        
        # 4 Update documents in a table
        output.append("\n#4 Update documents in a table")
        
        sql = "update " + tableName + " set value = ? where name = ?"
        statement = ibm_db.prepare(conn, sql)
        ibm_db.bind_param(statement, 1, 4)
        ibm_db.bind_param(statement, 2, user2.name)
        ibm_db.execute(statement)
        
        output.append( "\tDocument to update: " + user2.name)
        output.append("\tUpdate By name SQL: " + sql)
        
        
        # 5 Delete documents in a table
        output.append("\n#5 Delete documents in a table")
        
        sql = "delete from " + tableName + " where name like '" + user1.name + "'"
        ibm_db.exec_immediate(conn, sql)
        
        output.append("\tDelete documents with name: " + user1.name)
        output.append("\tDelete By name SQL: " + sql)
        
        # 6 Drop a table
        output.append("\n#6 Drop a table")
        
        sql = "drop table " + tableName;
        ibm_db.exec_immediate(conn, sql)
        
        output.append("\tDrop table: " + tableName)
        output.append("\tDrop Table SQL: " + sql)
    
    except Exception as e:
        logging.exception(e) 
        output.append("EXCEPTION (see log for details): " + str(e))
    finally:
        if conn:
            ibm_db.close(conn)
            output.append("\nConnection closed")
            
    return output

@app.route("/")
def displayPage():
    return render_template('index.html')

@app.route("/databasetest")
def runSample():
    output = []
    try:
        output = doEverything()
    except Exception as e:
        logging.exception(e) 
        output.append("EXCEPTION (see log for details): " + str(e))

    return render_template('tests.html', output=output)

if (__name__ == "__main__"):
    app.run(host='0.0.0.0',port=port)