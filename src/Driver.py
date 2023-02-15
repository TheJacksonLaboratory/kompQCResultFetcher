# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import logging
import os
import random
import socket
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler

import mysql.connector
import pandas as pd
from mysql.connector import errorcode
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from urllib3.connection import HTTPConnection

import Config as Config
from dccImage.dccImage import impcInfo

outputDir = "/Users/chent/Desktop/KOMP_Project/FetchQCResult/docs/Output"

try:
    os.mkdir(outputDir)

except FileExistsError as e:
    print("File exists")

"""Setup logger"""

logger = logging.getLogger(__name__)
FORMAT = "[%(asctime)s->%(filename)s->%(funcName)s():%(lineno)s]%(levelname)s: %(message)s"
logging.basicConfig(format=FORMAT, filemode="w", level=logging.DEBUG, force=True)
logging_filename = outputDir + "/" + 'Models.log'
handler = RotatingFileHandler(logging_filename, maxBytes=10000000000, backupCount=10)
handler.setFormatter(logging.Formatter(FORMAT))
logger.addHandler(handler)

"""
Function to list of JR nubers into n parts randomly
@param:
    colonyIds: List of JR numbers
    n: size of partitioned list
"""


def partition(colonyIds, n):
    random.shuffle(colonyIds)
    return [colonyIds[i::n] for i in range(n)]


"""
Function to connect to database schema
@:param:
    schema: Name of schema to connect
"""


def init(schema) -> mysql.connector:
    User = Config.User
    Password = Config.Password
    Host = Config.Port

    try:
        conn = mysql.connector.connect(host=Host, user=User, password=Password, database=schema)
        logger.info(f"Successfully connected to {schema}")
        return conn

    except mysql.connector.Error as err1:
        if err1.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logger.error("Wrong user name or password passed")

        elif err1.errno == errorcode.ER_BAD_DB_ERROR:
            logger.error("No such schema")

        else:
            error = str(err1.__dict__["orig"])
            logger.error("Error message: {error}".format(error=error))

    except ConnectionError as err2:
        logger.error("Unable to return connect to database due to" + err2.strerror)

    return None


"""
Function to get IMPC test code
@:param
    conn: Connection to MySQL database
    sql: SQL statement to be executed 
"""


def queryParameterKey(conn, sql, imageType) -> list:
    if not conn or not sql:
        print("Empty connection/statement found")
        return []

    result = []
    cursor = conn.cursor(buffered=True, dictionary=True)
    cursor.execute(sql)
    impcTestCode = cursor.fetchall()
    print(impcTestCode)

    for pair in impcTestCode:
        for key in pair.keys():
            result.append(pair[key])

    return result


"""
Function to get colony ids/JR numbers
@:param
    conn: Connection to MySQL database
    sql: SQL statement to be executed 
"""


def queryColonyId(conn, sql) -> list:
    if not conn or not sql:
        print("Empty string")
        return []

    colonyIds = []
    cursor = conn.cursor(buffered=True, dictionary=True)
    cursor.execute(sql)
    queryResult = cursor.fetchall()

    # print(queryResult)
    for dict_ in queryResult:
        # if dict_["TestImpcCode"] in nameMap.keys():
        colonyIds.append(dict_["JR"])

    # Remove duplicate item
    result = [*set(colonyIds)]
    return result


"""
   Recursive function to find last page of 
   Pseudo Code:
       if payload["total"] == 0
           return (0, 0)

       else if payload["total"] < size:
           return (start, size)

       else:
           return findLastPage(size + 1, size*(multiplier + 1))
   """


def getLastPage(colonyId) -> tuple:
    pass


"""
Function to store data into database
@:parameter
    testCode -> impcTestCode
    animalId -> mouseId
    start -> start page
    size = number of related animal in one page    
"""


def insert_to_db(dataset, tableName):
    """
    insertStmt = "INSERT INTO komp.dccQualityIssues (AnimalName, Taskname, TaskInstanceKey, ImpcCode, StockNumber, DateDue, Issue) VALUES ( '{0}','{1}',{2},'{3}','{4}','{5}','{6}')". \
        format(msg['AnimalName'], msg['TaskName'], int(msg['TaskInstanceKey']), msg['ImpcCode'], msg['StockNumber'],
               msg['DateDue'], msg['Issue'].replace("'", "\""))
    """
    user = Config.User
    password = Config.Password
    host = Config.Port
    database = Config.Database

    if not dataset:
        logger.warning("No record retrieved!")
        return

    df = pd.concat(dataset)
    df.drop("procedureKey", axis = 1, inplace=True)
    currTime = [datetime.now() for i in range(len(df.index))]
    insertData = df.copy()
    insertData["modifiedTime"] = pd.Series(currTime).values
    #print(insertData.iloc[2552])
    print(list(insertData.columns))
    try:
        # Use dba instead of root
        engine = create_engine("mysql+mysqlconnector://{0}:{1}@{2}/{3}".
                               format(user, password, host, database),
                               pool_recycle=1, pool_timeout=57600,
                               future=True)

        with engine.connect() as conn:
            logger.debug("Getting the column names")
            keys = conn.execute(text("SELECT * FROM komp.dccImages;")).keys()

        #print(keys)
        insertData.columns = keys
        print(insertData)
        insertData.to_sql(tableName, engine, if_exists='append', index=False)
        insertionResult = engine.connect().execute(text("SELECT * FROM komp.dccImages;"))
        logger.debug(f"Insertion result is:{insertionResult}")
        result = engine.connect().execute(text("SELECT * FROM dccImages;"))
        if result.all():
            logger.info("Data successfully inserted!")
        else:
            logger.debug("No record found, please check your dataframe")

    except SQLAlchemyError as e:
        error = str(e.__dict__["orig"])
        logger.error("Error message: {error}".format(error=error))


"""
Input a .txt file, with animalId/parameterCode/colonyId on each line.
1. If the line is a colonyId -> query by JR number
2. If the line is an animalId -> query by animal id
3. If the line is 
"""


def main():
    """Set higher timeout"""
    HTTPConnection.default_socket_options = (HTTPConnection.default_socket_options + [
        (socket.SOL_SOCKET, socket.SO_SNDBUF, 1000000),
        (socket.SOL_SOCKET, socket.SO_RCVBUF, 1000000)
    ])

    """Setup Database connection"""
    conn_to_rslims = init("rslims")
    conn_to_komp = init("komp")

    """"Read sql script"""
    here = os.path.dirname(os.path.abspath(__file__))
    sqlFile = os.path.join(here, 'dccImage.sql')
    fptr = open(sqlFile, "r")
    sqlFile = fptr.read()
    commands = sqlFile.split(";")

    # Query database
    parameterKeys = queryParameterKey(conn_to_rslims, commands[1], imageType="IMPC")
    print(len(parameterKeys))
    logger.debug("Number of impc test codes is {size}".format(size=len(parameterKeys)))
    colonyIds = queryColonyId(conn_to_rslims, commands[2])
    logger.debug("Number of JR number is {size}".format(size=len(colonyIds)))
    """
    for parameterKey in parameterKeys:
        print(parameterKey)
        newImage = impcInfo("", "", parameterKey, "komp.dccimages")
        result = newImage.getByParameterKey()
        insert_to_db(result, "dccimages")

    """
    filePtr = sys.argv[1]
    with open(filePtr, "r") as f:
        lines = f.readlines()

    print(lines)

    for line in lines:
        line = line.split(":")
        logger.debug(f"Reading {line}")
        if line[0] == "Parameter Key":
            newImage = impcInfo("", "", line[1].strip(), "test")
            result = newImage.getByParameterKey("", "", 0, sys.maxsize)
            logger.debug("Number of records found:{len(result)}")
            insert_to_db(result, "dccimages")

    #Close the connection
    conn_to_komp.close()
    conn_to_rslims.close()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
