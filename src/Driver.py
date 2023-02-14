# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import os
import random
import sys
from logging.handlers import RotatingFileHandler

from sqlalchemy import create_engine
import pandas as pd
import logging
import mysql.connector
from mysql.connector import errorcode
from sqlalchemy.exc import SQLAlchemyError
import socket
from urllib3.connection import HTTPConnection
from dccImage.dccImage import impcInfo
import Config as Config

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

    user = Config.User
    password = Config.Password
    host = Config.Port

    try:
        conn = mysql.connector.connect(host=host, user=user, password=password, database=schema)
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

    df = pd.concat(dataset)
    df.reset_index(inplace=True)
    df.drop("procedureKey", inplace=True)

    try:
        engine = create_engine("mysql+mysqlconnector://root:{0}@{1}/{2}".
                               format(password, host, database),
                               pool_recycle=1, pool_timeout=57600,
                               future=True)
        df.to_sql(tableName, engine, if_exists='append', index=False)

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
    HTTPConnection.default_socket_options = (
            HTTPConnection.default_socket_options + [
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
    parameterKeys = queryParameterKey(conn_to_rslims, commands[0], imageType="IMPC")
    print(len(parameterKeys))
    logger.debug("Number of impc test codes is {size}".format(size=len(parameterKeys)))
    colonyIds = queryColonyId(conn_to_rslims, commands[1])
    logger.debug("Number of JR number is {size}".format(size=len(colonyIds)))

    #newImage = impcInfo("", "", "IMPC_EYE_050_001", "test")

    filePtr = sys.argv[1]
    with open(filePtr, "r") as f:
        lines = f.readlines()

    print(lines)

    if len(lines) == 1:
        line = lines[0].split(":")
        logger.debug(f"Reading {line}")

        if line[0] == "Parameter Key":

            newImage = impcInfo("", "", "IMPC_EYE_050_001", "test")
            #print(newImage.parameterKey)
            result = newImage.getByParameterKey()
            insert_to_db(result, "dccimages")

    """Close the connection"""
    conn_to_komp.close()
    conn_to_rslims.close()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
