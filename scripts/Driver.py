# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import configparser
import os
import json
import random
import sys
from logging.handlers import RotatingFileHandler

import requests
from sqlalchemy import create_engine, text
import numpy as np
import pandas as pd
import time
import logging
import mysql.connector
from mysql.connector import errorcode
from sqlalchemy.exc import SQLAlchemyError

import dccImage
import urllib3, socket
from urllib3.connection import HTTPConnection

outputDir = "Output"
try:
    os.mkdir(outputDir)

except FileExistsError as e:
    print("File exists")

"""Setup logger"""

logger = logging.getLogger(__name__)
FORMAT = "[%(asctime)s->%(filename)s->%(funcName)s():%(lineno)s]%(levelname)s: %(message)s"
logging.basicConfig(format=FORMAT, filemode="w", level=logging.DEBUG, force=True)
logging_filename = outputDir + "/" + 'App.log'
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
    config = configparser.ConfigParser()
    config.read('db.ini')
    user = config.get("section_a", "User")
    password = config.get("section_a", "Password")
    host = config.get("section_a", "Port")

    try:
        conn = mysql.connector.connect(host=host, user=user, password=password, database=schema)
        logger.info(f"Successfully connected to {schema}")
        return conn

    except mysql.connector.Error as err1:
        if err1.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")

        elif err1.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")

        else:
            print(err1)

    except ConnectionError as err2:
        print("Unable to connect to DB due to" + err2.strerror)
        print()


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
    config = configparser.ConfigParser()
    config.read('db_init.INI')
    password = config.get("section_a", "Password")
    host = config.get("section_a", "Port")
    database = config.get("section_a", "Database")
    df = pd.concat(dataset)
    df.reset_index(inplace=True)

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
    filePtr = sys.argv[1]
    with open(filePtr, "r") as f:
        lines = f.readlines()

    print(lines)

    """Set higher timeout"""

    HTTPConnection.default_socket_options = (
            HTTPConnection.default_socket_options + [
        (socket.SOL_SOCKET, socket.SO_SNDBUF, 1000000),
        (socket.SOL_SOCKET, socket.SO_RCVBUF, 1000000)
    ])

    """Setup Database connection"""
    conn_to_rslims = init("rslims")
    conn_to_komp = init("komp")

    """Read sql script"""
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

    """
    newImage = dccImage.impcInfo("", "", "IMPC_EYE_050_001", "test")
    df = newImage.queryByParameterKey()
    df.to_csv("test.csv")
    """

    """Close the connection"""
    conn_to_komp.close()
    conn_to_rslims.close()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
