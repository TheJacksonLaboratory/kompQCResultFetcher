# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import configparser
import os
import json
from logging.handlers import RotatingFileHandler

import requests
from sqlalchemy import create_engine, text
import numpy as np
import pandas as pd
import logging
import mysql.connector
from mysql.connector import errorcode
from sqlalchemy.exc import SQLAlchemyError
import src.FetchIMPC.IMPC as impc
import src.FetchEBI.EBI as ebi

"""
Overall design:
1. Query database for animal/file information uploaded to DCC
2. Get data relate to the resulting animal and retrieve data
3. Search for animal with status: "Fail"
4. Generate a human-readable report and deliver to technician 
"""

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

nameMap = {"IMPC_EYE_001": "IMPC_EYE_050_001", "IMPC_CSD_001": "IMPC_CSD_085_001",
           "IMPC_ECG_001": "IMPC_ECG_025_001)"}

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
Function to traverse the url dictionary and retrieve data from the urls
@:param
    urlMap: dictionary that stores urls corresponding to the given test code 
"""


def BFS(urlMap) -> None:
    if not urlMap:
        return

    testCodes = urlMap.keys()
    dataset = []
    # fileName = "data.json"
    for key in testCodes:
        for url in urlMap[key]:
            print(url)
            try:
                response = requests.get(url)
                payload = response.json()
                """
                with open(fileName, 'w') as fp:
                    json.dump(payload, fp)
                """
                for dict_ in payload:
                    df = pd.Series(dict_).to_frame()
                    df = df.transpose()
                    dataset.append(df)
                    """
                    data = pd.DataFrame.from_records(payload)
                    print(data.size)
                    dataset.append(data)
                    print(dataset)
                    """
                logger.debug("Number of records found:{num}".format(num=len(dataset)))

            except requests.exceptions.RequestException as err1:
                logger.warning(err1)

            except requests.exceptions.HTTPError as err2:
                logger.warning(err2)

            except requests.exceptions.ConnectionError as err3:
                logger.warning(err3)

            except requests.exceptions.Timeout as err4:
                logger.warning(err4)

        # Insert data into database

    fName = "test.csv"
    df = pd.concat(dataset)
    df.to_csv(outputDir + "/" + fName)
    #insert_to_db(df, key)


"""
Function to store data into database
@:parameter
    testCode -> impcTestCode
    animalId -> mouseId
    start -> start page
    size = number of related animal in one page    
"""


def insert_to_db(df, tableName):
    config = configparser.ConfigParser()
    config.read('db_init.INI')
    password = config.get("section_a", "Password")
    host = config.get("section_a", "Port")
    database = config.get("section_a", "Database")

    try:
        engine = create_engine("mysql+mysqlconnector://root:{0}@{1}/{2}".
                               format(password, host, database),
                               pool_recycle=1, pool_timeout=57600,
                               future=True)
        df.to_sql(tableName, engine, if_exists='append', index=False)

    except SQLAlchemyError as e:
        error = str(e.__dict__["orig"])
        logger.warning("Error message: {error}".format(error=error))


def main():
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
    colonyIds = queryColonyId(conn_to_rslims, commands[1])

    urlMap = {i: [] for i in parameterKeys}
    for colonyId in colonyIds:
        logger.info(f"JR number is :{colonyId}")

        impcImage = impc.impcInfo(colonyId, urlMap)
        impcImage.generateURL(0, 50, impcImage, parameterKeys)
        BFS(impcImage.urlMap)

    """
    with open('urls.txt', 'w') as convert_file:
        convert_file.write(json.dumps(urlMap))
    """

    # BFS(urlMap)
    """Close the connection"""
    conn_to_komp.close()
    conn_to_rslims.close()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
