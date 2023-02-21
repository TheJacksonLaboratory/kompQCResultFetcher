import logging
import os
import random
from datetime import datetime
from logging.handlers import RotatingFileHandler
import mysql.connector
import pandas as pd
from mysql.connector import errorcode
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

"""
def getDbServer():
    return 'rslims.jax.org'


def getDbUsername():
    return 'dba'


def getDbPassword():
    return 'rsdba'


def getDbSchema():
    return 'komp'

"""
user = "dba"
pwd = "rsdba"
server = "rslims-dev.jax.org"
database = "komp"

outputDir = "/Users/chent/Desktop/KOMP_Project/FetchDCCResult/docs/Output"

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
    try:

        conn = mysql.connector.connect(host=server, user=user, password=pwd, database=schema)
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


def queryParameterKey(conn, sql) -> list:
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
    insertStmt = "INSERT INTO komp.dccQualityIssues (AnimalName, Taskname, TaskInstanceKey, ImpcCode, StockNumber,
    DateDue, Issue) VALUES ( '{0}','{1}',{2},'{3}','{4}','{5}','{6}')". \ format(msg['AnimalName'], msg['TaskName'],
    int(msg['TaskInstanceKey']), msg['ImpcCode'], msg['StockNumber'], msg['DateDue'], msg['Issue'].replace("'", "\""))
    """

    if not dataset:
        logger.warning("No record retrieved!")
        return

    df = pd.concat(dataset)
    if tableName == "dccimages":

        df.drop("procedureKey", axis=1, inplace=True)
        currTime = [datetime.now() for i in range(len(df.index))]
        insertData = df.copy()
        insertData["modifiedTime"] = pd.Series(currTime).values

        try:

            engine = create_engine("mysql+mysqlconnector://{0}:{1}@{2}/{3}".
                                   format(user, pwd, server, database),
                                   pool_recycle=3600,
                                   pool_timeout=57600,
                                   future=True)

            with engine.connect() as conn:
                logger.debug("Getting the column names")
                keys = conn.execute(text("SELECT * FROM komp.dccImages;")).keys()

            insertData.columns = keys
            #print(insertData)
            insertData.to_sql(tableName, engine, if_exists='append', index=False, chunksize=1000)
            insertionResult = engine.connect().execute(text("SELECT * FROM komp.dccImages;"))
            logger.debug(f"Insertion result is:{insertionResult}")
            result = engine.connect().execute(text("SELECT COUNT(*) FROM dccImages;"))
            print(result.first()[0])

        except SQLAlchemyError as err:
            error = str(err.__dict__["orig"])
            logger.error("Error message: {error}".format(error=error))

    elif tableName == "ebiimages":

        try:
            engine = create_engine("mysql+mysqlconnector://{0}:{1}@{2}/{3}".
                                   format(user, pwd, server, database),
                                   pool_recycle=3600,
                                   pool_timeout=57600,
                                   future=True)
            # print(df)
            df.to_sql(tableName, engine, if_exists='append', index=False, chunksize=1000)
            result = engine.connect().execute(text("SELECT COUNT(*) FROM komp.ebiimages;"))
            logger.debug(f"Number of rows in table is {result.first()[0]}")
        except SQLAlchemyError as err:
            error = str(err.__dict__["orig"])
            logger.error("Error message: {error}".format(error=error))

    else:

        logger.warning("Not a valid table!")
        return
