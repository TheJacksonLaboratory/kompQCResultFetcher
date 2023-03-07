# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import logging
import os
import random
import socket
import sys
from logging.handlers import RotatingFileHandler

import pandas as pd
from sqlalchemy import create_engine
from urllib3.connection import HTTPConnection

import Model.IMPC as impc
from db_init import db_init as db
import Omero.OmeroImage as Omero

outputDir = "/Users/chent/Desktop/KOMP_Project/FetchDCCResult/docs/Output"

try:
    os.mkdir(outputDir)

except FileExistsError as e:
    print("File exists")

"""Setup logger"""

logger = logging.getLogger("Core")
FORMAT = "[%(asctime)s->%(filename)s->%(funcName)s():%(lineno)s]%(levelname)s: %(message)s"
logging.basicConfig(format=FORMAT, filemode="w", level=logging.DEBUG, force=True)
logging_filename = outputDir + "/" + 'App.log'
handler = RotatingFileHandler(logging_filename, maxBytes=10000000000, backupCount=10)
handler.setFormatter(logging.Formatter(FORMAT))
logger.addHandler(handler)

"""
Config a .txt file, with animalId/parameterCode/colonyId on each line.
1. If the line is a colonyId -> query by JR number
2. If the line is an animalId -> query by animal id
3. If the line is a IMPC code -> query by parameter key
"""

'''
def main():
    """Set higher timeout"""
    HTTPConnection.default_socket_options = (HTTPConnection.default_socket_options + [
        (socket.SOL_SOCKET, socket.SO_SNDBUF, 1000000),
        (socket.SOL_SOCKET, socket.SO_RCVBUF, 1000000)
    ])

    """Setup Database connection"""
    conn_to_rslims = db.init("rslims")
    conn_to_komp = db.init("komp")

    """"Read sql script"""
    here = os.path.dirname(os.path.abspath(__file__))
    sqlFile = os.path.join(here, '../docs/Config/dccImage.sql')
    fptr = open(sqlFile, "r")
    sqlFile = fptr.read()
    commands = sqlFile.split(";")

    # Query database
    parameterKeys = db.queryParameterKey(conn_to_rslims, commands[1])
    print(len(parameterKeys))
    logger.debug("Number of impc test codes is {size}".format(size=len(parameterKeys)))
    colonyIds = db.queryColonyId(conn_to_rslims, commands[2])
    logger.debug("Number of JR number is {size}".format(size=len(colonyIds)))

    """
    testImageInfo = impcInfo()
    testImageInfo.setParameterKey(parameterKeys[0])
    testImageInfo.setColonyId(colonyIds[0])
    print(testImageInfo.getParameterKey())
    
    for parameterKey in parameterKeys:
        print(parameterKey)
        newImage = impcInfo("", "", parameterKey, "komp.dccimages")
        result = newImage.getByParameterKey()
        insert_to_db(result, "dccimages")

    """

    """
    argv[1], argv[2]: (website name, file name)
    Usage eg:
        python App.py [-e] [-i] [-r] test.txt
        -e -> Retrieve result from EBI
        -i -> Retrieve result from IMPC
        -r -> Generate Report 
    """

    """User wants to query EBI"""
    if sys.argv[1] == "-e":

        if sys.argv[2] == "image":
            filePtr = sys.argv[3]
            with open(filePtr, "r") as f:
                lines = f.readlines()

            for line in lines:
                line = line.split(":")
                logger.debug(f"Reading {line}")
                if line[0] == "Parameter Key":
                    newImage = ebiInfo("", "", line[1].strip(), "ebiimages")
                    result = newImage.getByParameterKey(0, 2 ** 31 - 1)
                    logger.debug("Number of records found:{size}".format(size=len(result)))
                    db.insert_to_db(result, newImage.tableName)

        elif sys.argv[2] == "procedure":
            #Pending implementation in the module
            pass

    """User wants to query IMPC"""
    if sys.argv[1] == "-i":

        if sys.argv[2] == "image":
            filePtr = sys.argv[3]
            with open(filePtr, "r") as f:
                lines = f.readlines()

            lineNumber = 0
            for line in lines:
                lineNumber = lineNumber + 1
                line = line.split(":")
                logger.info(f"Reading {line}")

                if line[0] == "Parameter Key":
                    logger.info("Query by {key}".format(key=line[0]))
                    #newImage = impcInfo("dccimages", parameterKey=line[1].strip())
                    result = impc.filter_image_by(parameterKey=line[1].strip(), start=0, resultsize=2 ** 31 - 1)
                    logger.debug("Number of records found:{size}".format(size=len(result)))
                    df = pd.concat(result)
                    df.to_csv(outputDir + "/" + "demo.csv")
                    #db.insert_to_db(result, newImage.tableName)

                elif line[0] == "AnimalId":
                    logger.info("Query by {key}".format(key=line[0]))
                    #newImage = impcInfo("dccimages", animalId=line[1].strip())
                    result = impc.filter_image_by(animalId=line[1].strip(), start=0, resultsize=2 ** 31 - 1)
                    #print(result)
                    logger.debug("Number of records found:{size}".format(size=len(result)))
                    db.insert_to_db(result, newImage.tableName)

                elif line[0] == "JR Number":
                    logger.info("Query by {key}".format(key=line[0]))
                    #newImage = impcInfo("dccimages", colonyId=line[1].strip())
                    result = impc.filter_image_by(colonyId=line[1].strip(), start=0, resultsize=2 ** 31 - 1)
                    #print(result)
                    logger.debug("Number of records found:{size}".format(size=len(result)))
                    db.insert_to_db(result, newImage.tableName)

                else:
                    logger.error("Invalid input file")
                    return

        elif sys.argv[2] == "procedure":
            #Pending implementation
            pass

    """User wants to generate report based on input files"""
    if sys.argv[1] == "-r":
        sql = """SELECT DISTINCT OrganismID, Output.ExternalID AS IMPCCODE, OutputValue AS FileName, _ProcedureInstance_key, url  -- url is at the DCC
                    FROM
                        Organism
                    INNER JOIN
                        OrganismStudy USING (_Organism_key)
                    INNER JOIN
                        ProcedureInstanceOrganism USING (_Organism_key)
                    INNER JOIN
                        ProcedureInstance USING (_ProcedureInstance_key)
                    INNER JOIN
                        OutputInstanceSet USING (_ProcedureInstance_key)
                    INNER JOIN
                        OutputInstance USING (_OutputInstanceSet_key)
                    INNER JOIN
                        Output USING (_Output_key)
                    LEFT OUTER JOIN
                        komp.dccimages ON (OrganismID = komp.dccimages.animalName AND komp.dccimages.parameterKey = Output.ExternalID)
                    WHERE
                        ProcedureInstance._ProcedureDefinitionVersion_key NOT IN ( 197 ) AND
                        _ProcedureStatus_key = 5 
                    AND
                        (_LevelTwoReviewAction_key=14 ) -- OR _LevelTwoReviewAction_key=13)
                    AND 
                        _DataType_key=7
                    AND 
                        (OutputValue IS NOT NULL AND CHAR_LENGTH(OutputValue) > 0)
                    AND 
                        Output.ExternalID = {parameterKey}
                    AND 
                        _Study_key IN (27,57,28)
                    AND 
                        url IS NULL;"""

        filePtr = sys.argv[2]
        with open(filePtr, "r") as f:
            lines = f.readlines()
            for line in lines:
                line = line.split(":")
                logger.debug(f"Reading {line}")
                if line[0] == "Parameter Key":
                    # print(sql.format(parameterKey = line[1].strip()))
                    missingFiles = db.getMissingFiles(conn_to_rslims,
                                                      sql.format(parameterKey=f'"{line[1].strip()}"'))
                    logger.info("Number of missing files associate with {parameterCode} is {n}".
                                format(parameterCode=line[1], n=len(missingFiles)))
                    # print(type(missingFiles))
                    fName = line[1].strip() + ".csv"
                    db.generateMissingReport(missingFiles, fName)

        #db.wrap(outputDir)

    else:
        logger.warning("Illegal input argument detected")

    # Close the connection
    conn_to_komp.close()
    conn_to_rslims.close()

'''


def main():
    """Set higher timeout"""
    HTTPConnection.default_socket_options = (HTTPConnection.default_socket_options + [
        (socket.SOL_SOCKET, socket.SO_SNDBUF, 1000000),
        (socket.SOL_SOCKET, socket.SO_RCVBUF, 1000000)
    ])

    parameterKey = "IMPC_EYE_050_001"
    result = impc.filter_image_by(parameterKey=parameterKey, start=0, resultsize=2 ** 31 - 1)
    user = "root"
    password = "Ql4nc,tzjzsblj"
    server = "localhost"
    database = "KOMP_Training"
    engine = create_engine("mysql+mysqlconnector://{0}:{1}@{2}/{3}".
                           format(user, password, server, database),
                           pool_recycle=3600,
                           pool_timeout=57600,
                           future=True)
    impc.insert_to_db(dataset=result, engine=engine, table="dccImages")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
