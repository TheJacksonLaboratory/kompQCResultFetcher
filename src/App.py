# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import logging
import os
import socket
import sys
from logging.handlers import RotatingFileHandler

from urllib3.connection import HTTPConnection

from Model.dccImage import impcInfo, ebiInfo
from db_init import db_init as db

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
Input a .txt file, with animalId/parameterCode/colonyId on each line.
1. If the line is a colonyId -> query by JR number
2. If the line is an animalId -> query by animal id
3. If the line is a IMPC code -> query by parameter key
"""


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
    sqlFile = os.path.join(here, 'db_init/dccImage.sql')
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
    for parameterKey in parameterKeys:
        print(parameterKey)
        newImage = impcInfo("", "", parameterKey, "komp.dccimages")
        result = newImage.getByParameterKey()
        insert_to_db(result, "dccimages")

    """

    """
    argv[1], argv[2]: (website name, file name)
    -e -> EBI
    -i -> IMPC
    """
    if sys.argv[1] == "-e":

        filePtr = sys.argv[2]
        with open(filePtr, "r") as f:
            lines = f.readlines()

        print(lines)

        for line in lines:
            line = line.split(":")
            logger.debug(f"Reading {line}")
            if line[0] == "Parameter Key":
                newImage = ebiInfo("", "", line[1].strip(), "dccimages")
                result = newImage.getByParameterKey(0, 2 ** 31 - 1)
                logger.debug("Number of records found:{size}".format(size=len(result)))
                db.insert_to_db(result, "ebiimages")

    elif sys.argv[1] == "-i":
        filePtr = sys.argv[2]
        with open(filePtr, "r") as f:
            lines = f.readlines()

        print(lines)

        for line in lines:
            line = line.split(":")
            logger.debug(f"Reading {line}")
            if line[0] == "Parameter Key":
                newImage = impcInfo("", "", line[1].strip(), "ebiimages")
                # newImage = ebiInfo("", "", line[1].strip(), "test")
                result = newImage.getByParameterKey("", "", 0, 2 ** 31 - 1)
                # print(result)
                logger.debug("Number of records found:{size}".format(size=len(result)))
                db.insert_to_db(result, "dccimages")

    else:
        logger.warning("Illegal input argument detected")

    # Close the connection
    conn_to_komp.close()
    conn_to_rslims.close()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
