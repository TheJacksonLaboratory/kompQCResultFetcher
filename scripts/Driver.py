# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import configparser
import os
import json
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


def init(schema) -> mysql.connector:

    config = configparser.ConfigParser()
    config.read('db.ini')
    user = config.get("section_a", "User")
    password = config.get("section_a", "Password")
    host = config.get("section_a", "Port")

    try:
        conn = mysql.connector.connect(host=host, user=user, password=password, database=schema)
        print(conn)
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
    impcCode = impc.queryTestCode(conn_to_rslims, commands[0])
    mouseIds = impc.queryMouseId(conn_to_rslims, commands[1], impcCode)
    urlMap = impc.generateURL(0, 50, mouseIds, impcCode)
    #impc.BFS(urlMap)

    #print(urlMap)
    #print(impcCode)
    #print(mouseIds)
    # print(len(mouseIds))

    """Close the connection"""
    conn_to_komp.close()
    conn_to_rslims.close()

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
