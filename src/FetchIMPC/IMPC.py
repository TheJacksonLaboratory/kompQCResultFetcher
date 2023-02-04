import os
import json
from collections import deque

import requests
from sqlalchemy import create_engine, text
import numpy as np
import pandas as pd
import csv
import logging
import mysql.connector
from mysql.connector import errorcode

DCC_URL_Template = "https://api.mousephenotype.org/media/J?start={start}&resultsize={size}"
queryTemplate = "https://api.mousephenotype.org/media/J?" \
                "parameterKey={parameterCode}&animalName={mouseId}&start={start}&resultsize={size}"
"""
@:parameter
testCode -> impcTestCode
animalId -> mouseId
start -> start page
size = number of related animal in one page    
"""

#impcTestCode = {"IMPC_EYE_001", "IMPC_EKG_001"}

"""
Function to get IMPC test code
@:param
    conn: Connection to MySQL database
    sql: SQL statement to be executed 
"""

def queryTestCode(conn, sql) -> list:

    if not conn or not sql:
        print("Empty connection/statement found")
        return []

    result = []
    cursor = conn.cursor(buffered = True, dictionary = True)
    cursor.execute(sql)
    impcTestCode = cursor.fetchall()
    #print(impcTestCode)

    for pair in impcTestCode:
        for key in pair.keys():
            result.append(pair[key])

    return result


"""
Function to get mouse ids; JR number as well as IMPC file names 
@:param
    conn: Connection to MySQL database
    sql: SQL statement to be executed 
"""


def queryMouseId(conn, sql, impcTestCode) -> list:
    if not conn or not sql:
        print("Empty string")
        return []

    mouseId = []
    cursor = conn.cursor(buffered=True, dictionary=True)
    cursor.execute(sql)
    queryResult = cursor.fetchall()

    # print(mouseIds)
    # print(type(mouseIds))

    for row in queryResult:
        if row["TestImpcCode"] in impcTestCode:
            mouseId.append(row["OrganismID"])

    return mouseId


"""
Function to get mouse ids; JR number as well as IMPC file names 
@:param
    conn: Connection to MySQL database
    sql: SQL statement to be executed 
"""


def queryJRNumbers(conn, sql) -> list:
    if not conn or not sql:
        print("Empty string")
        return []

    result = []
    return result


def generateURL(start, size, mouseId, impcTestCode) -> dict:
    # URL = DCC_URL_Template.format(start=start, size=size)
    urlMap = {i: [] for i in impcTestCode}
    for testCode in impcTestCode:
        #print(testCode)
        for m in mouseId:
            url = queryTemplate.format(testCode=testCode, mouseId=m,
                                       start=start, size=size)
            print(url)
            urlMap[testCode].append(url)

    return urlMap


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
    url_set = set()

    for key in testCodes:
        for url in urlMap[key]:
            if url not in url_set:
                print(url)
                try:
                    response = requests.get(url)
                    payload = response.json()
                    data = pd.DataFrame.from_records(payload)
                    dataset.append(data)

                except requests.exceptions.RequestException as err1:
                    print("Request Error")

                except requests.exceptions.HTTPError as err2:
                    print("Http Error")

                except requests.exceptions.ConnectionError as err3:
                    print("Connection Error")

                except requests.exceptions.Timeout as err4:
                    print("Timeout Error")

                url_set.add(url)

        # Insert data into database
        fName = key + ".csv"
        df = pd.concat(dataset)
        df.to_csv(fName, "w")
        insert_to_db(df, key)


"""
Function to store data into database
@:parameter
testCode -> impcTestCode
animalId -> mouseId
start -> start page
size = number of related animal in one page    
"""


def insert_to_db(data, table) -> None:
    pass


def getFailedResults(URL) -> list:

    if not URL:
        return []

    result = []
    try:
        response = requests.get(URL)
        data = response.json()
        with open('data.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        mediaFiles = data["mediaFiles"]
        for mf in mediaFiles:
            print(mf["animalName"])
            if mf["status"] == "fail":
                print("Failed file:" + mf["xmlFileName"])
            print()

    except requests.exceptions.RequestException as err1:
        print(err1.strerror)

    except requests.exceptions.HTTPError as err2:
        print(err2.strerror)

    except requests.exceptions.ConnectionError as err3:
        print(err3.response)

    except requests.exceptions.Timeout as err4:
        print("Request Error")

    return result


'''
dict_ = generateURL(0, 50)
URL = list(dict_.keys())[0]

# print(response.text)
'''
