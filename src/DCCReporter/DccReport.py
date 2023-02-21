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

def queryDB(conn, sql) -> list:

    if not conn:
        return []

    result = []
    cursor = conn.cursor(buffered=True, dictionary=True)
    cursor.execute(sql)
    queryResult = cursor.fetchall()

    #print(queryResult)
    for row in queryResult:
        result.append(row)
    print(result)
    return result


def generateReport(parameterCodes) -> None:
    pass