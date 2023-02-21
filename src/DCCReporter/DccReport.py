import logging
import os
import random
from datetime import datetime
from logging.handlers import RotatingFileHandler
import pandas as pd
import zipfile
import smtplib
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)


def queryDB(conn, sql) -> list:
    if not conn:
        return []

    result = []
    cursor = conn.cursor(buffered=True, dictionary=True)
    cursor.execute(sql)
    queryResult = cursor.fetchall()

    if not queryResult:
        logger.warning("No missing record found")

    logger.debug("Number of result missing files found:{size}".format(size = len(queryResult)))
    # print(queryResult)
    for row in queryResult:
        data = pd.Series(row).to_frame()
        data = data.transpose()
        print(data)
        result.append(data)

    return result


def generateReport(parameterCodes, fName) -> None:
    if not parameterCodes or not fName:
        print("Unvalid input")
        return

    df = pd.concat(parameterCodes)
    print(df)
    here = "/Users/chent/Desktop/KOMP_Project/FetchDCCResult/docs/Output/"
    outFile = here + fName
    df.to_csv(outFile)
    return


def wrap(filePath):

    fileList = os.listdir(filePath)
    for file in fileList:
        if not file.endswith(".csv"):
            logger.info(f"Removing file {file}")
            fileList.remove(file)

    with zipfile.ZipFile("report.zip", "w") as zipFile:
        logger.info("Compressing")
        for f in fileList:
            zipFile.write(f, compress_type=zipfile.ZIP_DEFLATED)


