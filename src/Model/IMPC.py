from datetime import datetime
from urllib.parse import urlencode, urlunsplit
import pandas as pd
import requests
from typing import Optional

import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, MetaData
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def filter_image_by(parameterKey: Optional[str] = None,
                    colonyId: Optional[str] = None,
                    animalId: Optional[str] = None,
                    strain: Optional[str] = None,
                    procedureKey: Optional[str] = None,
                    status: Optional[str] = None,
                    phase: Optional[str] = None,
                    pipelineKey: Optional[str] = None,
                    xmlFileName: Optional[str] = None,
                    start: Optional[int] = None,
                    resultsize: Optional[int] = None) -> list:
    """
    Function to get all results related to one specific parameter key
    @:param
        start: start index of an image
        resultSize: number of json object displayed on one page
    """

    result = []
    filters = {}

    if start < 0 or start > 2 ** 31 - 1 \
            or resultsize > 2 ** 31 - 1 or resultsize <= 0:
        raise ValueError("Invalid start or result size")

    else:
        """Generate the url"""
        if parameterKey is not None:
            filters["parameterKey"] = parameterKey

        if colonyId is not None:
            filters["colonyId"] = colonyId

        if animalId is not None:
            filters["animalId"] = animalId

        if start:
            filters["start"] = str(start)

        if resultsize:
            filters["resultsize"] = str(resultsize)

        if strain:
            filters["strain"] = strain

        if procedureKey:
            filters["procedureKey"] = procedureKey

        if pipelineKey:
            filters["pipelineKey"] = pipelineKey

        if phase:
            filters["phase"] = phase

        if status:
            filters["status"] = status

        if xmlFileName:
            filters["xmlFileName"] = xmlFileName

        query = urlencode(query=filters, doseq=True)
        url = urlunsplit(("https", "api.mousephenotype.org", "/media/J", query, ""))
        print(url)

        """Get data back from impc"""
        try:
            response = requests.get(url)
            payload = response.json()
            '''Data found'''
            if payload["total"] > 0:
                # colNames = payload["mediaFiles"][0].keys()
                for dict_ in payload["mediaFiles"]:
                    result.append(dict_)
                    # print(result)

        except requests.exceptions.HTTPError as err1:
            error = str(err1.__dict__["orig"])
            print(error)

        except requests.exceptions.ConnectionError as err2:
            error = str(err2.__dict__["orig"])
            print(error)

        except requests.exceptions.Timeout as err3:
            error = str(err3.__dict__["orig"])
            print(error)

        except requests.exceptions.RequestException as err4:
            error = str(err4.__dict__["orig"])
            print(error)

    return result


def filter_zipFile_by(centre: Optional[str] = None,
                      updatedSinceDate: Optional[datetime] = None,
                      ignoreWarnings: Optional[bool] = True,
                      validationIssues: Optional[bool] = True,
                      xmlErrors: Optional[bool] = True,
                      zipErrors: Optional[bool] = True) -> list:

    return


def filter_xml_by() -> list:
    return


def filter_embryo_by() -> list:
    return


Base = declarative_base()


def getDbServer():
    return 'rslims.jax.org'


def getDbUsername():
    return 'dba'


def getDbPassword():
    return 'rsdba'


def getDbSchema():
    return 'komp'


class dccImage(Base):
    # __table__ = "dccImages"
    url = Column(String)
    animalName = Column(String)
    genotype = Column(String)
    strain = Column(String)
    parameterKey = Column(String)
    phase = Column(String)
    xmlFileName = Column(String)
    dateModified = Column(DateTime)

    def __init__(self, url, animalName, genotype, strain, parameterKey, phase, xmlFileName, dateModified):
        self.url = url
        self.animalName = animalName
        self.genotype = genotype
        self.strain = strain
        self.parameterKey = parameterKey
        self.phase = phase
        self.xmlFileName = xmlFileName
        self.dateModified = dateModified


def insert_to_db(dataset: list, table: str) -> None:
    if not dataset:
        raise ValueError("Empty list passed")
        return

    '''
    user = getDbUsername()
    password = getDbPassword()
    server = getDbServer()
    database = getDbSchema()
    '''

    user = "root"
    password = "Ql4nc,tzjzsblj"
    server = "localhost"
    database = "KOMP_Training"

    engine = create_engine("mysql+mysqlconnector://{0}:{1}@{2}/{3}".
                           format(user, password, server, database),
                           pool_recycle=3600,
                           pool_timeout=57600,
                           future=True)

    # Check if table exists in the schema
    if not sqlalchemy.inspect(engine).has_table("BOOKS"):
        # Create table
        metadata = MetaData(engine)

    else:
        # Table found
        Session = sessionmaker(engine)
        for image in dataset:

            print(image)
            del image["procedureKey"]
            image["dateModified"] = datetime.utcnow()

            with Session.begin() as session:
                try:
                    session.add(image)

                except SQLAlchemyError as err:
                    error = str(err.__dict__["orig"])
                    print(error)

                finally:
                    session.close()
