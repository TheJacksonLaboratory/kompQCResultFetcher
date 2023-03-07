from datetime import datetime
from urllib.parse import urlencode, urlunsplit
import pandas as pd
import requests
from typing import Optional

import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, MetaData, Engine
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.testing.schema import Table


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
        parameterKey:IMPC code, e.g. IMPC_EYE_050_001
        colonyId: JR number of an animal. usually starts with "JR"
        animalId: Name of animal
        strain: ID of a given strain
        procedureKey:
        status:
        phase:
        pipelineKey:
        xmlFileName:
        start: start index of the querying
        resultsize: number of json object displayed on one page
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
            error = str(err1.__dict__)
            print(error)

        except requests.exceptions.ConnectionError as err2:
            error = str(err2.__dict__)
            print(error)

        except requests.exceptions.Timeout as err3:
            error = str(err3.__dict__)
            print(error)

        except requests.exceptions.RequestException as err4:
            error = str(err4.__dict__)
            print(error)

    return result


def filter_procedure_by() -> list:
    return []


def filter_zipFile_by(centre: Optional[str] = None,
                      updatedSinceDate: Optional[datetime] = None,
                      ignoreWarnings: Optional[bool] = True,
                      validationIssues: Optional[bool] = True,
                      xmlErrors: Optional[bool] = True,
                      zipErrors: Optional[bool] = True,
                      start: Optional[int] = None,
                      resultsize: Optional[int] = None) -> list:
    result = []
    filters = {}

    if start < 0 or start > 2 ** 31 - 1 \
            or resultsize > 2 ** 31 - 1 or resultsize <= 0:
        raise ValueError("Invalid start or result size")

    else:

        """Generate the url"""
        if centre is not None:
            filters["centre"] = centre

        if updatedSinceDate is not None:
            filters["updateSinceDate"] = updatedSinceDate

        if ignoreWarnings:
            filters["ignoreWarnings"] = ignoreWarnings

        if start:
            filters["start"] = str(start)

        if resultsize:
            filters["resultsize"] = str(resultsize)

        if validationIssues:
            filters[" validationIssues"] = validationIssues

        if xmlErrors:
            filters["xmlErrors"] = xmlErrors

        if zipErrors:
            filters["zipErrors"] = zipErrors

        query = urlencode(query=filters, doseq=True)
        url = urlunsplit(("https", "api.mousephenotype.org", "/tracker/centre/zip", query, ""))
        print(url)

        """Get data back from impc"""
        try:
            response = requests.get(url)
            payload = response.json()
            print(payload)
            #Getting data pending implementation
            
        except requests.exceptions.HTTPError as err1:
            error = str(err1.__dict__)
            print(error)

        except requests.exceptions.ConnectionError as err2:
            error = str(err2.__dict__)
            print(error)

        except requests.exceptions.Timeout as err3:
            error = str(err3.__dict__)
            print(error)

        except requests.exceptions.RequestException as err4:
            error = str(err4.__dict__)
            print(error)

    return result


def filter_xml_by() -> list:
    return []


def filter_embryo_by() -> list:
    return []


def getDbServer():
    return 'rslims.jax.org'


def getDbUsername():
    return 'dba'


def getDbPassword():
    return 'rsdba'


def getDbSchema():
    return 'komp'


Base = declarative_base()


class dccImage(Base):
    __tablename__ = "dccImages"

    url = Column(String, primary_key=True)
    animalName = Column(String)
    genotype = Column(String)
    strain = Column(String)
    status = Column(String)
    parameterKey = Column(String)
    phase = Column(String)
    xmlFileName = Column(String)
    dateModified = Column(DateTime)

    """
    __mapper_args__ = {
        "primary_key": [url, animalName, genotype, strain, parameterKey,phase, xmlFileName, dateModified]
    }
    """

    def __init__(self, url, animalName, genotype, strain, status, parameterKey, phase, xmlFileName, dateModified):
        self.url = url
        self.animalName = animalName
        self.genotype = genotype
        self.strain = strain
        self.status = status
        self.parameterKey = parameterKey
        self.phase = phase
        self.xmlFileName = xmlFileName
        self.dateModified = dateModified


def insert_to_db(dataset: list, engine: sqlalchemy.engine, table: str) -> None:
    if not dataset:
        raise ValueError("Empty list passed")
        return

    # Check if table exists in the schema
    if not sqlalchemy.inspect(engine).has_table(table):
        # Create table
        try:
            Base.metadata.create_all(engine)
        except SQLAlchemyError as e:
            error = str(e.__dict__)
            print(error)

    # Table found
    Session = sessionmaker(engine)
    for data in dataset:

        print(data)
        del data["procedureKey"]
        # data["dateModified"] = datetime.utcnow()
        data.update({"dateModified": datetime.utcnow()})

        with Session.begin() as session:
            try:
                record = dccImage(**data)
                session.add(record)
                session.commit()

            except SQLAlchemyError as err:
                error = str(err.__dict__)
                print(error)

            finally:
                session.close()
