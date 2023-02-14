import logging
import os
import sys
from logging.handlers import RotatingFileHandler
import pandas as pd
import requests
from urllib.parse import urlencode, urlunsplit, quote_plus

nameMap = {"IMPC_EYE_001": "IMPC_EYE_050_001", "IMPC_CSD_001": "IMPC_CSD_085_001",
           "IMPC_ECG_001": "IMPC_ECG_025_001"}

EBI_URL_Template = "https://www.ebi.ac.uk/mi/impc/solr/impc_images/select" \
                   "?q=parameter_stable_id:%20IMPC_EYE_050_001%20AND%20phenotyping_center:" \
                   "JAX&wt=json&indent=true&start=401&rows=200"

class imageInfo:

    def __init__(self, colonyId, animalId, parameterKey):
        self.colonyId = colonyId
        self.animalId = animalId
        self.parameterKey = parameterKey


class impcInfo(imageInfo):
    nameMap = {"IMPC_EYE_001": "IMPC_EYE_050_001", "IMPC_CSD_001": "IMPC_CSD_085_001",
               "IMPC_ECG_001": "IMPC_ECG_025_001"}

    filters = ["parameterKey", "genotype", "start", "resultsize"]

    """
    @attribute
        tableName: Table in the schema to be insert
    """

    def __init__(self, colonyId, animalId, parameterKey, tableName):
        super().__init__(colonyId, animalId, parameterKey)
        self.tableName = tableName

    """
    Function to generate desired query url 
    @:param
    filter: type of filter you want to add to url
    *args: 
    """

    def generateURL(self, *args) -> str:

        parameters = {}
        for i in range(len(args)):
            parameters[self.filters[i]] = args[i]

        query = urlencode(query=parameters, doseq=True)
        link = urlunsplit(("https", "api.mousephenotype.org", "/media/J", query, ""))
        print(link)

        return link

    """
    Function to get all results related to one specific parameter key
    @:param
        :parameterKey: DCC parameter test code, e.g. IMPC_EYE_050_001
    """

    def getByParameterKey(self, *args) -> list:

        if not self.parameterKey:
            print("No such parameter key")
            return []

        result = []
        numRecords = 0
        # url = self.parameterKeyTemplate.format(parameterCode=self.parameterKey)
        url = self.generateURL(self.parameterKey, args)
        try:
            response = requests.get(url)
            payload = response.json()
            '''Data found'''
            if payload["total"] > 0:
                numRecords += payload["total"]
                colNames = payload["mediaFiles"][0].keys()
                for dict_ in payload["mediaFiles"]:
                    data = pd.Series(dict_).to_frame()
                    data = data.transpose()
                    data.columns = colNames
                    result.append(data)

        except requests.exceptions.RequestException as err1:
            error = str(err1.__dict__["orig"])
            print(error)

        except requests.exceptions.HTTPError as err2:
            error = str(err2.__dict__["orig"])
            print(error)

        except requests.exceptions.ConnectionError as err3:
            error = str(err3.__dict__["orig"])
            print(error)

        except requests.exceptions.Timeout as err4:
            error = str(err4.__dict__["orig"])
            print(error)

        return result

    """
    Function to get all results related to one specific colonyId
    @:param
        colonyId: JAX mouse colonyId
    """

    def getByColonyId(self, *args) -> list:

        if not self.colonyId:
            print("No colonyId found")
            return []

        url = self.generateURL(self.colonyId, args)
        result = []
        try:
            response = requests.get(url)
            payload = response.json()
            '''Data found'''
            if payload["total"] > 0:
                colNames = payload["mediaFiles"][0].keys()
                for dict_ in payload["mediaFiles"]:
                    data = pd.Series(dict_).to_frame()
                    data = data.transpose()
                    data.columns = colNames
                    result.append(data)

        except requests.exceptions.RequestException as err1:
            error = str(err1.__dict__["orig"])
            print(error)

        except requests.exceptions.HTTPError as err2:
            error = str(err2.__dict__["orig"])
            print(error)

        except requests.exceptions.ConnectionError as err3:
            error = str(err3.__dict__["orig"])
            print(error)

        except requests.exceptions.Timeout as err4:
            error = str(err4.__dict__["orig"])
            print(error)

        return result

    """
       Function to get all results related to one specific mouse
       @:param
           animalId: JAX mouse id/organism id
    """

    def getByAnimalId(self, *args) -> list:
        if not self.animalId:
            print("Invalid Input")
            return []

        url = self.generateURL(self.animalId, *args)
        result = []
        try:
            response = requests.get(url)
            payload = response.json()
            '''Data found'''
            if payload["total"] > 0:
                colNames = payload["mediaFiles"][0].keys()
                for dict_ in payload["mediaFiles"]:
                    data = pd.Series(dict_).to_frame()
                    data = data.transpose()
                    data.columns = colNames
                    result.append(data)

        except requests.exceptions.RequestException as err1:
            error = str(err1.__dict__["orig"])
            print(error)

        except requests.exceptions.HTTPError as err2:
            error = str(err2.__dict__["orig"])
            print(error)

        except requests.exceptions.ConnectionError as err3:
            error = str(err3.__dict__["orig"])
            print(error)

        except requests.exceptions.Timeout as err4:
            error = str(err4.__dict__["orig"])
            print(error)

        return result


class ebiInfo(imageInfo):
    '''
    EBI images table mapping
    Symbol => allele_symbol
    AnimalID => external_sample_id
    DOB => date_of_birth
    ImpcCode => parameter_stable_id
    JR => colony_id
    Sex => sex
    DownLoadFilePath = download_url
    JPEG = jpeg_url
    ExperimentName = experiment_source_id
    '''

    """
       @attribute
           tableName: Table in the schema to be insert
       """

    def __init__(self, colonyId, animalId, parameterCode, tableName):
        super().__init__(colonyId, animalId, parameterCode)
        self.tableName = tableName

    def mapping(self, parameterKeys):
        pass
