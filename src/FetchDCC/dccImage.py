import logging
import os
from logging.handlers import RotatingFileHandler
import pandas as pd
import requests


"""Setup logger"""

outputDir = "Output"
try:
    os.mkdir(outputDir)

except FileExistsError as e:
    print("File exists")

logger = logging.getLogger(__name__)
FORMAT = "[%(asctime)s->%(filename)s->%(funcName)s():%(lineno)s]%(levelname)s: %(message)s"
logging.basicConfig(format=FORMAT, filemode="w", level=logging.DEBUG, force=True)
logging_filename = 'App.log'
handler = RotatingFileHandler(logging_filename, maxBytes=100000000, backupCount=10)
handler.setFormatter(logging.Formatter(FORMAT))
logger.addHandler(handler)

nameMap = {"IMPC_EYE_001": "IMPC_EYE_050_001", "IMPC_CSD_001": "IMPC_CSD_085_001",
           "IMPC_ECG_001": "IMPC_ECG_025_001"}

EBI_URL_Template = "https://www.ebi.ac.uk/mi/impc/solr/impc_images/select" \
                   "?q=parameter_stable_id:%20IMPC_EYE_050_001%20AND%20phenotyping_center:" \
                   "JAX&wt=json&indent=true&start=401&rows=200"


# result = pd.DataFrame.from_records(urlMap)
# result.to_csv("urls.txt")


class imageInfo:

    def __init__(self, colonyId, animalId, parameterCode):
        self.colonyId = colonyId
        self.animalId = animalId
        self.parameterCode = parameterCode


class impcInfo(imageInfo):
    DCC_URL_Template = "https://api.mousephenotype.org/media/J?start={start}&resultsize={size}"
    animalIdTemplate = "https://api.mousephenotype.org/media/J?parameterKey={parameterCode}&animalName={" \
                       "mouseId}&start={start}&resultsize={size}"
    colonyIdTemplate = "https://api.mousephenotype.org/media/J?genotype={JRNumber}&start={start}&resultsize={size}"
    parameterKeyTemplate = "https://api.mousephenotype.org/media/J?parameterKey={parameterCode}"
    nameMap = {"IMPC_EYE_001": "IMPC_EYE_050_001", "IMPC_CSD_001": "IMPC_CSD_085_001",
               "IMPC_ECG_001": "IMPC_ECG_025_001"}

    baseURL = ""
    """
    @attribute
        tableName: Table in the schema to be insert
    """

    def __init__(self, colonyId, animalId, parameterCode, tableName):
        super().__init__(colonyId, animalId, parameterCode)
        self.tableName = tableName

    def generateURL(self, start, size):

        parameterCodes = []
        for key in nameMap.keys():
            parameterCodes.append(nameMap[key])

        # print(parameterCodes)
        logger.info(f"Final number of parameter codes are :{len(parameterCodes)}")
        for parameterCode in parameterCodes:
            url = self.queryByColonyId.format(parameterKey=parameterCode, JRNumber=self.colonyId,
                                              start=start, size=size)
            logger.debug(f"Resulting url is :{url}")
            self.urlList.append(url)

    """
    Function to get all results related to one specific parameter key
    @:param
        :parameterKey: DCC parameter test code, e.g. IMPC_EYE_050_001
    """

    def queryByParameterKey(self) -> list:

        if not self.parameterCode or self.parameterCode not in nameMap:
            logger.error("No such parameter key")
            return []

        result = []
        numRecords = 0
        url = self.parameterKeyTemplate.format(parameterCode=self.parameterCode)
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
            logger.warning("Error message: {error}".format(error=error))

        except requests.exceptions.HTTPError as err2:
            error = str(err2.__dict__["orig"])
            logger.warning("Error message: {error}".format(error=error))

        except requests.exceptions.ConnectionError as err3:
            error = str(err3.__dict__["orig"])
            logger.warning("Error message: {error}".format(error=error))

        except requests.exceptions.Timeout as err4:
            error = str(err4.__dict__["orig"])
            logger.warning("Error message: {error}".format(error=error))

        return result

    """
    Function to get all results related to one specific colonyId
    @:param
        colonyId: JAX mouse colonyId
    """

    def queryByColonyId(self) -> list:

        if not self.colonyId:
            logger.error("No colonyId found")
            return []

        (start, dest) = self.getLastPage()
        url = self.colonyIdTemplate.format(JRNumber=self.colonyId, start=start, size=dest)
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
            logger.warning("Error message: {error}".format(error=error))

        except requests.exceptions.HTTPError as err2:
            error = str(err2.__dict__["orig"])
            logger.warning("Error message: {error}".format(error=error))

        except requests.exceptions.ConnectionError as err3:
            error = str(err3.__dict__["orig"])
            logger.warning("Error message: {error}".format(error=error))

        except requests.exceptions.Timeout as err4:
            error = str(err4.__dict__["orig"])
            logger.warning("Error message: {error}".format(error=error))

        return result

    """
       Function to get all results related to one specific mouse
       @:param
           animalId: JAX mouse id/organism id
    """

    def queryByAnimalId(self) -> pd.DataFrame:
        pass

    """
    Recursive function to find last page of 
    Pseudo Code:
        if payload["total"] == 0
            return (0, 0)
    
        else if payload["total"] < size:
            return (start, size)
    
        else:
            return findLastPage(size + 1, size*(multiplier + 1))
    """
    def getLastPage(self) -> tuple:
        pass


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
