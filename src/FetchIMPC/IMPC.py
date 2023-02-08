import logging
from logging.handlers import RotatingFileHandler

"""
Skip the animal id, query parameter id, then match with the animal id,
get xml filaneme, aniumal id and JR number(genotypeing)
"""

logger = logging.getLogger(__name__)
FORMAT = "[%(asctime)s->%(filename)s->%(funcName)s():%(lineno)s]%(levelname)s: %(message)s"
logging.basicConfig(format=FORMAT, filemode="w", level=logging.DEBUG, force=True)
logging_filename = 'App.log'
handler = RotatingFileHandler(logging_filename, maxBytes=100000000, backupCount=10)
handler.setFormatter(logging.Formatter(FORMAT))
logger.addHandler(handler)

DCC_URL_Template = "https://api.mousephenotype.org/media/J?start={start}&resultsize={size}"
queryByAnimalId = "https://api.mousephenotype.org/media/J?" \
                  "parameterKey={parameterCode}&animalName={mouseId}&start={start}&resultsize={size}"
queyByColonyId = "https://api.mousephenotype.org/media/J?parameterKey={parameterKey}" \
                 "&genotype={JRNumber}&start={start}&resultsize={size}"

nameMap = {"IMPC_EYE_001": "IMPC_EYE_050_001", "IMPC_CSD_001": "IMPC_CSD_085_001",
           "IMPC_ECG_001": "IMPC_ECG_025_001)"}


# result = pd.DataFrame.from_records(urlMap)
# result.to_csv("urls.txt")


class imageInfo:

    def __init__(self, colonyId):
        self.colonyId = colonyId


"""One image info -> image info of an entire colony id"""


class impcInfo(imageInfo):
    def __init__(self, colonyId, urlMap):
        super().__init__(colonyId)
        self.urlMap = urlMap

    def generateURL(self, start, size, image, impcTestCode):
        parameterCodes = []
        for key in nameMap.keys():
            for i in impcTestCode:
                if i == nameMap[key]:
                    logger.info(f"Parameter key is {i}")
                    parameterCodes.append(nameMap[key])

        #print(parameterCodes)
        logger.info(f"Final number of parameter codes are :{len(parameterCodes)}")
        for parameterCode in parameterCodes:
            url = queyByColonyId.format(parameterKey=parameterCode, JRNumber=image.colonyId,
                                        start=start, size=size)
            logger.debug(f"Resulting url is :{url}")
            image.urlMap[parameterCode].append(url)




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

    def __init__(self, colonyId, urlMap):
        super().__init__(colonyId, urlMap)


    def mapping(self, parameterKeys):

        pass