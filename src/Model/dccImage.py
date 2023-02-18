import logging
from urllib.parse import urlencode, urlunsplit

import pandas as pd
import requests

nameMap = {"IMPC_EYE_001": "IMPC_EYE_050_001", "IMPC_CSD_001": "IMPC_CSD_085_001",
           "IMPC_ECG_001": "IMPC_ECG_025_001"}

logging.getLogger('dccImage').addHandler(logging.NullHandler())

class imageInfo:

    def __init__(self, colonyId, animalId, parameterKey):
        self.colonyId = colonyId
        self.animalId = animalId
        self.parameterKey = parameterKey


class impcInfo(imageInfo):
    filters = ["parameterKey", "genotype", "start", "resultsize"]

    """
    @attribute
        tableName: Table in the schema to be insert
    """

    def __init__(self, colonyId, animalId, parameterKey, tableName):
        super().__init__(colonyId, animalId, parameterKey)
        self.tableName = tableName

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

        """Generate the url"""
        parameters = {"parameterKey": self.parameterKey}
        for i in range(len(args)):
            if args[i] == "":
                #print(args[i])
                continue
            parameters[self.filters[i]] = args[i]

        #print(parameters)
        query = urlencode(query=parameters, doseq=True)
        url = urlunsplit(("https", "api.mousephenotype.org", "/media/J", query, ""))
        logging.debug(f"URL generated is:{url}")
        print(url)

        """Get data back from impc"""
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

        except requests.exceptions.HTTPError as err1:
            error = str(err1.__dict__["orig"])
            logging.warning(error)
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

    """
    Function to get all results related to one specific colonyId
    @:param
        colonyId: JAX mouse colonyId
    """

    def getByColonyId(self, *args) -> list:

        if not self.colonyId:
            print("No colonyId found")
            return []

        result = []
        """Generate the url"""
        parameters = {"genotype": self.colonyId}
        for i in range(len(args)):
            if args[i] == "":
                print(args[i])
                continue
            parameters[self.filters[i]] = args[i]

        print(parameters)
        query = urlencode(query=parameters, doseq=True)
        url = urlunsplit(("https", "api.mousephenotype.org", "/media/J", query, ""))
        print(url)
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

    """
       Function to get all results related to one specific mouse
       @:param
           animalId: JAX mouse id/organism id
    """

    def getByAnimalId(self, *args) -> list:
        if not self.animalId:
            print("Invalid Input")
            return []

        """Generate the url"""
        parameters = {"animalName": self.animalId}
        for i in range(len(args)):
            if args[i] == "":
                print(args[i])
                continue
            parameters[self.filters[i]] = args[i]

        print(parameters)
        query = urlencode(query=parameters, doseq=True)
        url = urlunsplit(("https", "api.mousephenotype.org", "/media/J", query, ""))
        print(url)

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


class ebiInfo(imageInfo):
    """
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
    """
    filters = ["parameterKey", "genotype", "start", "rows"]
    EBI_URL_Template = "https://www.ebi.ac.uk/mi/impc/solr/impc_images/select?q=parameter_stable_id" \
                       ":%20{parameterKey}%20AND%20phenotyping_center:JAX&wt=json&indent=true&start=" \
                       "{start}&rows={dest}"

    keys = {"parameter_stable_id": "ImpcCode", "date_of_birth": "DOB",
            "external_sample_id": "AnimalID", "allele_symbol": "Symbol",
            "download_url": "DownLoadFilePath", "jpeg_url": "JPEG",
            "experiment_source_id": "ExperimentName",
            "colony_id": "JR", "sex": "Sex"}
    """
       @attribute
           tableName: Table in the schema to be insert
       """

    def __init__(self, colonyId, animalId, parameterCode, tableName):
        super().__init__(colonyId, animalId, parameterCode)
        self.tableName = tableName

    """
        Function to get all results related to one specific parameter key
        @:param
            :parameterKey: DCC parameter test code, e.g. IMPC_EYE_050_001
    """

    def getByParameterKey(self, *args) -> list:

        if not self.parameterKey:
            print("No such parameter key")
            return []

        url = self.EBI_URL_Template.format(parameterKey=self.parameterKey, start=args[0], dest=args[1])
        print(url)
        result = []
        try:
            payload = {}
            headers = {
                'Accept': 'application/json',
                'Authorization': 'Basic c3ZjLWxpbXNkYkBqYXgub3JnOnZBJmNlMyhST3pBTA=='
            }
            response = requests.request("GET", url, headers=headers, data=payload)
            data = response.json()
            self.BFS(data["response"]["docs"], result)

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

        print(result)
        return result

    """
    Function to get all results related to one specific colonyId
    @:param
        colonyId: JAX mouse colonyId
    """

    def getByColonyId(self, *args) -> list:
        pass

    """
    Function to get all results related to one specific mouse
    @:param
        animalId: JAX mouse id/organism id
    """

    def getByOrg(self, *args) -> list:
        pass

    """
    Function to traverse nested json object
    @:param
    graph: Nested json object 
    """

    def BFS(self, graph, result):

        if not graph:
            print("Empty Json Object!")
            return
        #print(graph)
        for g in graph:
            tempDict_ = {}
            for node in g.keys():
                #print(g[node])

                """Ignore the metadata"""
                if isinstance(g[node], list):
                    logging.info("Ignore the metadata")
                    continue
                """
                If we found a match with keys, add it to the temp dict
                """
                if node in self.keys:
                    logging.debug(f"Adding {node} now")
                    tempDict_[self.keys[node]] = g[node]

            data = pd.Series(tempDict_).to_frame()
            data = data.transpose()
            result.append(data)
