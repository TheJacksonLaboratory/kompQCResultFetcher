import pandas as pd
import requests

filters = ["parameterKey", "genotype", "start", "rows"]
EBI_URL_Template = "https://www.ebi.ac.uk/mi/impc/solr/impc_images/select?q={filter}" \
                   ":%20{val}%20AND%20phenotyping_center:JAX&wt=json&indent=true&start=" \
                   "{start}&rows={dest}"

keys = {"parameter_stable_id": "ImpcCode", "date_of_birth": "DOB",
        "external_sample_id": "AnimalID", "allele_symbol": "Symbol",
        "download_url": "DownLoadFilePath", "jpeg_url": "JPEG",
        "experiment_source_id": "ExperimentName",
        "colony_id": "JR", "sex": "Sex"}


def getByParameterKey(self, *args) -> list:
    if not self.parameterKey:
        print("No such parameter key")
        return []

    url = self.EBI_URL_Template.format(filter="parameter_stable_id", parameterKey=self.parameterKey,
                                       start=args[0], dest=args[1])
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
        print(len(data["response"]["docs"]))
        self.BFS(data["response"]["docs"], result)
        # print(result)
        return result

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


"""
Function to get all results related to one specific colonyId
@:param
    colonyId: JAX mouse colonyId
"""


def getByColonyId(self, *args) -> list:
    if not self.colonyId:
        return []

    url = self.EBI_URL_Template.format(filter="colony_id", parameterKey=self.colonyId,
                                       start=args[0], dest=args[1])
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
        print(len(data["response"]["docs"]))
        self.BFS(data["response"]["docs"], result)
        # print(result)
        return result

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


"""
Function to get all results related to one specific mouse
@:param
    animalId: JAX mouse id/organism id
"""


def getByOrg(self, *args) -> list:
    pass


"""
   Function to get all results related to one specific mouse
   @:param
       animalId: JAX mouse id
   """


def filter_by_animalId(self, *args) -> list:
    if not self.colonyId:
        return []

    url = self.EBI_URL_Template.format(filter="external_sample_id", parameterKey=self.animalId,
                                       start=args[0], dest=args[1])
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
        print(len(data["response"]["docs"]))
        self.BFS(data["response"]["docs"], result)
        # print(result)
        return result

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


"""
Function to traverse nested json object
@:param
graph: Nested json object 
"""


def BFS(self, graph, result):
    if not graph:
        print("Empty Json Object!")
        return
    # print(graph)
    tempDict_ = {}
    for g in graph:
        for node in g.keys():
            # print(g[node])
            """Ignore the metadata"""
            if isinstance(g[node], list):
                continue
            """
            If we found a match with keys, add it to the temp dict
            """
            if node in self.keys:

                tempDict_[self.keys[node]] = g[node]

        data = pd.Series(tempDict_).to_frame()
        data = data.transpose()
        result.append(data)
        # print(len(result))
