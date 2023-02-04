import os
import json
import requests
import sqlalchemy
import numpy as np
import pandas as pd
import logging
import mysql.connector


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