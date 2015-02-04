# -*- coding: utf-8 -*-
"""
Created on Thu Dec 11 21:45:19 2014

@author: atreya
"""

import pandas as pd
from Utils import utils


def getNNP():
    dataPath = utils.getDemoDataPath()+"//trend_nnp.csv"
    data = pd.read_csv(dataPath)
    return data["Trends_NNP"].tolist()


arr_NNP = getNNP()

for nnp in arr_NNP:
    arr = filter(None,nnp.split(" "))



