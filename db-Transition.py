import csv
from urllib.request import urlopen
import io
import json
from pymongo import MongoClient
from dateutil.parser import parse
from datetime import datetime

# In order for this to work, you need to run hmsParser.py FIRST

# Connect to both MongoDB collections -- milestones and milestonesCons
localClient = MongoClient("mongodb://username:password@localhost/LINCS")
localDb = localClient["LINCS"]
localMd = localDb["milestones"]

masterClient = MongoClient("mongodb://username:password@master/LINCS")
masterDb = masterClient["LINCS"]
masterMd = masterDb["milestones"]
masterCons = masterDb["milestonesCons"]
masterPres = masterDb["milestones-PreRelease"]

lorettaClient = MongoClient("mongodb://username:password@loretta/LINCS")
lorettaDb = lorettaClient["LINCS"]
lorettaMd = lorettaDb["milestones"]
# localMd.drop()

# for doc in lorettaMd.find({}):
#    localMd.insert(doc)
# for doc in masterMd.find({}):
#    localMd.insert(doc)

# Use to update production database once local is stable
# lorettaMd.drop()
# for doc in localMd.find({}):
#    lorettaMd.insert(doc)

# Use to update pre-release db in master
masterPres.drop()
for doc in masterCons.find({}):
    masterPres.insert(doc)
for doc in lorettaMd.find({}):
    masterPres.insert(doc)
