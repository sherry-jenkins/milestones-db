import csv
from urllib.request import urlopen
import io
import json
from pymongo import MongoClient
from dateutil.parser import parse
from datetime import datetime

# In order for this to work, you need to run hmsParser.py FIRST

# Connect to both MongoDB collections -- milestones and milestonesCons
client = MongoClient("mongodb://username:password@master/LINCS")
db = client["LINCS"]
md = db["milestones"]
mdC = db["milestonesCons"]
mdC.drop()

# Consolidated KINOMEscan assay
kinomeDict = {}
kinomeDict["assay"] = "KINOMEscan"
kinomeDict["assay-info"] = "The KINOMEscan assay platform is based on a competition binding assay that is run for a compound of interest against each of a panel of 317 to 456 kinases. The assay has three components: a kinase-tagged phage, a test compound, and an immobilized ligand that the compound competes with to displace the kinase. The amount of kinase bound to the immobilized ligand is determined using quantitative PCR of the DNA tag.  Results for each kinase are reported as \"Percent of control\", where the control is DMSO and where a 100% result means no inhibition of kinase binding to the ligand in the presence of the compound, and where low percent results mean strong inhibition. The KINOMEscan data are presented graphically on TREEspot Kinase Dendrograms (http://www.discoverx.com/services/drug-discovery-development-services/treespot-data-analysis). For this study, HMS LINCS investigators have graphed results for kinases classified as 35 \"percent of control\" (in the presence of the compound, the kinase is 35% as active for binding ligand in the presence of DMSO), 5 \"percent of control\" and 1 \"percent of control\"."
kinomeDict["center"] = "HMS-LINCS"
kinomeDict["phase"] = "LP1"
kinomeDict["release-link"] = "http://lincs.hms.harvard.edu/data/kinomescan/"
kinomeProteinNames = []
kinomeProteins = []
kinomePerts = []
# Init meta dicts
kinomePertMeta = { "count-type": [{ "count": 0, "type": "small molecules" }] }
kinomeProtMeta = { "count": 0, "type": "proteins" }
# Init with an old date. Will be updated.
kinomeReleaseDate = {
"date": datetime(2002,12,25,0,0,0),
"releaseLevel": 1 }

# Consolidated KiNativ assay
kinativDict = {}
kinativDict["assay"] = "KiNativ"
kinativDict["assay-info"] = "The KiNativ assay platform is based on the use of biotinylated acyl phosphates of ATP or ADP that act as probes by reacting with protein kinases on conserved lysine residues in the ATP binding pocket to covalently attach a biotin moiety.  Using these probes, cell lysates treated with a kinase inhibitor can be labeled with BHAcATP or BHAcADP, with kinases inhibited by the compound of interest exhibiting reduced or no labeling.  This is followed by digestion with trypsin, isolation of biotinylated peptides, and analysis by mass spectrometry to determine the extent of labeling of peptides from each kinase."
kinativDict["center"] = "HMS-LINCS"
kinativDict["phase"] = "LP1"
kinativDict["release-link"] = "http://lincs.hms.harvard.edu/db/datasets/?search=&extra_form_shown=true&dataset_type=KiNativ"
kinativProteinNames = []
kinativProteins = []
kinativCellLineNames = []
kinativCellLines = []
kinativPerts = []
# Init meta dicts
kinativPertMeta = {"count-type": [{ "count": 0, "type": "small molecules" }]}
kinativProtMeta = { "count": 0, "type": "proteins" }
kinativLineMeta = { "count": 0, "type": "cell lines" }
# Init with an old date. Will be updated.
kinativReleaseDate = {
"date": datetime(2002,12,25,0,0,0),
"releaseLevel": 1 }

# Copy all documents from original milestones DB
# Good to keep the separate database as well.
for doc in md.find():
    mdC.insert(doc)

# Iterate through all KINOMEscan assay entries in MongoDB
for kinomeDoc in mdC.find({'assay':{'$regex':'KINOMEscan'}}):
    # Iterate through proteins, append unique for consolidated dict
    for proteinDoc in kinomeDoc["proteins"]:
        protName = proteinDoc["name"]
        if protName not in kinomeProteinNames:
            kinomeProteinNames.append(protName)
            kinomeProteins.append(proteinDoc)
    # Iterate through perturbagens, append for consolidated dict
    for pertDoc in kinomeDoc["perturbagens"]:
        kinomePerts.append(pertDoc)
    # Add perturbagens are unique in this case, so increment count
    kinomePertMeta["count-type"][0]["count"] = \
            kinomePertMeta["count-type"][0]["count"] + 1
    for dateDoc in kinomeDoc["release-dates"]:
        # Check if date is later than current one. Replace if true
        if dateDoc["date"] > kinomeReleaseDate["date"]:
            kinomeReleaseDate["date"] = dateDoc["date"]

#Iterate through all KiNativ assay entries in MongoDB
for kinativDoc in mdC.find({'assay':{'$regex':'KiNativ'}}):
    # Iterate through proteins, append unique for consolidated dict
    for proteinDoc in kinativDoc["proteins"]:
        protName = proteinDoc["name"]
        if protName not in kinativProteinNames:
            kinativProteinNames.append(protName)
            kinativProteins.append(proteinDoc)
    # Iterate through cell lines, append unique for consolidated dict
    for lineDoc in kinativDoc["cell-lines"]:
        lineName = lineDoc["name"]
        if lineName not in kinativCellLineNames:
            kinativCellLineNames.append(lineName)
            kinativCellLines.append(lineDoc)
    # Iterate through perturbagens, append for consolidated dict
    for pertDoc in kinativDoc["perturbagens"]:
        kinativPerts.append(pertDoc)
    # Add perturbagens are unique in this case, so increment count
    kinativPertMeta["count-type"][0]["count"] = \
            kinativPertMeta["count-type"][0]["count"] + 1
    for dateDoc in kinativDoc["release-dates"]:
        # Check if date is later than current one. Replace if true
        if dateDoc["date"] > kinativReleaseDate["date"]:
            kinativReleaseDate["date"] = dateDoc["date"]

# Remove individual KINOMEscan and KiNativ documents in MongoDB
mdC.remove({'assay':{'$regex':'KINOMEscan'}})
mdC.remove({'assay':{'$regex':'KiNativ'}})

# Build the rest of final kinomeDict to be inserted
kinomeProtMeta["count"] = len(kinomeProteinNames)
kinomeProtMetaArr = [kinomeProtMeta]
kinomeDict["proteins-meta"] = kinomeProtMetaArr
kinomeDict["proteins"] = kinomeProteins
kinomeDict["perturbagens"] = kinomePerts
kinomeDict["perturbagens-meta"] = kinomePertMeta
kinomeReleaseDatesArr = [kinomeReleaseDate]
kinomeDict["release-dates"] = kinomeReleaseDatesArr

# Build the rest of final kinativDict to be inserted
kinativProtMeta["count"] = len(kinativProteinNames)
kinativLineMeta["count"] = len(kinativCellLineNames)
kinativProtMetaArr = [kinativProtMeta]
kinativLineMetaArr = [kinativLineMeta]
kinativDict["proteins-meta"] = kinativProtMetaArr
kinativDict["cell-lines-meta"] = kinativLineMetaArr
kinativDict["proteins"] = kinativProteins
kinativDict["cell-lines"] = kinativCellLines
kinativDict["perturbagens"] = kinativPerts
kinativDict["perturbagens-meta"] = kinativPertMeta
kinativReleaseDatesArr = [kinativReleaseDate]
kinativDict["release-dates"] = kinativReleaseDatesArr

# Insert two final dicts into MongoDB
mdC.insert(kinomeDict)
mdC.insert(kinativDict)
