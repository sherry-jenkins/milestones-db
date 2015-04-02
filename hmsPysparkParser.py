if __name__ == "__main__":

    import csv
    from urllib2 import urlopen
    import io
    import json
    from dateutil.parser import parse
    from pyspark import SparkContext, SparkConf

    confSpark = SparkConf().setAppName('hmsParser')
    conf = confSpark.setMaster('mesos://catherine:5050')
    sc = SparkContext(conf=conf)

    phaseOneIds = ["20001","20002","20003","20004","20006","20007","20008",
    "20009","20010","20011","20012","20013","20014","20015","20016","20017",
    "20020","20021","20022","20023","20024","20025","20026","20027","20028",
    "20029","20030","20031","20032","20033","20034","20035","20036","20037",
    "20038","20039","20040","20041","20042","20043","20044","20045","20046",
    "20047","20048","20049","20050","20051","20052","20053","20054","20055",
    "20056","20057","20058","20059","20060","20061","20062","20063","20064",
    "20065","20066","20067","20068","20069","20070","20071","20072","20073",
    "20074","20075","20076","20077","20078","20079","20080","20081","20082",
    "20083","20084","20085","20086","20087","20088","20089","20090","20091",
    "20092","20093","20094","20095","20096","20097","20098","20099","20100",
    "20101","20102","20103","20104","20105","20106","20107","20108","20109",
    "20110","20111","20112","20113","20114","20115","20116","20117","20118",
    "20119","20121","20124","20125","20126","20127","20128","20129","20130",
    "20131","20132","20133","20134","20135","20136","20120","20137","20138",
    "20139","20140","20146","20147","20148","20149","20150","20151","20152",
    "20153","20154","20155","20156","20157","20158","20159","20160","20161",
    "20162","20163","20164","20165","20166","20167","20168","20169","20170",
    "20171","20172","20173","20174","20175","20176","20177","20178","20179",
    "20180","20181","20182","20183","20184","20185","20186","20187","20188",
    "20189","20190","20191","20192","20193","20194","20195","20196","20197",
    "20198","20199","20200","20201","20202","20203","20204","20206","20207",
    "20209","20210","20211"]

    # Dictionaries for caching lines, sms, and ps
    lineCache = {}
    smCache = {}
    psCache = {}

    def produceResults(ID):
        print(ID)
        emptyDict = {}

        # outD will be the final dict/JSON for the mongoDB database
        outD = emptyDict.copy()

        # Load JSON of Dataset using Harvard"s API
        datasetUrl = "http://lincs.hms.harvard.edu/db/api/v1/dataset/" + \
                str(ID) + "/?format=json"
        reply = urlopen(datasetUrl).read().decode("utf8")
        dataset = json.loads(reply)

        outD["center"] = "HMS-LINCS"
        outD["assay"] = dataset["assayTitle"]
        outD["assay-info"] = dataset["assayDescription"]
        outD["phase"] = "LP1"

        # Change from YYYY-MM-DD to MM/DD/YYYY TZ because I know this works parse and
        # gives the proper time zone --> -400 is in the middle of the U.S
        dateArr = []
        date = dataset["mostRecentUpdate"]
        dateSplit = date.split("-")
        rawDate = dateSplit[1] + "/" + dateSplit[2] + "/" + dateSplit[0]
        cleanDate = parse(rawDate + " -0400")
        releaseDate = {
                "date": cleanDate,
                "releaseLevel": 1
                }
        dateArr.append(releaseDate)

        if dateArr:
            outD["release-dates"] = dateArr

        # Download csv and parse to get associated small molecules, cell lines,
        # and/or proteins
        # Get names of small molecules and IDs of cell lines/proteins
        csvUri = dataset["datapointFile"]["uri"]
        csvResponse = urlopen(csvUri)
        csvI = csvResponse.read().decode("utf-8")
        csvIO = io.StringIO(csvI, newline="")
        cr = csv.reader(csvIO)

        smIds = []
        lineIds = []
        pIds = []

        smArr = []
        cLineArr = []
        pArr = []

        for row in cr:
            if row[0] == "datarecordID":
                continue

            # Get small molecule ID, check if in cache.
            # If not in cache, download, get meta-data, and cache it.
            if row[2] and row[3]:
                strSMId = str(row[2]) + "-" + str(row[3])
                if strSMId not in smIds:
                    smIds.append(strSMId)
                    smDict = emptyDict.copy()
                    smUrl = "http://lincs.hms.harvard.edu/db/api/v1/" + \
                            "smallmolecule/" + strSMId + "/?format=json"
                    if strSMId not in smCache:
                        smReply = urlopen(smUrl).read().decode("utf8")
                        respDict = json.loads(smReply)
                        smCache[strSMId] = respDict
                    else:
                        respDict = smCache[strSMId]
                    smDict["hmsId"] = strSMId
                    smDict["lincsId"] = respDict["smLincsID"]
                    smDict["name"] = respDict["smName"]
                    smDict["type"] = "small molecule"
                    smArr.append(smDict)

            # Get cell line ID, check if in cache.
            # If not in cache, download, get meta-data, and cache it.
            if row[9]:
                strLineId = str(row[9])
                if strLineId not in lineIds:
                    lineIds.append(strLineId)
                    cLineDict = emptyDict.copy()
                    lineUrl = "http://lincs.hms.harvard.edu/db/api/v1/cell/" + \
                            strLineId + "/?format=json"
                    if strLineId not in lineCache:
                        lineReply = urlopen(lineUrl).read().decode("utf8")
                        respDict = json.loads(lineReply)
                        lineCache[strLineId] = respDict
                    else:
                        respDict = lineCache[strLineId]
                    cLineDict["hmsId"] = strLineId
                    cLineDict["name"] = respDict["clName"]
                    if respDict["clCellType"]:
                        cLineDict["type"] = respDict["clCellType"] #Cancer cell lines?
                    if respDict["clOrgan"]:
                        cLineDict["tissue"] = respDict["clOrgan"]
                    if respDict["clOrganism"]:
                        cLineDict["organism"] = respDict["clOrganism"]
                    cLineArr.append(cLineDict)

            # Get protein ID, check if in cache.
            # If not in cache, download, get meta-data, and cache it.
            if row[11]:
                strPId = str(row[11])
                if strPId not in pIds:
                    pIds.append(strPId)
                    pDict = emptyDict.copy()
                    pUrl = "http://lincs.hms.harvard.edu/db/api/v1/protein/" + \
                            strPId + "/?format=json"
                    if strPId not in pCache:
                        pReply = urlopen(pUrl).read().decode("utf8")
                        respDict = json.loads(pReply)
                        pCache[strPId] = respDict
                    else:
                        respDict = pCache[strPId]
                    pDict["hmsId"] = strPId
                    pDict["name"] = respDict["ppName"]
                    if respDict["ppProteinType"]:
                        pDict["type"] = respDict["ppProteinType"]
                    if respDict["ppProteinSource"]:
                        pDict["source"] = respDict["ppProteinSource"]
                    if respDict["ppProteinForm"]:
                        pDict["form"] = respDict["ppProteinForm"]
                    if respDict["ppSourceOrganism"]:
                        pDict["organism"] = respDict["ppSourceOrganism"]
                    pArr.append(pDict)

        smLength = len(smIds)
        lineLength = len(lineIds)
        pLength = len(pIds)

        if smArr:
            outD["perturbagens"] = smArr

        if not smLength == 0:
            pertMeta = emptyDict.copy()
            pertCountArr = []

            countType = {
                    "count": smLength,
                    "type": "small molecules"
                    }
            pertCountArr.append(countType)
            pertMeta["count-type"] = pertCountArr

        if pertMeta:
            outD["perturbagens-meta"] = pertMeta

        if cLineArr:
            outD["cell-lines"] = cLineArr

        if not lineLength == 0:
            cLineMetaArr = []
            cLineMeta = {
                    "count": lineLength,
                    "type": "cell lines" # Is this right?
                    }                    # Are any iPSC differentiated?
            cLineMetaArr.append(cLineMeta)

        if cLineMetaArr:
            outD["cell-lines-meta"] = cLineMetaArr

        if not pLength == 0:
            pMetaArr = []
            pMeta = {
                    "count": pLength,
                    "type": "proteins" # Right? Are any iPSC differentiated?
                    }
            pMetaArr.append(pMeta)
            outD["proteins-meta"] = pMetaArr

        if pArr:
            outD["proteins"] = pArr

        sparkList = [ID]
        return sparkList

    with open('outputJSON.txt', 'w+') as outJson:

        # RUN IT IN SPARK!
        idTuple = tuple(phaseOneIds)
        distIds = sc.parallelize(phaseOneIds, 18)
        output = distIds.map(produceResults).reduce(lambda x,y : x+y)
        outJSon.write(output)
        print output

