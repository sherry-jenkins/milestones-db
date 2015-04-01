# coding=utf-8

import csv
from pymongo import MongoClient
from datetime import datetime
from dateutil.parser import parse

client = MongoClient('mongodb://mmcdermott:kroyweN@127.0.0.1/Milestones')
db = client['Milestones']
md = db['data']
md.drop()

dInit = {}
mongoArr = []

with open('milestones-consolidated.txt', 'rU') as data:
    reader = csv.reader(data, skipinitialspace=False, delimiter="\t")
    mcArr = []
    for row in reader:
        if any(row):
            mcArr.append(row)
for inp in mcArr:
    if inp[0] and not (inp[0] == 'center'):
        # print(inp)
        dictTot = dInit.copy()
        dictTot['center'] = inp[0]
        assayDict = {}
        assayDict['name'] = inp[1]
        assayDict['info'] = inp[2]

        dictTot['assay'] = assayDict

        # cell-lines
        allCellLines = []
        clDictInit = {}

        # Format of cell line cell (from Excel):
        # name1,type1(cell line or iPSC differentiated),
        # class1(normal,cancer line,…)\control-or-disease,
        # tissue1;...
        # Split by ; then , to build arr of objs

        cellLineArr = inp[3].split(";")
        for cLine in cellLineArr:
            clDict = clDictInit.copy()
            cLineData = cLine.split(",")
            if cLineData[0]:
                clDict['name'] = cLineData[0]
            if len(cLineData) > 1:
                if cLineData[1]:
                    clDict['type'] = cLineData[1]
                if cLineData[2]:
                    # Check if \ in class
                    #            --->   For diseased (not cancer) cell lines
                    if "\\" in cLineData[2]:
                        classArr = cLineData[2].split("\\")
                        clDict['class'] = classArr[0]
                        clDict['controlOrDisease'] = classArr[1]
                    else:
                        clDict['class'] = cLineData[2]
                if cLineData[3]:
                    clDict['tissue'] = cLineData[3]
            if clDict:
                allCellLines.append(clDict)

        if allCellLines:
            dictTot['cellLines'] = allCellLines

        # cell-lines-meta: count1,type1;count2,type2
        # Split by ; then \ to build arr of objs
        cLineMetaArr = []
        cLineMetaDictInit = {}

        cLineMetaData = inp[4].split(";")
        for meta in cLineMetaData:
            # print(meta)
            cLineMetaDict = cLineMetaDictInit.copy()
            cLineMeta = meta.split("\\")
            if cLineMeta[0]:
                cnt = cLineMeta[0]
                cLineMetaDict['count'] = float(cnt) if '.' in cnt else int(cnt)
                if cLineMeta[1]:
                    cLineMetaDict['type'] = cLineMeta[1]
                if cLineMetaDict:
                    cLineMetaArr.append(cLineMetaDict)

        if cLineMetaArr:
            dictTot['cellLinesMeta'] = cLineMetaArr

        # perturbagens: name1,type1(,perturbagens1);name2,type2(,purturbagens2)
        # Split by ; then , to build arr of objs
        pertArr = []
        pertDictInit = {}

        pertsAll = inp[5].split(";")
        for perts in pertsAll:
            pertDict = pertDictInit.copy()
            pertData = perts.split(",")
            if pertData[0]:
                pertDict['name'] = pertData[0]
            if len(pertData) > 1:
                pertDict['type'] = pertData[1]
            # Check if has perturbagens value
            if len(pertData) == 3:
                pertDict['perturbagens'] = pertData[2]
            if pertDict:
                pertArr.append(pertDict)

        if pertArr:
            dictTot['perturbagens'] = pertArr

        # perturbagens-meta

        pertMetaArr = []
        pertMetaDict = {}

        # pair:type1,count1;type2,count2
        # Split by \ and get pair
        # Then split [1] by ; and then , to build arr of objs

        getPair = inp[6].split("\\")
        if getPair[0] == "true":
            pertMetaDict['pair'] = True
        elif getPair[0] == "false":
            pertMetaDict['pair'] = False

        pertCountArr = []
        pertCountDictInit = {}
        if getPair[1]:
            pertCountMeta = getPair[1].split(";")
            print(pertCountMeta)
            for countMeta in pertCountMeta:
                print(countMeta)
                countData = countMeta.split(",")
                pertCountDict = pertCountDictInit.copy()
                if countData[0]:
                    pertCountDict['type'] = countData[0]
                if len(countData) > 1:
                    if countData[1]:
                        pertCountDict['count'] = int(countData[1])
                    if pertCountDict:
                        pertCountArr.append(pertCountDict)

        if pertCountArr:
            pertMetaDict['countType'] = pertCountArr

        # dose1(,dose2,...);dose-count
        doseAll = inp[7].split(";")
        pertMetaDict['dose'] = doseAll[0].split(",")
        if len(doseAll) > 1:
            if doseAll[1]:
                pertMetaDict['doseCount'] = int(doseAll[1])

        # time1(,time2,...);time-unit;time-points
        pertMetaTime = inp[8].split(";")
        timeArr = pertMetaTime[0].split(",")
        if timeArr:
            for time in timeArr:
                if time:
                    time = int(time)
            pertMetaDict['time'] = timeArr
        if len(pertMetaTime) > 1:
            pertMetaDict['timeUnit'] = pertMetaTime[1]
            pertMetaDict['timePoints'] = int(pertMetaTime[2])

        if pertMetaDict:
            dictTot['perturbagensMeta'] = pertMetaDict

        # instance-meta: reps,tech-reps
        instanceMetaDict = {}

        reps = inp[9].split(",")
        if reps[0]:
            instanceMetaDict['reps'] = int(reps[0])
        if len(reps) == 2:
            instanceMetaDict['techReps'] = int(reps[1])

        # instance-meta map: pert1,cell-line1;pert2,cell-line2
        mapArr = []
        mapDictInit = {}

        # pprint(inp[10])

        if inp[10] == 'one-all':
            # print("TRUE")
            # print(pertArr)
            for pertObj in pertArr:
                # pprint(pertObj)
                if 'name' in pertObj:
                    for cLineObj in allCellLines:
                        mapDict = mapDictInit.copy()
                        mapDict['perturbagen'] = pertObj['name']
                        if 'name' in cLineObj:
                            mapDict['cellLine'] = cLineObj['name']
                        mapArr.append(mapDict)
        else:
            allMaps = inp[10].split(";")
            for mapData in allMaps:
                mapDict = mapDictInit.copy()
                maps = mapData.split(",")
                if len(maps) == 2:
                    mapDict['perturbagen'] = maps[0]
                    mapDict['cellLine'] = maps[1]
                    if mapDict:
                        mapArr.append(mapDict)

        if mapArr:
            instanceMetaDict['map'] = mapArr

        if instanceMetaDict:
            dictTot['instanceMeta'] = instanceMetaDict

        # readouts -> name1\datatype1;name2\datatype2
        readoutsArr = []
        readoutsDictInit = {}

        allReadouts = inp[11].split(";")
        for readout in allReadouts:
            readoutDict = readoutsDictInit.copy()
            readoutData = readout.split("\\")
            if readoutData[0]:
                readoutDict['name'] = readoutData[0]
            if len(readoutData) == 2:
                readoutDict['datatype'] = readoutData[1]
            if readoutDict:
                readoutsArr.append(readoutDict)

        if readoutsArr:
            dictTot['readouts'] = readoutsArr

        # readout-count
        if inp[12]:
            dictTot['readoutCount'] = inp[12]

        # Data release dates
        dateDict = {}
        if inp[13]:
            lvlOneDate = parse(inp[13] + ' -0400')
            dateDict['levelOne'] = lvlOneDate

        if inp[14]:
            lvlTwoDate = parse(inp[14] + ' -0400')
            dateDict['levelTwo'] = lvlTwoDate

        if inp[15]:
            lvlThreeDate = parse(inp[15] + ' -0400')
            dateDict['levelThree'] = lvlThreeDate

        if inp[16]:
            lvlFourDate = parse(inp[16] + ' -0400')
            dateDict['levelFour'] = lvlFourDate

        if dateDict:
            dictTot['releaseDates'] = dateDict

        if inp[17]:
            dictTot['releaseLink'] = inp[17]
            
        md.insert(dictTot)
        print(dictTot)
# print(dictTot)
