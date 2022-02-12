VBox()
FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…
nodeB_to_umtsCell
[18]


nodeB_to_umtsCell_lookup = (
    nodeB.select('$type', nodeB.nodeBId.alias('$refId'), nodeB.name)
    .withColumn('$action', F.lit('lookup'))
    .distinct()
)


#nodeB_to_umtsCell_lookup.show()
nodeB_to_umtsCell_lookup.write.json(path=bucketUrl + bucketOutputPath + 'nodeB_to_umtsCell_lookup', mode='overwrite')


nodeB_to_umtsCell = (q.where((q.iub_rbsid.isNotNull()) & (q.technology.like('WCDMA%')) & (q.rru_donor_node.isin(['remote', 'neither'])))
    .withColumn('nodeBId', eaiInt(F.col('iub_rbsid'))).where('nodeBId is not null')                            # filter out any integer conversion errors
    .withColumn('$nodeB', F.array(eaiInt(F.col('iub_rbsid'))))
    .withColumn('$action', F.lit('createOrUpdate'))
    .withColumn('$type', F.lit('ocw/umtsCell'))
    .select('$type', '$action', q.cell_name.alias('$refId'), q.cell_name.alias('name'), '$nodeB', )
)


#nodeB_to_umtsCell.show()
nodeB_to_umtsCell.write.json(path=bucketUrl + bucketOutputPath + 'nodeB_to_umtsCell', mode='overwrite')


VBox()
FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…
eNodeB_to_lteCell
[19]
# eNodeB_to_lteCell

eNodeB_to_lteCell_lookup = (
    eNodeB
    .select('$type', eNodeB.eNodeBId.alias('$refId').cast(StringType()), eNodeB.name)
    .withColumn('$action', F.lit('lookup'))
    .distinct()
)

#eNodeB_to_lteCell_lookup.show()
eNodeB_to_lteCell_lookup.write.json(path=bucketUrl + bucketOutputPath + 'eNodeB_to_lteCell_lookup', mode='overwrite')

eNodeB_to_lteCell = (
   q.where((q.enbid_dec.isNotNull()) & (q.technology.like('LTE%')) & (q.rru_donor_node.isin(['remote', 'neither', 'rruDonor'])))
    .withColumn('eNodeBId', eaiInt(F.col('enbid_dec'))).where('eNodeBId is not null')             # filter out any integer conversion errors
    .withColumn('$eNodeB', F.array(eaiInt(F.col('enbid_dec')).cast(StringType())))                # Converting this BACK to string as I think the refid (above) really likes a string...FFS
    .withColumn('$action', F.lit('createOrUpdate'))
    .withColumn('$type', F.lit('ocw/lteCell'))
    .select('$type', '$action', q.cell_name.alias('$refId'), q.cell_name.alias('name'), '$eNodeB', )
)

#eNodeB_to_lteCell.show()
eNodeB_to_lteCell.write.json(path=bucketUrl + bucketOutputPath + 'eNodeB_to_lteCell', mode='overwrite')



VBox()
FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…
nrCells_to_gnbdu
Note: there is no direct relationship to between nrCells and gnbcuup

[20]
# this is a bit trickier as I need to link to just the distributed gNodeB's, but you can't tell if it is distributed from the quoll data alone.

# create a list of all of the nrCell to node relationships
c2n = (q.where((q.enbid_dec.isNotNull()) & (q.technology.like('NR%')) & (q.rru_donor_node.isin(['remote', 'neither'])))
    .withColumn('gNodeBId', eaiInt(F.col('enbid_dec')))
    .select('gNodeBId', q.cell_name, q.technology, q.cell_status)
    .where('gNodeBId is not null')
)

#tmp.count()     # 22,502


""""""
This is now not needed as there is not relationship between the nrCell and the gNB-CU

# now join these with the list of base staions so that we can determine if they are du or cu

#nrCells_to_gnbdu
nrCells_to_gnbdu = (c2n
 .join(mbs, c2n.gNodeBId == mbs.id, 'leftouter')
 .where(""type = 'ocw/gNB-DU'"")
 .select(c2n.gNodeBId.alias('gnbduId'),
        'cell_name')
# .show(100)
# .coalesce(1).write.csv(path='s3://emrdisco/eai_objects/nrCells_to_gnbdu/csv', mode='overwrite', header=True)
# .write.json(path='s3://emrdisco/eai_objects/nrCells_to_gnbdu', mode='overwrite')
)


#nrCells_to_gnbcuup
nrCells_to_gnbcuup = (c2n
 .join(mbs, c2n.gNodeBId == mbs.id, 'leftouter')
 .where(""type = 'ocw/gNB-CU-UP'"")
 .select(c2n.gNodeBId.alias('gnbcuupId'),
        'cell_name')
# .show(100)
# .coalesce(1).write.csv(path='s3://emrdisco/eai_objects/nrCells_to_gnbcuup/csv', mode='overwrite', header=True)
# .write.json(path='s3://emrdisco/eai_objects/nrCells_to_gnbcuup', mode='overwrite')
)

#print(nrCells_to_gnbdu.count())
#print(nrCells_to_gnbcuup.count())
""""""

# Lookup the gNodB-DU objects
nrCells_to_gnbdu_lookup = (
    c2n
    .join(gNBDU, (c2n.gNodeBId == gNBDU.gnbduId), 'inner')                    # just get the gNodeB's that we need, alos to get the gNodeB's name
    .withColumn('$type', F.lit('ocw/gnbdu'))
    .withColumn('$action', F.lit('lookup'))
    .select(
        '$type', '$action',
        c2n.gNodeBId.alias('$refId'), gNBDU.name
    )
    .distinct()
)

#nrCells_to_gnbdu_lookup.show()
nrCells_to_gnbdu_lookup.write.json(path=bucketUrl + bucketOutputPath + 'nrCells_to_gnbdu_lookup', mode='overwrite')



# update the nrCells
nrCells_to_gnbdu = (
    c2n
    .withColumn('$type', F.lit('ocw/nrCell'))
    .withColumn('$action', F.lit('createOrUpdate'))
    .withColumn('$gnbdu', F.array(F.col('gNodeBId')))
    .select(
        '$type', '$action',
        c2n.cell_name.alias('$refId'), c2n.cell_name.alias('name'),
        '$gnbdu'
    )
    .distinct()
)

#nrCells_to_gnbdu.show()
nrCells_to_gnbdu.write.json(path=bucketUrl + bucketOutputPath + 'nrCells_to_gnbdu', mode='overwrite')



VBox()
FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…
Create Repeaters
[46]

def cleanRepeaterTechnology(qStr):
    if qStr == None:
        return None
    else:
        try:
            # 'NR26G' == 'NR26000'
            if qStr[-1:] == 'G':
                qStr = qStr[:2] + qStr.replace('G', '000')[2:]

            # replace WCDMA with UMTS
            qStr = qStr.replace('WCDMA', 'UMTS')
            return qStr
        except:
            print('Error fixing the System Type field:', qStr)
            return None

eaiSystemType = F.udf(cleanRepeaterTechnology, StringType())



repeater = (
    q.where((q.rru_donor_node == 'repeater'))
     .withColumn('$type', F.lit('ocw/repeater'))
     .withColumn('$action', F.lit('createOrUpdate'))
     .withColumn('status', eaiCellStatus(F.col('cell_status')))
     .withColumn('|telstraRepeaterAttributes|cellType', eaiCellType(F.col('base_station_type')))                     # ocw:telstraCellTypePicklist
     .withColumn('|telstraRepeaterAttributes|hasPriorityAssistCustomers', eaiBool(F.col('priority_assist')))
     .withColumn('|telstraRepeaterAttributes|hasWirelessLocalLoopCustomers', eaiBool(F.col('wll')))
     .withColumn('|telstraRepeaterAttributes|isSubjecttoEmbargo', eaiBool(F.col('embargo_flag')))
     .withColumn('|telstraRepeaterAttributes|hasSpecialEvent', eaiBool(F.col('special_event_cell')))
     .withColumn('|telstraRepeaterAttributes|hasSignificantSpecialEvent', eaiBool(F.col('special_event')))
     .withColumn('|telstraRepeaterAttributes|hasHighSeasonality', eaiBool(F.col('high_seasonality')))
     .withColumn('|telstraRepeaterAttributes|mobileServiceArea', F.col('msa'))
     .withColumn('|telstraRepeaterAttributes|quollIndex', eaiInt(F.col('cell_index')))
     .withColumn('systemType', eaiSystemType(F.col('technology')))
     .select(
         '$type', '$action', q.cell_name.alias('$refId'),
         q.cell_name.alias('name'), 'status',
         'systemType',
         q.note.alias('comments'),


         # Dynamic Attributes
         '|telstraRepeaterAttributes|cellType',
         q.cell_inservice_date.alias('|telstraRepeaterAttributes|originalOnAirDate'),
         q.coverage_classification.alias('|telstraRepeaterAttributes|coverageClassification'),
         q.coverage_statement.alias('|telstraRepeaterAttributes|coverageStatement'),
         '|telstraRepeaterAttributes|hasPriorityAssistCustomers',
         '|telstraRepeaterAttributes|hasWirelessLocalLoopCustomers',
         q.optimisation_cluster.alias('|telstraRepeaterAttributes|optimisationCluster'),
         q.owner.alias('|telstraRepeaterAttributes|wirelessServiceOwner'),
         '|telstraRepeaterAttributes|hasSpecialEvent',
         '|telstraRepeaterAttributes|hasSignificantSpecialEvent',
         '|telstraRepeaterAttributes|isSubjecttoEmbargo',
         '|telstraRepeaterAttributes|hasHighSeasonality',
         '|telstraRepeaterAttributes|mobileServiceArea',
         '|telstraRepeaterAttributes|quollIndex'
        )
)

#repeater.show()
repeater.write.json(path=bucketUrl + bucketOutputPath + 'repeater', mode='overwrite')

#col = 'systemType'
#repeater.select(col).groupBy(col).agg({col: 'count'}).orderBy(col).show(100, truncate = False)



VBox()
FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…
Create Relationship Between Repeater and it's Donor
[47]
# rfCell_to_repeater

# Note:  There are a lot of active_repeater_donor_node values that can't be matched with a cell.These will FAIL.
#        - Can we do any DQ work on this?

# lookup the cells
rfCell_to_repeater_lookup = (
    q
    .where((q.rru_donor_node == 'repeater') & q.active_repeater_donor_node.isNotNull())
    .withColumn('$type', eaiTechnologyToType(F.col('technology')))
    .withColumn('$action', F.lit('lookup'))
    .select('$type', '$action',
        q.active_repeater_donor_node.alias('$refId'), q.active_repeater_donor_node.alias('name')
    )
    .distinct()
)

#rfCell_to_repeater_lookup.show()
rfCell_to_repeater_lookup.write.json(path=bucketUrl + bucketOutputPath + 'rfCell_to_repeater_lookup', mode='overwrite')

# update the repeaters
rfCell_to_repeater = (
    q
    .where((q.rru_donor_node == 'repeater') & q.active_repeater_donor_node.isNotNull())
    .withColumn('$type', F.lit('ocw/repeater'))
    .withColumn('$action', F.lit('createOrUpdate'))
    .withColumn('$rfCell', F.array(F.col('active_repeater_donor_node')))
    .select('$type', '$action',
        q.cell_name.alias('$refId'), q.cell_name.alias('name'),
            '$rfCell'
    )
)

#rfCell_to_repeater.show()
rfCell_to_repeater.write.json(path=bucketUrl + bucketOutputPath + 'rfCell_to_repeater', mode='overwrite')



VBox()
FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…
Wireless Network
[23]

def getMcc(plmn):
    if plmn == None:
        return None
    else:
        try:
            return int(str(plmn)[0:3])
        except:
            print('Error extracting MCC from PLMN:', plmn)
            return None

eaiMcc = F.udf(getMcc, IntegerType())

def getMnc(plmn):
    if plmn == None:
        return None
    else:
        try:
            return str(plmn)[3:5]
        except:
            print('Error extracting MNC from PLMN:', plmn)
            return None

eaiMnc = F.udf(getMnc, StringType())


wirelessNetwork = (q
 .where(q.plmn.isNotNull())
 .where(q.plmn != 99999)
 .withColumn('$type', F.lit('ocw/wirelessNetwork'))
 .withColumn('$action', F.lit('createOrUpdate'))
 .withColumn('country', F.lit('Australia'))
 .withColumn('mcc', eaiMcc(F.col('plmn')))
 .withColumn('mnc', eaiMnc(F.col('plmn')))
 .withColumn('operatorName', F.lit('Telstra'))
 .withColumn('status', F.lit('ACTIVE'))
 .select('$type', '$action', q.plmn.alias('$refId'), q.plmn.alias('name'), 'status',
         'country', 'mcc', 'mnc', 'operatorName')
 .distinct()
 .orderBy(q.plmn)
)


#wirelessNetwork.show()
wirelessNetwork.write.json(path=bucketUrl + bucketOutputPath + 'wirelessNetwork', mode='overwrite')





VBox()
FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…
nrCells_to_wirelessNetwork
[24]
#Reversing the direction.  This makes the lookups much smaller and hopefully more effecient.


nrCells_to_wirelessNetwork = (
    q
    .where(q.technology.like('NR%') & (q.rru_donor_node.isin(['remote', 'neither'])))
    .where(q.plmn.isNotNull())
    .withColumn('$type', F.lit('ocw/nrCell'))
    .withColumn('$action', F.lit('createOrUpdate'))
    .withColumn('$wirelessNetworks', F.array(F.col('plmn')))
    .select('$type', '$action', q.cell_name.alias('$refId'), q.cell_name.alias('name'), '$wirelessNetworks')
)

#nrCells_to_wirelessNetwork.show()
nrCells_to_wirelessNetwork.write.json(path=bucketUrl + bucketOutputPath + 'nrCells_to_wirelessNetwork', mode='overwrite')



nrCells_to_wirelessNetwork_lookup = (
    q
    .where(q.technology.like('NR%') & (q.rru_donor_node.isin(['remote', 'neither'])))
    .where(q.plmn.isNotNull())
    .withColumn('$type', F.lit('ocw/wirelessNetwork'))
    .withColumn('$action', F.lit('lookup'))
    #.withColumn('$nrCells', F.array(F.col('cell_name')))
    .select('$type', '$action',
            q.plmn.alias('$refId'),
            q.plmn.alias('name')#,'$nrCells'
           )
    .distinct()
)

#nrCells_to_wirelessNetwork_lookup.show()
nrCells_to_wirelessNetwork_lookup.write.json(path=bucketUrl + bucketOutputPath + 'nrCells_to_wirelessNetwork_lookup', mode='overwrite')

VBox()
FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…
wirelessNetwork_to_eNodeB
[25]
# lookup wirelessNetrowk

wirelessNetwork_to_eNodeB_lookup = (
    q
    .where(q.technology.like('LTE%') & (q.rru_donor_node.isin(['remote', 'neither'])))
    .where(q.plmn.isNotNull())
    .withColumn('$type', F.lit('ocw/wirelessNetwork'))
    .withColumn('$action', F.lit('lookup'))
    #.withColumn('$nrCells', F.array(F.col('cell_name')))
    .select('$type', '$action',
            q.plmn.alias('$refId'),
            q.plmn.alias('name')#,'$nrCells'
           )
    .distinct()
)

#wirelessNetwork_to_eNodeB_lookup.show()
wirelessNetwork_to_eNodeB_lookup.write.json(path=bucketUrl + bucketOutputPath + 'wirelessNetwork_to_eNodeB_lookup', mode='overwrite')


# update eNodeB  ($wirelessNetwork)

wirelessNetwork_to_eNodeB = (
    q
    .where(q.technology.like('LTE%') & (q.rru_donor_node.isin(['remote', 'neither'])))
    .where(q.plmn.isNotNull())
    .withColumn('eNBId', eaiInt(F.col('enbid_dec')))
    .join(eNodeB, (F.col('eNBId') == eNodeB.eNodeBId), 'inner')
    .withColumn('$type', F.lit('ocw/eNodeB'))
    .withColumn('$action', F.lit('createOrUpdate'))
    .withColumn('$wirelessNetwork', F.array(F.col('plmn')))
    .select('$type', '$action',
            #q.enbid_dec.alias('$refId'), q.enbid_dec.alias('name'),
            eNodeB.name.alias('$refId'),  eNodeB.name,
            '$wirelessNetwork')
    .distinct()
)

#wirelessNetwork_to_eNodeB.show()
wirelessNetwork_to_eNodeB.write.json(path=bucketUrl + bucketOutputPath + 'wirelessNetwork_to_eNodeB', mode='overwrite')

#eNodeB.show()


VBox()
FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…
rnc_to_nodeB relationships
[26]


# lookup RNC

rnc_to_nodeB_lookup = (
    q.where(q.technology.like('WCDMA%') & (q.rru_donor_node.isin(['remote', 'neither'])))
    .withColumn('qNBId', eaiInt(F.col('iub_rbsid')))
    .join(nodeB, (F.col('qNBId') == nodeB.nodeBId), 'inner')
    .withColumn('$type', F.lit('ocw/rnc'))
    .withColumn('$action', F.lit('lookup'))
    .where(q.bsc_rnc_node.isNotNull())
    .select('$type', '$action',
        q.bsc_rnc_node.alias('$refId'), q.bsc_rnc_node.alias('name')
           )
    .distinct()
)

#rnc_to_nodeB_lookup.show()
rnc_to_nodeB_lookup.write.json(path=bucketUrl + bucketOutputPath + 'rnc_to_nodeB_lookup', mode='overwrite')


# update nodeB  ($rnc)

rnc_to_nodeB = (
    q.where(q.technology.like('WCDMA%') & (q.rru_donor_node.isin(['remote', 'neither'])))
    .withColumn('qNBId', eaiInt(F.col('iub_rbsid')))
    .join(nodeB, (F.col('qNBId') == nodeB.nodeBId), 'inner')
    #.where('qNBId = 320000304')
    .where(q.bsc_rnc_node.isNotNull())
    .withColumn('$type', F.lit('ocw/nodeB'))
    .withColumn('$action', F.lit('createOrUpdate'))
    .withColumn('$rnc', F.array(q.bsc_rnc_node))
    .select('$type', '$action',
        nodeB.name.alias('$refId'), nodeB.name, '$rnc'
           )
    .distinct()
)

#rnc_to_nodeB.show()
rnc_to_nodeB.write.json(path=bucketUrl + bucketOutputPath + 'rnc_to_nodeB', mode='overwrite')",""