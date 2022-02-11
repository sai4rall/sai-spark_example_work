931 -1251
eaiNodeBName = F.udf(genNodeBName, StringType())

def getBbhType(tech):
    if tech == None:
        return None

    if tech.find('LTE') >= 0:
        return 'ocw/eNodeB'

    if tech.find('NR') >= 0:
        #return 'ocw/gnbcuup'
        return 'ocw/gnbdu'

    return None

eaiBbhType = F.udf(getBbhType, StringType())





n = (t.where(t.network.isin(['4G (LRAN)']) & (t.rbs_id.isNotNull()))
 .withColumn('name', eaiEGNodeBName(F.col('du_number'), F.col('site_name'), F.col('rbs_id'), F.col('node_code')))
 .withColumn('type', F.lit('ocw/eNodeB'))
 .select('name', 'type', t.node_code,
         t.rbs_id.alias('id'), t.virtual_rnc,   # t.site_name,
         t.status
         #t.address_id,
         #t.nodeb_allocation_id.alias('tempestId')#    , t.du_number
        )
# .where("myDu is null")
# .where(t.node_code == 'AADP')
# .show(200, truncate = False)
# .printSchema()
)

n = n.union(t.where(t.network.isin(['5G (NGRAN)']) & (t.gnb_id.isNotNull()))
 .withColumn('name', eaiEGNodeBName(F.col('du_number'), F.col('site_name'), F.col('gnb_id'), F.col('node_code')))
 .withColumn('type', F.lit('ocw/gnbdu'))
 .select('name', 'type', t.node_code,
         t.gnb_id.alias('id'), t.virtual_rnc,   # t.site_name,
         t.status
         #t.address_id,
         #t.nodeb_allocation_id.alias('tempestId')#    , t.du_number
        )
 )


n = n.union(t.where(t.network.isin(['3G (WRAN)']) & (t.rbs_id.isNotNull()))
 .withColumn('name', eaiNodeBName(F.col('site_name'), F.col('node_code')))
 .withColumn('type', F.lit('ocw/nodeB'))
 .select('name', 'type', t.node_code,
         t.rbs_id.alias('id'), t.virtual_rnc, #  t.site_name,
         t.status
         #t.address_id,
         #t.nodeb_allocation_id.alias('tempestId')#    , t.du_number
        )
 )


# when joining in the BBH spreadhseets make sure that the BBH entry is used as it has the correct type (in that case)


b2 = (b
 .withColumn('type', eaiBbhType(F.col('technology')))
 .withColumn('status', F.lit('Unknown'))
 .select(b.name, 'type', b.node_code, b.id, b.virtual_rnc, 'status')
      )

bs = (b2
       .join(n, (b2.name == n.name), 'inner')
       .select(b2.name, b2.type, b2.node_code, b2.id, b2.virtual_rnc, b2.status)      # select the bbh side
       .union(b2.join(n, b2.name == n.name, 'left_anti'))
       .union(n.join(b2, n.name == b2.name, 'left_anti'))       # psudo right_anti
       .where('name is not null')
       .distinct()
)

# n.count = 42749
# b.count = 5698
#n.count()
#print(bs.count())       # 44425
#bs.select('type').distinct().show()
#bs.orderBy(bs.name).coalesce(1).write.csv(path='s3://emrdisco/eai_objects/baseStation/csv', mode='overwrite', header=True, quoteAll=True)
#bs.write.json(path='s3://emrdisco/eai_objects/baseStation', mode='overwrite')


VBox()
FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…
Import ENM Data
[15]
# read in the ENM data


# UDF's
def extractNameFromMecontext(qStr, paddOne=False):
    if qStr == None:
        return None
    else:
        try:
            if paddOne:             # for NodeB's we need to add the implied '1' to the name if it is just 4 chars long.
                tmp = qStr.split("_")[1]
                if len(tmp) == 4:
                    return tmp + '1'
                else:
                    return tmp
            else:                   # otherwise just extract the name
                return qStr.split("_")[1]
        except:
            print('error extracting name from mecontext for:', qStr)
            return None

eaiNameFromMecontext = F.udf(extractNameFromMecontext, StringType())


def extractIdFromMecontext(qStr):
    if qStr == None:
        return None
    else:
        try:
            return int(qStr.split("_")[-1])
        except:
            print('error extracting ID from mecontext for:', qStr)
            return None

eaiIdFromMecontext = F.udf(extractIdFromMecontext, IntegerType())



# enm_nodeB.csv
enmNodeBSchema = StructType([
    StructField('mecontext', StringType(), False)
])
nb_e = spark.read.csv('s3://emrdisco/enm/enm_nodeB.csv', header=True, schema=enmNodeBSchema)

nb_e = (nb_e
 .withColumn('name', eaiNameFromMecontext(F.col('mecontext'), F.lit(True)))
 .withColumn('id', eaiIdFromMecontext(F.col('mecontext')))
 .withColumn('type', F.lit('ocw/nodeB'))
 .withColumn('status', F.lit('In Service'))
 .select('name', 'id', 'type', 'status',
        F.substring('name', 1, 4).alias('nodeCode'))
 .where('id is not null')
)



# enm_eNodeB.csv
enmBaseStationSchema = StructType([
    StructField('mecontext', StringType(), False),
    StructField('id', IntegerType(), False)
])
enb_e = spark.read.csv('s3://emrdisco/enm/enm_eNodeB.csv', header=True, schema=enmBaseStationSchema)

enb_e = (enb_e
         .withColumn('name', eaiNameFromMecontext(F.col('mecontext'), F.lit(True)))
         .withColumn('type', F.lit('ocw/eNodeB'))
         .withColumn('status', F.lit('In Service'))
         .select('name', 'id', 'type', 'status',
                F.substring('name', 1, 4).alias('nodeCode'))
         .where('id is not null')
        )


# enm_gNodeB-DU.csv
gnbd_e = spark.read.csv('s3://emrdisco/enm/enm_gNodeB-DU.csv', header=True, schema=enmBaseStationSchema)

gnbd_e = (gnbd_e
         .withColumn('name', eaiNameFromMecontext(F.col('mecontext'), F.lit(True)))
         .select('name', 'id', gnbd_e.mecontext,)
         .where('id is not null')
        )


# enm_gNodeB-CU-UP.csv
"""
This is not required for Release 1.  After further understanding the model we have realised that the Basebands at a BBU are infact the DU units!
gnbcu_e = spark.read.csv('s3://emrdisco/enm/enm_gNodeB-CU-UP.csv', header=True, schema=enmBaseStationSchema)

gnbcu_e = (gnbcu_e
         .withColumn('name', eaiNameFromMecontext(F.col('mecontext'), F.lit(True)))
         .select('name', 'id', gnbcu_e.mecontext)
         .where('id is not null')
        )


# enm_gNodeB-CU-CP.csv
gnbcc_e = spark.read.csv('s3://emrdisco/enm/enm_gNodeB-CU-CP.csv', header=True, schema=enmBaseStationSchema)

gnbcc_e = (gnbcc_e
         .withColumn('name', eaiNameFromMecontext(F.col('mecontext'), F.lit(True)))
         .select('name', 'id', gnbcc_e.mecontext)
         .where('id is not null')
        )
"""


gnb_e = gnbd_e
#gnb_e = gnb_e.union(gnbcu_e)
#gnb_e = gnb_e.union(gnbcc_e)
gnb_e = gnb_e.distinct()

def enmGnbType(mecontext):
    if mecontext == None:
        return None

    if mecontext.find("_BBH_") >= 0:
        #return "ocw/gnbcuup"
        # ^ is not required as we now are not including the CU objects.
        return "ocw/gnbdu"
    else:
        return "ocw/gnbdu"

eaiEnmGnbType = F.udf(enmGnbType, StringType())


gnb_e = (gnb_e
       .withColumn('type', eaiEnmGnbType(F.col('mecontext')))
       .withColumn('status', F.lit('In Service'))
       .select('name', 'id', 'type', 'status',
              F.substring('name', 1, 4).alias('nodeCode'))
      )

#gnb_e.show(truncate = False)
VBox()
FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…
Create Master Base Station List (mbs)
This is a combination of the Tempest, BBH spreadsheets and ENM

[16]
# combine the ENM dataframes

enm = nb_e
enm = enm.union(enb_e)
enm = enm.union(gnb_e)
#enm = enm.union(bts.select(bts.name, bts.btsId.alias('id'), bts.type, bts.status, bts.name.alias('nodeCode')))

# rename the bs fields so we can filter them out of the join results.
b3 = bs.select(bs.name.alias('bsname'), bs.id, bs.type.alias('bstype'),
               bs.status.alias('bsstatus'), bs.node_code.alias('bsnodeCode'))
# For all of the id's that match ENM and SB, keep the ENM version
tmp1 = enm.join(b3, "id", 'left_outer').select('id', 'name', 'type', 'status', 'nodeCode')     #for this join get the ENM side

# get the remaining records that are in BS but not in ENM
tmp3 = bs.join(enm, "id", 'left_anti').select('id', 'name', 'type', 'status', bs.node_code.alias('nodeCode'))

mbs = tmp1.union(tmp3)

#mbs.orderBy(mbs.name).show()

# convert all the statuses from Tempest, BBH XLSX and ENM into valid ENM statuses values
# TPD-1275 and TPD-1328
mbs = (mbs
       .select(mbs.id, mbs.name, mbs.type, mbs.status.alias('tmp'), mbs.nodeCode)
       .withColumn('status', eaiStatus(F.col('tmp')))
       .withColumn('$refId', F.col('name'))
       .withColumn('$type', F.col('type'))
       .withColumn('$action', F.lit('createOrUpdate'))
       .select('$type', '$refId', '$action', mbs.id, mbs.name, mbs.type, 'status', mbs.nodeCode)
)


# bs gives a consolidated list of base stations from TEMPEST and BBH XLSX
#   we now need to split these up again so that we can add in type specific fields
#   and also tweak the fields we display

nodeB = mbs.where(mbs.type == 'ocw/nodeB').select('$type', '$refId', '$action', mbs.id.alias('nodeBId'), mbs.name, mbs.status)
eNodeB = mbs.where(mbs.type == 'ocw/eNodeB').select('$type', '$refId', '$action', mbs.id.alias('eNodeBId'), mbs.name, mbs.status)
gNBDU = mbs.where(mbs.type == 'ocw/gnbdu').select('$type', '$refId', '$action', mbs.id.alias('gnbduId'), mbs.name, mbs.status)
#gNBCUUP = mbs.where(mbs.type == 'ocw/gnbcuup').select('$type', '$refId', '$action', mbs.id.alias('gnbcuupId'), mbs.name, mbs.status)


#print(nodeB.count())       # 14706
#print(eNodeB.count())      # 25490
#print(gNBDU.count())       # 1063
#print(gNBCUUP.count())     # 3162


nodeB.write.json(path=bucketUrl + bucketOutputPath + 'nodeB', mode='overwrite')
eNodeB.write.json(path=bucketUrl + bucketOutputPath + 'eNodeB', mode='overwrite')
gNBDU.write.json(path=bucketUrl + bucketOutputPath + 'gNB-DU', mode='overwrite')
#gNBCUUP.write.json(path=bucketUrl + bucketOutputPath + 'gNB-CU-UP', mode='overwrite')


#gNBCUUP.show()

# ToDo:  for nodeB, change the id field to nodeBId
VBox()
FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…
Create Relationships Between Cells and Base Stations
NNI-1236

bts_to_gsmCell
[17]


bts_to_gsmCell_lookup = (bts
                         .select('$type', '$refId', 'name')
                         .withColumn('$action', F.lit('lookup'))
                         .select('$type', '$refId', '$action', 'name')
                         .distinct()
                        )

#bts_to_gsmCell_lookup.show()
bts_to_gsmCell_lookup.write.json(path=bucketUrl + bucketOutputPath + 'bts_to_gsmCell_lookup', mode='overwrite')


# backup :   (q.iub_rbsid.isNotNull()) & (q.technology.like('GSM%')) & (q.cell_status != 'Erroneous entry')
bts_to_gsmCell = (q
    .where((q.technology.like('GSM%')) & (q.rru_donor_node.isin(['remote', 'neither'])))
    .select(q.cell_name, q.cell_name.substr(1, 4).alias('btsName'))
    #.withColumn('btsId', eaiInt(F.col('iub_rbsid')))
    .withColumn('$action', F.lit('createOrUpdate'))
    .withColumn('$type', F.lit('ocw/gsmCell'))
    .withColumn('$bts', F.array(F.col('btsName')))
    .select('$type',  q.cell_name.alias('$refId'), '$action', q.cell_name.alias('name'), '$bts')
      )

#bts_to_gsmCell.show(50)
bts_to_gsmCell.write.json(path=bucketUrl + bucketOutputPath + 'bts_to_gsmCell', mode='overwrite')

