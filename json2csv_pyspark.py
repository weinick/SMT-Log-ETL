from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType
from pyspark.sql import Row
def template():
    dic = {'HeadSegmentId': None,
           'Pick.ActualPickPosition.Angle': None,
           'Pick.ActualPickPosition.X': None,
           'Pick.ActualPickPosition.Y': None,
           'Pick.MeasuredComponentHeight': None,
           'Pick.NominalPickPosition.Angle': None,
           'Pick.NominalPickPosition.X': None,
           'Pick.NominalPickPosition.Y': None,
           'Pick.PickupLocationId': None,
           'Pick.Result': None,
           'Pick.TimeStampSinceStationStarted': None,
           'Pick.VacuumAbsolute': None,
           'Pick.VacuumRelative': None,
           'Pick.ZEndPosition': None,
           'Pick.ZTargetPosition': None,
           'Place.ActualPlacePosition.Angle': None,
           'Place.ActualPlacePosition.X': None,
           'Place.ActualPlacePosition.Y': None,
           'Place.AirkissPressure': None,
           'Place.BoardPanelId': None,
           'Place.HoldingCircuitPressure': None,
           'Place.NominalPlacePosition.Angle': None,
           'Place.NominalPlacePosition.X': None,
           'Place.NominalPlacePosition.Y': None,
           'Place.ReferenceDesignator': None,
           'Place.Result': None,
           'Place.TimeStampSinceStationStarted': None,
           'Place.VacuumAbsolute': None,
           'Place.VacuumRelative': None,
           'Place.ZEndPosition': None,
           'Place.ZTargetPosition': None,
           'CameraIndividualId': None,
           'MeasurementResult': None,
           'PhiDeviation': None,
           'TimeStampSinceStationStarted': None,
           'VisionDumpPath': None,
           'XDeviation': None,
           'YDeviation': None,
           'GantryId': None,
           'Head': None,
           'HeadName': None,
           'Id': None,
           'NozzleTypeId': None,
           'SegmentNumber': None,
           'name': None,
           'dt': None,
           'product_type': None
           }
    return dic


schema = StructType()
for key in template().keys():
    schema = schema.add(key, StringType())


def parser(col1, col2, name):
    global lst_individual
    try:
        citem = col1[0]
        path = citem['Pcb']['Path']
        dt = citem['StationTime']
        lst_headers = []
        for item in col2["ProductionProgressData"]["HeadContext"]["HeadSegments"]["HeadSegment"]:
            tmp_res = {}
            item = item.asDict()
            for key in item.keys():
                tmp_res[key] = item[key]
            lst_headers.append(tmp_res)

        lst_individual = []
        for item in col2["ProductionProgressData"]["ComponentIndividuals"]['ComponentIndividual']:
            tmp_res = template()
            item = item.asDict()
            for key in item.keys():
                tmp = item[key]

                if key == 'Measures':
                    tmp = tmp['MeasureList'][0]
                    tmp = tmp.asDict()
                    for key in tmp.keys():
                        tmp_res[key] = str(tmp[key])
                elif isinstance(tmp, Row):
                    tmp = tmp.asDict()
                    for key1 in tmp.keys():
                        if isinstance(tmp[key1], (list, Row)):
                            tmp2 = tmp[key1]
                            if isinstance(tmp2, list):
                                tmp2 = tmp2[0]
                            tmp2 = tmp2.asDict()
                            for key2 in tmp2.keys():
                                if isinstance(tmp2[key2], Row):
                                    tmp3 = tmp2[key2].asDict()
                                    for key3 in tmp3.keys():
                                        tmp_res[key + "." + key1 + "." + key2 + "." + key3] = str(tmp3[key3])
                                else:
                                    tmp_res[key + "." + key1 + "." + key2] = str(tmp2[key2])
                        else:
                            tmp_res[key + "." + key1] = str(tmp[key1])
                else:
                    tmp_res[key] = str(tmp)
            lst_individual.append(tmp_res)

        remove_lst = ['Dip', 'ElectrialMeasurement', 'Place']
        for item1 in lst_individual:
            item1['product_type'] = path
            item1['dt'] = dt
            item1['name'] = name
            for item2 in lst_headers:
                if item2['Id'] == item1['HeadSegmentId']:
                    item1.update(item2)
                    break
            for remove_key in remove_lst:
                if remove_key in item1.keys():
                    item1.pop(remove_key)
    except:
        pass
    return lst_individual


input_path = "s3://siplacedemo/datasource/Raw_Data_Prod/*.json"  
output_path = "s3://siplacedemo/result/csv/Year=2021/Month=12/Day=12/"  
partition_nums = 1
spark = SparkSession.builder.getOrCreate()
data = spark.read.option("multiLine", "true").json(input_path)
data.select(['ProductionStartedData', 'ProgressDataList', 'name']).rdd.flatMap(
    lambda x: parser(x[0], x[1], x[2])).toDF(schema=schema).repartition(partition_nums).write.mode('append').csv(
    output_path, header=True)