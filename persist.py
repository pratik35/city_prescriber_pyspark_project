import logging.config
from pyspark.sql import *

logging.config.fileConfig('Properties\configuration\logging.config')
logger = logging.getLogger('Persist')

def persist_file(df,format,filepath,split_no,headerReq,compressionType):
    try:
        logger.warning('Persist file method started......')
        print("Number of partitions:"+str(df.rdd.getNumPartitions()))
        df.coalesce(split_no).write.mode('overwrite').format(format).save(filepath,header=headerReq,compression=compressionType)
    except Exception as exp:
        logger.error(str(exp))
    else:
        logger.warning('persist file execution completed successfully')


def data_hive_persist(spark,df,dfName,PartitionBy,Mode):
    try:
        logger.warning("persisting data in hive table for {}".format(dfName))
        logger.warning("creating a database")
        spark.sql("create database if not exists Hive_Database")
        #spark.sql("use database DB")
        logger.warning("writing {} into hive table by {} ".format(df,PartitionBy))
        df.write.saveAsTable("Hive_Database."+dfName,partitionBy=PartitionBy,mode=Mode)

    except Exception as exp:
        logger.error("an error occured when creating hive table ",str(exp))
        raise
    else:
        logger.warning("Data successfully persited into hive table")