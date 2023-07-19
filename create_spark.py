from pyspark.sql import SparkSession
import logging.config

logging.config.fileConfig('Properties\configuration\logging.config')
loggers = logging.getLogger('Create_spark')

def get_spark_object(envn,appName):
    try:
        loggers.info('get_spark_object method started')
        if envn == 'DEV':
            master = 'local'
        else:
            master = 'Yarn'
        loggers.info('master is {}'.format(master))
        spark = SparkSession.builder.master(master).appName(appName).enableHiveSupport().config('spark.driver.extraclasspath','mysql-connector-j-8.0.32.jar').getOrCreate()
        spark.sparkContext.setLogLevel('WARN')
        
    except Exception as exp:
        loggers.error('An error occured in the get_spark_object====',str(exp))
        raise
    else:
        loggers.info('Spark object created.....')
    return spark