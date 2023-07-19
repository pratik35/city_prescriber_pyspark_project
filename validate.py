import logging.config

logging.config.fileConfig('Properties\configuration\logging.config')
loggers = logging.getLogger('Validate')

def get_current_date(spark):
    try:
        loggers.warning('started the get_current_date method...')
        output = spark.sql("select current_date")
        #output.show()
        #print(output.collect()[0])
        #print("Date & Time::::::",output.collect()[0].__getitem__('current_date()'))
        #print(type(output.collect()[0].__getitem__('current_date()')))
        print("Validating spark object with current date--->",output.collect()[0].__getitem__('current_date()'))
    except Exception as exp:
        logging.error("An error occured when calling get_current_date() please check with the trace===", str(exp))
        raise
    else:
       loggers.warning('Validations done, going forward....') 