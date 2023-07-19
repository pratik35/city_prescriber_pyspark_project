import logging.config

logging.config.fileConfig('Properties\configuration\logging.config')
logger = logging.getLogger('Ingest')

def load_files(spark,file_dir,file_format,Header,InferSchema):
    try:
        logger.warning('load_files method started....')
        if file_format == 'parquet':
            df = spark.read.format(file_format).load(file_dir)
        elif file_format == 'csv':
            df = spark.read.csv(file_dir,header=Header,inferSchema=InferSchema)
    except Exception as exp:
        logger.error("An error occured at load_files====",str(exp))
        raise
    else:
        logger.warning('dataframe created successfully which is of {}'.format(file_format))
    return df

def display_df(df,dfName):
    df_show = df.show()
    logger.warning('displaying the dataframe {}'.format(dfName))
    return df_show 

def df_count(df,dfName):
    try:
        logger.warning('here to count the records in the {}'.format(dfName))
        df_c=df.count()
    except Exception as exp:
        raise
    else:
        logger.warning("Number of records present in {} are::{}".format(df,df_c)) 
    return df_c
