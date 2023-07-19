import get_env_variables as gav
from create_spark import get_spark_object 
from validate import get_current_date
from ingest import load_files,display_df,df_count
from data_processing import *
from data_transformation import *
from persist import *
from time import perf_counter
import logging
import logging.config
import os
import sys

logging.config.fileConfig('Properties\configuration\logging.config')


def main():
    try:
        #print("Header : "+gav.header)
        #print("OLAP_Path : "+gav.src_olap)
        logging.info('calling spark object')
        spark = get_spark_object(gav.envn,gav.appName)
        #print('Object Created...',spark)
        
        logging.info('Validating spark object........')
        get_current_date(spark)

        #Reading OLAP file
        for file in os.listdir(gav.src_olap):
            file_dir=gav.src_olap+'\\'+file #extra '\' is given for escape sequence as it will give error otherwise
            print(file_dir)
            if file.endswith('.parquet'):
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'
            elif file.endswith('.csv'):
                file_format = 'csv'
                header = gav.header
                inferSchema = gav.inferSchema

        logging.info('reading file which is of > {} '.format(file_format))
        df_city = load_files(spark,file_dir,file_format,header,inferSchema)
        display_df(df_city,'df_city')
        logging.info('validating the dataframe')
        df_count(df_city,'df_City')

        #Reading OLTP file
        for file2 in os.listdir(gav.src_oltp):
            file_dir=gav.src_oltp+'\\'+file2 #extra '\' is given for escape sequence as it will give error otherwise
            print(file_dir)
            if file2.endswith('.parquet'):
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'
            elif file2.endswith('.csv'):
                file_format = 'csv'
                header = gav.header
                inferSchema = gav.inferSchema

        logging.info('reading file which is of > {} '.format(file_format))
        df_fact = load_files(spark,file_dir,file_format,header,inferSchema)
        display_df(df_fact,'df_fact')
        logging.info('validating the dataframe')
        df_count(df_fact,'df_fact')

        df_city_sel,df_presc_sel = data_clean(df_city,df_fact)
        display_df(df_city_sel,'df_city')
        display_df(df_presc_sel,'df_fact')

        #df_presc_sel.printSchema()

        logging.info("data_processing execution strted")
        df_report = data_report(df_city_sel,df_presc_sel)
        logging.info("displaying df_report")
        display_df(df_report,"df_report")
        
        df_report2 = data_report2(df_presc_sel)
        logging.info("displaying df_report2")
        display_df(df_report2,"df_report2")

        logging.info("Persisting df_report and df_report2")
        city_path=gav.city_path
        persist_file(df_report,'parquet',city_path,1,False,'snappy')

        presc_path=gav.presc_path
        persist_file(df_report2,'parquet',presc_path,2,False,'snappy')

        logging.info("Persisting df_report and df_report2 as hive table")
        data_hive_persist(spark,df_report,"df_city","state_name","append")
        data_hive_persist(spark,df_report2,"df_presc","presc_state","append")

    except Exception as exp:
        logging.error("An error occured when calling main() please check with the trace===", str(exp))
        sys.exit(1)

if __name__ == '__main__':
    start_time = perf_counter()
    main()
    end_time = perf_counter()
    logging.info("total amount of time taken: "+str((end_time - start_time))+" seconds")
    logging.info('Application done')