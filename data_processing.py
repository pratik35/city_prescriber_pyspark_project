import logging.config

from pyspark.sql.functions import *
from pyspark.sql import *


logging.config.fileConfig('Properties\configuration\logging.config')
logger = logging.getLogger('Data_processing')

def data_clean(df1,df2):
    try:
        logger.warning('data_clean method() started...')
        logger.warning('selecting required columns and converting some of columns into upper case...')
        
        df_city_sel = df1.select(upper(col('city')).alias('city'),col('state_id'),upper(col('state_name')).alias('state_name'),upper(df1.county_name).alias('country_name'),df1.population,df1.zips)

        logger.warning('working on OLTP dataset and selecting couple of columns and rename....')

        df_presc_sel = df2.select(df2.npi.alias('presc_id'),df2.nppes_provider_last_org_name.alias('presc_lname'),df2.nppes_provider_first_name.alias('presc_fname'),
                                 df2.nppes_provider_city.alias('presc_city'),df2.nppes_provider_state.alias('presc_state'),df2.specialty_description.alias('presc_spclt'),
                                 col('drug_name'),df2.total_claim_count.alias('tx_cnt'),df2.total_day_supply,df2.total_drug_cost,df2.years_of_exp)
        
        logger.warning('Adding a new column to df_presc_sel')
        df_presc_sel = df_presc_sel.withColumn('country_name',lit('USA')) \
                                   .withColumn('years_of_exp',regexp_replace(col('years_of_exp'), r"^="," ")) \
                                   .withColumn('years_of_exp',col('years_of_exp').cast('int')) \
                                   .withColumn('presc_fullname', concat_ws(" ",'presc_fname','presc_lname')) \
                                   .drop('presc_lname','presc_fname')
        
        logger.warning('dropping null values and filling null values')
        df_presc_sel = df_presc_sel.dropna(subset="presc_id")
        df_presc_sel = df_presc_sel.dropna(subset="drug_name")
        mean_tx_cnt = df_presc_sel.select(mean(col('tx_cnt')).alias('mean')).collect()[0][0] #collect() converts the dataset into list(Row()) and [0][0] gets the first row and column
        df_presc_sel = df_presc_sel.fillna(mean_tx_cnt,'tx_cnt') #fills the rows of the column 'tx_cnt' with the value present in the variable mean_txn_cnt
        

                                   
    except Exception as exp:
        logger.error("An error occured at data_clean() method===",str(exp))
    else:
        logger.warning("data_clean() method executed done, go forward....")
    return df_city_sel,df_presc_sel