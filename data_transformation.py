import logging.config
from udf import *
from pyspark.sql import *

logging.config.fileConfig('Properties\configuration\logging.config')
logger = logging.getLogger('Data_transformation')

def data_report(df_city_sel,df_presc_sel):
    try:
        logger.warning("processing in data_report method..")
        logger.warning("calculating total zip counts in {}".format(df_city_sel))
        df_city_split = df_city_sel.withColumn('zip_count',column_split_count(col("zips")))
        logger.warning("calculating distinct prescribers with total txn cnt")
        df_presc_grp = df_presc_sel.groupBy(col("presc_state"),col("presc_city")).agg(countDistinct("presc_id").alias("presc_counts"),sum("tx_cnt"))
        logger.warning("Don't report a city if no prescriber assigned to it....let's join df_city_sel and df_presc_sel")
        df_city_join = df_city_split.join(df_presc_grp,(df_city_split.state_id == df_presc_grp.presc_state) & (df_city_split.city == df_presc_grp.presc_city),'inner')
        df_final = df_city_join.select("city","state_name","country_name","population","zip_count","presc_counts")
    except Exception as exp:
        logger.error("an error occured while running data_report function",str(exp))
        raise
    else:
        logger.warning("Data_report successfuly executed...go forward")
    return df_final


def data_report2(df_presc_sel):
    try:
        logger.warning("processing in data_report2 method..")
        logger.warning("executing the task ::: consider the prescribers only from 20 to 50 years_of_exp and rank the prescriber based on their tx_cnt for each state")
        spec = Window.partitionBy("presc_state").orderBy(col('tx_cnt').desc())
        df_presc_report = df_presc_sel.select("presc_id","presc_fullname","presc_state","Country_name","years_of_exp","tx_cnt","total_day_supply","total_drug_cost",) \
                                      .filter((df_presc_sel.years_of_exp >= 20) & (df_presc_sel.years_of_exp <= 50)) \
                                      .withColumn("dense_rank",dense_rank().over(spec)) \
                                      .filter(col("dense_rank") <= 5)

    except Exception as exp:
        logger.error("an error occured when processing data_report2 method :::",str(exp))
        raise
    else:
        logger.warning("Data_report2 successfuly executed...go forward")
    return df_presc_report
