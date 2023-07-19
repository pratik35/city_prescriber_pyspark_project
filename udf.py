from pyspark.sql.functions import *
from pyspark.sql.types import *

@udf(returnType=IntegerType())

def column_split_count(column):
    return len(column.split(' ')) #split function converts the output into list and len() function gets the length of the list