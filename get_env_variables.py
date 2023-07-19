import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['envn'] = 'DEV'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'

header = os.environ['header']
inferSchema = os.environ['inferSchema']
envn = os.environ['envn']
appName = 'PROJECT'
current_path = os.getcwd() #gives current path of the project
src_olap = current_path + '\source\olap'
src_oltp = current_path + '\source\oltp'
city_path = 'output\cities'
presc_path = 'output\prescriber'