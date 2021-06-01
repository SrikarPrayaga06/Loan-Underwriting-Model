import pyspark.sql.functions as f
# from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import IntegerType
# from pyspark.sql import HiveContext
# import json
# import sys


def main():
	# Target value generator
	# This function will later get registered and be applied on each row of our monthly dataset
	def func(status, balance):
		if (status >= 3): # According to writeup, "Your target label is defined as default at 90 days or more delinquent."
			return 1      # According to description on guide, values bigger than or equal to 3 indicate 90 or more days delinquent

		if ((balance == '03') | (balance == '06') | (balance == '09')): # According to writeup, "if you find ‘Zero Balance Code’ in the performance data listed as ’03’, ’06’, or ’09’,
																	    # the loan should also be considered a default."
			return 1
		else:
			return 0

	# Loading file into pyspark
	# sc = spark.SparkContext('local') <- Pre defined
	log_txt = sc.textFile("s3://dsc102-teamsj-scratch/monthly_2018_q4.txt")
	# sqlContext = SQLContext(sc) <- Pre defined
	temp = log_txt.map(lambda k: k.split("|")) # Split the values around '|' for every row
	monthly = temp.toDF()
	# sc = SparkContext(appName='Name')

	# spark = SparkSession.builder.appName("Name").getOrCreate()
	# sc = spark_session.SparkContext

	# monthly = spark.read.csv('s3://dsc102-teamsj-scratch/monthly_2018_q4.txt', header=False, inferSchema=True, sep='|')
	# print(monthly.collect())

	# Register the function beforehand
	function = f.udf(lambda x,y:func(x,y), IntegerType())
	sqlContext.udf.register("function", function)

	# Drop unnecessary columns; only need 'Loan Sequence Number' as key, `Current Loan Delinquency Status` and `Zero Balance Code` for label generation.
	monthly_cols_ind = ['_1', '_4', '_9']
	monthly = monthly.select(monthly_cols_ind)

	# Rename column labels
	col_labels = ['Loan Sequence Number', 'Current Loan Delinquency Status temp', 'Zero Balance Code']

	for i in range(len(monthly.columns)):
		monthly = monthly.withColumnRenamed(monthly.columns[i], col_labels[i])


	# Fill missing entries with arbitrary filler values
	monthly = monthly.fillna('-1', subset=['Current Loan Delinquency Status temp'])
	monthly = monthly.fillna('', subset=['Zero Balance Code'])

	# Replace unidentified values to some negative value to indicate them as non-default
	status_replacer = {'XX':'-1', 'R':'-10', 'Space (3)':'-12'}
	monthly = monthly.replace(status_replacer, subset=['Current Loan Delinquency Status temp'])

	# Modify dtype of delinquency status to int to match the type required for our `func`
	monthly = monthly.withColumn('Current  ', monthly['Current Loan Delinquency Status temp'].cast('int')).drop('Current Loan Delinquency Status temp')

	# Append created labels to monthly dataset and store in separate target variable
	result = monthly.withColumn('Target', function('Current Loan Delinquency Status', 'Zero Balance Code'))

	# Only keep key and target variable
	output = result.select(['Loan Sequence Number', 'Target'])

	# Need to rename Loan Sequence Number to store output as parquet
	output = output.withColumnRenamed('Loan Sequence Number', 'Loan_Sequence_Number')

	print('all good!')

if __name__ == '__main__':
	# SparkConf.setMaster('local[*]')
	# sc = SparkContext(appName='demo')
	conf = SparkConf()
	sc = SparkContext(conf=conf)
	main()

	sc.stop()
	# spark_session.stop()

# Below are the codes we ran to store our label prepped output file as parquet directly in S3 bucket
# Commented out since already done so
# parquet_name = 's3://dsc102-teamsj-scratch/monthly_2018_q4_parquet4'
# output.write.parquet(parquet_name)

