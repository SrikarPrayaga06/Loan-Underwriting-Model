import pyspark.sql.functions as f
from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import *

# Raw monthly dataset loader
def get_monthly(spark, src='s3://dsc102-teamsj-scratch/monthly_2018_q4.txt'):
	returner = spark.read.format('csv').option('delimiter', '|').load(src)
	return returner

# Spark session generator 
def generate_spark():
	spark = SparkSession.builder.config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.1').getOrCreate()
	return spark

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

# Label creator after grouping by Loan Sequence Number
# Those with average target score of 0.5 or higher will get labeled as default
# Otherwise, non-default, or 0
def target_mask(val):
	if val >= 0.5:
		return 1
	else:
		return 0

def label_prep(spark):
	monthly = get_monthly(spark)

	sc = SparkContext('local')
	sqlContext = SQLContext(sc)
	function = f.udf(lambda x,y:func(x,y), IntegerType())
	sqlContext.udf.register("function", function)

	mask = f.udf(lambda z: target_mask(z), IntegerType())
	sqlContext.udf.register("masker", mask)

	monthly_cols_ind = ['_c0', '_c3', '_c8']
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
	monthly = monthly.withColumn('Current Loan Delinquency Status', monthly['Current Loan Delinquency Status temp'].cast('int')).drop('Current Loan Delinquency Status temp')

	# Append created labels to monthly dataset and store in separate target variable
	result = monthly.withColumn('Target', function('Current Loan Delinquency Status', 'Zero Balance Code'))

	# Only keep key and target variable
	output = result.select(['Loan Sequence Number', 'Target'])

	# Need to rename Loan Sequence Number to store output as parquet
	output = output.withColumnRenamed('Loan Sequence Number', 'Loan_Sequence_Number')

	# Group by Loan Sequnece Number
	grouped = output.groupby('Loan_Sequence_Number').agg({'Target':'mean'})
	returner = grouped.withColumn('Label', mask('avg(Target)')).drop('Target')

	return returner

def main():
	spark = generate_spark()
	output = label_prep(spark)

	print('finished')


if __name__ == '__main__':
	main()

