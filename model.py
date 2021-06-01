from dask.distributed import Client
import dask.dataframe as dd 
from dask_ml.linear_model import LogisticRegression
import pyarrow

# Feature dataset loader
def get_features(src='s3://dsc102-teamsj-scratch/origination_2018_q4_parquet4'):
	data = dd.read_parquet(src)
	return data

# Target dataset loader
def get_target(src='s3://dsc102-teamsj-scratch/label_2018_q4_parquet'):
	data = dd.read_parquet(src)
	return data

# Merge two datasets and prepare the training set for fitting model
def data_prep(df1, df2):
	merged = df1.join(df2, df1.Loan_Sequence_Number == df2.Loan_Sequence_Number)
	return merged

# Fir the model using Logistic Regression
def fit_model(data):
	lr = LogisticRegression()
	X,y = data.drop(columns='Target'), data['Target']
	lr.fit(X, y)
	return lr

def main():
	client = Client() # Ensure concurrency

	features = get_features()
	target = get_target()

	data = data_prep(features, target);

	model = fit_model(data)
	X = data.drop(columns='Target')
	preds = model.predict(X) # Make empirical prediction

	# Save prediced values as parquet in S3 bucket
	parquet_name = 's3://dsc102-teamsj-scratch/prediction_parquet'
	preds.to_parquet(parquet_name);

	print('finished')

if __name__ == '__main__':

	main()