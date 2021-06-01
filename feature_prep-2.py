import dask.dataframe as dd
import dask.array as da
from dask_ml.preprocessing import DummyEncoder, StandardScaler, MinMaxScaler
from dask_ml.compose import ColumnTransformer
from dask.distributed import Client
import pyarrow.parquet as pq

def main():

	client = Client()

	# Specify data type for each attribute
	# If not specified, Pandas try to analyze data values and guess data types for all columns --> Low Memory Error

	types = [int, int, str, int, float, float, str, str, float, float, int, float, float, str, str, str, str, str, int, str, str, int, int, str, str, str, str, str, str, str, str]
	dtype = {i:types[i] for i in range(31)}
	usecols_ind = [0, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 17, 19, 20, 21, 29]

	# Import data with dtype specification and selected attributes
	data = dd.read_csv('origination_2018_q4.txt', sep='|', header=None, dtype=dtype, usecols=usecols_ind)

	# Rename attributes for better interpretation of each attribue
	cols = ['Credit Score', 'First Time Homebuyer Flag', 'MSA', 'MI %', 'Number of Units', 'Occupancy Status', 'CLTV', 'DTI', 'Original UPB', 'LTV', 'Original Interest Rate', 'Property Type', 'Loan Sequence Number', 'Loan Purpose', 'Original Loan Term', 'Property Valuation Method']
	data_cols = [col.replace(' ', '_') for col in cols]
	data_labels = {usecols_ind[i]:data_cols[i] for i in range(len(usecols_ind))}
	data = data.rename(columns=data_labels)

	# Preserve raw data, create copy of dataset to preprocess upon
	output = data.copy()

	# Drop rows with unidentified/missing credit score, missing loan sequence number, original UPB
	invalid_score = 9999
	output = output[output['Credit_Score'] != invalid_score]
	output = output.dropna(subset=['Credit_Score'])
	output = output.dropna(subset=['Loan_Sequence_Number'])
	output = output.dropna(subset=['Original_UPB'])

	# Replace unidentified homebuyer flag to N
	# homebuyer_replacer = {'9':'N'}
	output['First_Time_Homebuyer_Flag'] = output['First_Time_Homebuyer_Flag'].fillna('N').replace('9', 'N')

	# Fill in missing MSA with mean value
	msa_mean = output['MSA'].mean()
	output['MSA'] = output['MSA'].fillna(msa_mean)

	# Replace unidentified MI % and fill in missing MI % with 0
	ml_replacer = {999:0, '999':0}
	output['MI_%'] = (output['MI_%'].fillna(0).replace(999, 0))

	# Replace unidentified Number of Units and fill in missing Number of Units with 1
	num_units_replacer = {999:1, '999':1}
	output['Number_of_Units'] = output['Number_of_Units'].fillna(1).replace(999, 1)

	# Replace unidentified Occupancy status with P and fill in missing Occupancy status with P
	occupancy_replacer = {'9':'P', 9:'P'}
	output['Occupancy_Status'] = output['Occupancy_Status'].fillna('P').replace('9', 'P')

	# Replace and fill in missing CLTV, DTI, and LTV with their mean values
	cltv_mean = output['CLTV'].mean()
	dti_mean = output['DTI'].mean()
	ltv_mean = output['LTV'].mean()
	cltv_replacer = {999:cltv_mean, '999':cltv_mean}
	dti_replacer = {999:dti_mean, '999':dti_mean}
	ltv_replacer = {999:ltv_mean, '999':ltv_mean}

	output['CLTV'] = (output['CLTV'].fillna(cltv_mean).replace(999, cltv_mean)) 
	output['DTI'] = (output['DTI'].fillna(dti_mean).replace(999, dti_mean)) 
	output['LTV'] = (output['LTV'].fillna(ltv_mean).replace(999, ltv_mean))

	# Fill in missing interest rate with average rate
	interest_mean = output['Original_Interest_Rate'].mean()
	output['Original_Interest_Rate'] = (output['Original_Interest_Rate'].fillna(interest_mean))

	# Fill in missing loan term with a year
	year = 365
	output['Original_Loan_Term'] = output['Original_Loan_Term'].fillna(year)

	# Replace unidentified property valudation method and missing ones with 3
	method_replacer = {9:3, '9':3}
	output['Property_Valuation_Method'] = output['Property_Valuation_Method'].fillna('3').replace('9','3')

	# Replace and fill in missing property types with Single-Family
	type_replacer = {'99':'SF'}
	output['Property_Type'] = output['Property_Type'].fillna('SF').replace('99', 'SF')

	# Replace and fill in missing loan purpose with R
	purpose_replacer = {'9':'R', 9:'R'}
	output['Loan_Purpose'] = output['Loan_Purpose'].fillna('R').replace('9', 'R')

	ex = output.copy()
	# Transformers to extract features from raw attributes
	cat_cols = ('Number_of_Units', 'Occupancy_Status', 'First_Time_Homebuyer_Flag', 'Property_Type', 'Loan_Purpose', 'Property_Valuation_Method')
	output = output.categorize(columns=cat_cols)

	min_max_cols = ('MI_%', 'Original_Loan_Term')
	standardizing_cols = ('MSA', 'Original_Interest_Rate', 'Original_UPB')

	# One hot encode, or dummy encode, the categorical columns
	# Scale MI % and Original Loan Term with respect to their corresponding minimum and maximum values
	# Stajndardize other numerical columns except credit score
	clt = ColumnTransformer(transformers=[
			('One hot', DummyEncoder(), cat_cols),
			('MinMax', MinMaxScaler(), min_max_cols),
			('Standardize', StandardScaler(), standardizing_cols),
		], remainder='passthrough', n_jobs=-1)

	result = clt.fit_transform(output)
	result = result.repartition(npartitions=4)
	parquet_name = 'origination_2018_q4_parquet4'
	result.to_parquet(parquet_name)

	print('done')
	# Below two lines are used to store preprocessed origination dataset as parquet file
	# Commented out because I've stored them in S3 already.
	# parquet_name = 'origination_2018_q4_parquet4'
	# result.to_parquet(parquet_name)

if __name__ == '__main__':

	main()


