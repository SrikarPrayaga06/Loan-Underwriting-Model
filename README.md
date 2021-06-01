# Loan-Underwriting-Model
# Preprocessing 
  Using the Originaton dataset from Freddie Mac we  used  Dask on an EC2 instance where we dropped unnecessary columns, imputed missing values and scaled values. Dropped columns include features like 'Super Conforming Flag,Pre-HARP Loan Sequence Numbe,'HARP Indicator',  'First Payment Date', 'Maturity Date', 'Channel', 'Property State', 'Postal Code', 'Number of Borrowers','Seller Name', 'Servicer Name,'Program Indicator',  and 'I/O Indicator'. Missing values in remaining features were imputed by using the mean or dropped. We also transformed specific features by one hot encoding and min max scaling. The Dask data frame of these transformed features are outputted as a parquet file.

# Label prep
The target variable is "Current Loan Delinquency Status" with the "Loan Sequence Number" extracted from the monthly performance dataset.  The missing values were either dropped or imputed in the case of "Zero Balance Code" having a certain value. These steps were all performed on EMR using pySpark. 

# Model Building
The parquet files from the preprocessing step and the target column from label prep are joined on the Loan Sequence Number. A simple logistic model was built with the columns extracted from the preprocessing step as the feature set and the processed column from label prep as the target variable. 

# Data Flow Graph 
![alt text](https://github.com/SrikarPrayaga06/Loan-Underwriting-Model/blob/main/graph.png)

