import dask.dataframe as dd
import dask.array as da
from dask_ml.preprocessing import DummyEncoder, StandardScaler, MinMaxScaler
from dask_ml.compose import ColumnTransformer
from dask.distributed import Client

# Change into multi-processing
client = Client()

# Specify data type for each attribute
# If not specified, Pandas try to analyze data values and guess data types for all columns --> Low Memory Error

types = [int, int, str, 
         int, float, float, str, str, float, 
         float, int, float, float, str,
         str, str, str, str,
         int, str, str, int, int,
         str, str, str, str, str, str, str, str]
dtype = {i:types[i] for i in range(31)}

# Import data with dtype specification
data = dd.read_csv(origination_file, sep='|', header=None, dtype=dtype)

# Column labels according to the guide
data_cols = ['Credit Score', 'First Payment Date',
            'First Time Homebuyer Flag', 'Maturity Date',
            'MSA', 'MI %', 'Number of Units', 'Occupancy Status',
            'CLTV', 'DTI', 'Original UPB', 'LTV',
            'Original Interest Rate', 'Channel',
            'PPM', 'Amortization Type', 'Property State',
            'Property Type', 'Postal Code', 'Loan Sequence Number',
            'Loan Purpose', 'Original Loan Term', 'Number of Borrowers',
            'Seller Name', 'Servicer Name', 'Super Conforming Flag',
            'Pre-HARP Loan Sequence Number',
            'Program Indicator', 'HARP Indicator', 'Property Valuation Method',
            'I/O Indicator']

data_labels = {i:data_cols[i] for i in range(31)}
data = data.rename(columns=data_labels)

data_df = data.compute() # Compute to drop columns

# Unnecessary columns to drop
drop_cols = ['Super Conforming Flag',
                         'Pre-HARP Loan Sequence Number',
                         'HARP Indicator',
                         'First Payment Date', 'Maturity Date',
                         'Channel', 'Property State',
                         'Postal Code', 'Number of Borrowers',
                         'Seller Name', 'Servicer Name',
                         'Program Indicator', 'I/O Indicator'] 
data_df = data_df.drop(columns=drop_cols) # 18 columns in total after drop

data = dd.from_pandas(data_df, npartitions=4) # Convert back into DASK dataframe

# `Credit Score` engineering; leave the raw values as is
invalid_score = 9999
data = data[data['Credit Score'] != invalid_score]
data = data.dropna(subset=['Credit Score'])

# Replace 9 to N in `First Time haomebuyer`; Dummy encode

data['First Time Homebuyer Flag'] = data['First Time Homebuyer Flag'].fillna('N').replace(homebuyer_replacer)

# Fill missing values with mean and standardize `MSA`
msa_mean = data['MSA'].mean().compute()
data['MSA'] = data['MSA'].fillna(msa_mean)

# Replace 999 in `MI %` to 0 and divide 100 since percentage; MinMaxScaler
ml_replacer = {999:0}
data['MI %'] = (data['MI %'].fillna(0).replace(ml_replacer)) / 100

# Replace 999 `Number of Units` to 1 and dummy encode
num_units_replacer = {999:1}
data['Number of Units'] = data['Number of Units'].fillna(1).replace(num_units_replacer)

# Replace 9 in Occupancy status to P and dummy encode
occupancy_replacer = {'9':'P'}
data['Occupancy Status'] = data['Occupancy Status'].fillna('P').replace(occupancy_replacer)

# Replace 999 in `CLTV`, `DTI`, `LTV`, median/mean and divide by 100. MixMaxScale these
cltv_mean = data['CLTV'].mean().compute()
dti_mean = data['DTI'].mean().compute()
ltv_mean = data['LTV'].mean().compute()
cltv_replacer = {999:cltv_mean}
dti_replacer = {999:dti_mean}
ltv_replacer = {999:ltv_mean}

data['CLTV'] = (data['CLTV'].fillna(cltv_mean).replace(cltv_replacer)) / 100
data['DTI'] = (data['DTI'].fillna(dti_mean).replace(dti_replacer)) / 100
data['LTV'] = (data['LTV'].fillna(ltv_mean).replace(ltv_replacer)) / 100

# Divide `Original Interest Rate` by 100 and standardize
data['Original Interest Rate'] = data['Original Interest Rate'] / 100

# `PPF` and `Amorization type` dummy encode

# Replace 99 property with SF and dummy encode
property_replacer = {'99':'SF'}
data['Property Type'] = data['Property Type'].fillna('SF').replace(property_replacer)

# Replace 9 in Loan Purpose with R or drop 9 and dummy encode
purpose_replacer = {'9':'R'}
data['Loan Purpose'] = data['Loan Purpose'].fillna('R').replace(purpose_replacer)

# MinMaxScaler `Original Loan Term`
year = 365
data['Original Loan Term'] = data['Original Loan Term'].fillna(year)

# Replace 9 in `Property Valuation Method` with 3 and dummy encode.
method_replacer = {9:3}
data['Property Valuation Method'] = data['Property Valuation Method'].fillna(3).replace(method_replacer)

# Extract feature from raw values
cat_cols = ('Number of Units', 'Occupancy Status', 'First Time Homebuyer Flag', 'Property Type', 'Loan Purpose', 'Property Valuation Method', 'PPM', 'Amortization Type')
min_max_cols = ('MI %', 'Original Loan Term')
standardizing_cols = ('MSA', 'Original Interest Rate')

clt = ColumnTransformer(transformers=[
		('One hot', DummyEncoder(), cat_cols),
		('MinMax', MinMaxScaler(), min_max_cols),
		('Standardize', StandardScaler(), standardizing_cols),
	], remainder='passthrough')
