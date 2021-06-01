# Download package information from all contributed sources in virtual env
# Lets the user know which package or file need updates
# sudo apt-get update

# sudo apt-get install awscli

# If Python not installed in OS of EC2 instance, need to run
sudo apt-get install python

# If command line instruction needed for creating virtual environment not present,
sudo apt-get install python3-venv

# Creating virtual environment named `env`
python3 -m venv env

# Accessing into env enviroment; generate isolated Python environment
# source /env/bin/activate

# In `env` virtual environment, install DASK librarypy
# pip install "dask[complete]" --no-cache-dir

# Install DASK Machine Learning package
# pip install dask-ml

# Install fastparquet library to save output file as parquet
# pip install fastparquet pyarrow -- Does not work, why?