# leneda_monthly-stats
These 2 python scripts are used to use the Leneda API in order to get monthly statistics from the different counters. 
## Retrieve Data  
The first script, `get_monthly_data.py` , retrives the data, generates reports in json, xlsx, and csv formats.  
It also creates and manages a sqlite3 database.  
The script is called like this : `python energy_fetcher.py --year 2025 --month 9`  
The year and month are optional. By default, the script uses the data from the previous month.  
Running the script with `python get_monthly_data.py --help` generates some help how to use it.  
The output data, and the database, are stored by default in `./data`, the yaml config file by default needs to be in the `./configs` folder  
## Get statistics  
The second script `analyse_monthly_data.py` uses the sqlite database and gnereates an interactive app to make some basic analyses of the data.  
The script is called like so `python energy_dashboard.py` without any parameters, and creates a webapp available at `http://127.0.0.1:8050`  

Please use the provided `requirements.txt` file to install all required modules  
## Prerequisites  
In order to be able to use these script, you need to hve acces to the leneda api.  
Informations about how to get this access, please check here `https://leneda.eu/fr/docs/api-reference.html`.   
You also need a yaml file, the default name is `monthly.yaml`, and the file is in the `.configs folder`.  
this yaml file conatines the description of the different users, and there POD's, the Leneda ID of the smartmeter.  
It also contains a list of the OBIS Codes to be retrieved.  
A sample file called `monthly_init.yaml` is provided in the repository. Please adapt the file to your needs, and rename ut to `monthy.yaml`.  
