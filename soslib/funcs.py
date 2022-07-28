import numpy as np
import pandas as pd
import xarray as xr
import datetime as dt
import requests
import pytz
import ftplib
import nctoolkit as nc

def get_daily_radsys_data(start, end):
    """Collects 1-minute daily radsys data.
    
    Variable include:
    -----------------
    start/end: Start/end datetime as string
    -----------------
    Returns: xarray dataset
    """
    # url to request from
    base_url = 'https://gml.noaa.gov/aftp/data/radiation/campaigns/Format/ckp/'

    # format dates
    if not isinstance(start, dt.date): 
        start = dt.datetime.strptime(start,'%Y-%m-%d')
    if not isinstance(end, dt.date): 
        end = dt.datetime.strptime(end ,'%Y-%m-%d')

    # get time delta    
    delta = end - start
    
    url_list = []
    for i in range(delta.days + 1):
        date = start + dt.timedelta(days=i)
        if date.timetuple().tm_yday < 10:
            url_list.append(base_url+f'ckp{str(date.year)[-2:]}00{date.timetuple().tm_yday}.dat')
        elif date.timetuple().tm_yday < 100:
            url_list.append(base_url+f'ckp{str(date.year)[-2:]}0{date.timetuple().tm_yday}.dat')
        else: 
            url_list.append(base_url+f'ckp{str(date.year)[-2:]}{date.timetuple().tm_yday}.dat')
    
    datasets = []
    for fn in url_list:
        print(fn)
        # read in data
        ckp_df = pd.read_csv(fn, 
                             skiprows=2, 
                             header=None, 
                             delim_whitespace=True, 
                             parse_dates={'time':[0,2,3,4,5]}, 
                             infer_datetime_format=True)
        datasets.append(met_data_formatting(ckp_df))
    radsys_ds = xr.concat(datasets, dim='time')
    radsys_ds.attrs.update({'QC_flag':'0 for good data, 1 for bad data, 2 for questionable data',
                       'no_data': -9999.9,
                       'time':'UTC',
                       'reported_data':'Reported data are 1 minute averages of 1 second samples, reported times are the end of the 1-min. averaging period',
                       'datastreamname':'Radsys'})
    # add a coordinate with LocalTime for Plotting
    time_utc = radsys_ds['time'].to_index().tz_localize(pytz.UTC)
    us_mtn = pytz.timezone('US/Mountain')
    tz_corrected = time_utc.tz_convert(us_mtn).tz_localize(None)
    local_da=xr.DataArray.from_series(tz_corrected)
    radsys_ds.coords.update({'local_time':tz_corrected})


    # with open('./radsys_attributes.txt', 'r') as j:
    #     attribute_dict = json.loads(j.read())
    attributes = attribute_dict()
    for variable in radsys_ds.variables:
        if variable in attributes.keys():
            radsys_ds[variable].attrs.update(attributes[variable])
    return radsys_ds

def met_data_formatting(ckp_df):
    """Formatting for radsys data"""
    # Convert DateTime column to datetime
    ckp_df['time'] = pd.to_datetime(ckp_df['time'], format='%Y %m %d %H %M')
    # Add column numbers
    col_num = [1,2,3,4,5,6,7,8,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39,41,43,45,47,49,51]
    col_num = [num - 1 for num in col_num]
    col_names = ["yyyy","jday","month","day","hour","min","dt","SZA","dw_solar","uw_solar","Direct horizontal","Diffuse",
                "dw_ir","DwCaseTemp","DwDomeTemp","uw_ir","UwCaseTemp","UwDomeTemp","UVB","PAR","NetSolar","NetIR",
                "TotalNet","AirTemp","RH","WindSpd","WindDir","Baro","SPN1_total_Avg","SPN1_diffuse_Avg"]
    ckp_df = ckp_df.rename(columns = dict(zip(col_num, col_names))) 
    # Create QC column names
    qc_col_numbers = ckp_df.columns[5::2]
    new_qc_names = [f'{col_name}_qc' for col_name in ckp_df.columns[4::2]]
    ckp_df = ckp_df.rename(columns = dict(zip(qc_col_numbers,new_qc_names)))

    ckp_ds = ckp_df.set_index('time', drop=True).to_xarray()
    return ckp_ds

def get_asfs_data(product, product_lvl, avg_time, start, end, raw=False, drop_freq=True): 
    """Collects desired ASFS data.

    Variable include:
    -----------------
    product:
    Kettle Ponds product code: asfs30
    Avery Picnic product code: asfs50
    -----------------
    product_lvl: 0, 1, 2
    -----------------
    avg_time: Averaging time either 10 or 30 minutes
    -----------------
    start/end: Start/end datetime as string
    -----------------
    raw: boolean for if raw data is desired
    -----------------
    Returns: xarray dataset
    """
    # Change to datetime if not given in this format
    if not isinstance(start, dt.date): 
        start = dt.datetime.strptime(start,'%Y-%m-%d')
    if not isinstance(end, dt.date): 
        end = dt.datetime.strptime(end ,'%Y-%m-%d')

    if product not in ['asfs30', 'asfs50']:
        print('Check product name, must be asfs30 or asfs50')
        return
    # Get all file names to iterate through
    files = get_asfs_files(product, product_lvl, avg_time, start, end, raw)
    
    # Initalize list to store datasets
    datasets = []

    # Iterate to get datasets for each day
    for i, file in enumerate(files):
        # Grab file using nctoolkit
        ds = nc.open_url( f'ftp://ftp1.esrl.noaa.gov/Observations/Campaigns/SPLASH/{product}/{product_lvl}_level_ingest/{file}')
        print(f'Loading {file}...')
        
        # Drop frequency dimension, if desired
        if drop_freq:
            print("Dropping freq dimension due to concatenation issues.")
            ds = ds.drop_dims('freq')
        # Add dataset to list
        datasets.append(ds.to_xarray())
    
    # Concatenate files here
    all_file_ds = xr.concat(datasets,dim='time')
    
    # Run deep clean to remove all loaded files
    nc.deep_clean()
    
    print('Done!')
    return all_file_ds


def get_asfs_files(product, product_lvl, avg_time, start, end, raw=False):
    """Gets the ASFS sled files from the specified location and time frame"""
    print('Connecting to FTP Server...')
    # Connect to NOAA's FTP
    ftp = ftplib.FTP('ftp1.esrl.noaa.gov') 
    # Anonymous login
    ftp.login() 
    # Navigate to file location
    ftp.cwd(f'./Observations/Campaigns/SPLASH/{product}/{product_lvl}_level_ingest')
    # Get list of all files
    files = ftp.nlst() 
    # Close FTP connection
    ftp.close()

    sel_files = []
    # Iterate to get file names depending on input dates and averaging time/raw data
    for file in files:
        if not raw and (f'{avg_time}min') in file:
            if dt.datetime.strptime(file.split(sep='.')[3],'%Y%m%d') >= start:
                if dt.datetime.strptime(file.split(sep='.')[3],'%Y%m%d') < end:
                    sel_files.append(file)
        elif raw and len(file.split('.') == 6):
            if dt.datetime.strptime(file.split(sep='.')[3],'%Y%m%d') >= start:
                if dt.datetime.strptime(file.split(sep='.')[3],'%Y%m%d') < end:
                    sel_files.append(file)
    print(f'Got {len(sel_files)} files!')
    return sel_files


def get_awdb_data(site_ids, element="WTEQ", sdate=dt.datetime(1899,10,1), edate=dt.datetime.now(), orient="records", server="https://api.snowdata.info/", sesh=None):
    """
    Adapted from Beau Uriona: https://github.com/snowex-hackweek/space-station/blob/main/contributors/beau-uriona/get_data.ipynb
    Takes a list of site ids or a single site id and by default returns SWE period of record data as a single or list of dataframes,
    but user can pass args to modify data returned.
    Variable include:
    -----------------
    site_ids: 
    site_id takes the form of a triplet made from <network_site_id>:<state_abbrv>:<network> where network is either SNTL or MNST
    -----------------
    element: 
    Valid elements include WTEQ, SNWD, PREC, SMS, STO, TAVG
    -----------------
    sdate: Start date as datetime object
    -----------------
    edate: End date as datetime object
    -----------------
    All other variables are not required
    -----------------
    Returns: dataframe
    """
    dfs = []
    return_single = False
    if not isinstance(site_ids, list):
        site_ids = [site_ids]
        return_single = True
    for site_id in site_ids:
        endpoint = "data/getDaily"
        date_args = f"sDate={sdate:%Y-%m-%d}&eDate={edate:%Y-%m-%d}"
        frmt_args = f"format=json&orient={orient}"
        all_args = f"?triplet={site_id}&{date_args}&element={element}&{frmt_args}"
        url = f"{server}{endpoint}{all_args}"
        print(f"getting data for {site_id} {element} starting {sdate:%Y-%m-%d} and ending {edate:%Y-%m-%d}")
        data_col_lbl = f"{site_id}" + ":" + f"{element}"
        if sesh:
            req = sesh.get(url)
        else:
            req = requests.get(url)
        if req.ok:
            df = pd.DataFrame.from_dict(req.json())
            df.columns = ["Date", data_col_lbl]
            df.set_index("Date", inplace=True)
        else:
            print("  No data returned!")
            df = pd.DataFrame(
                data=[
                    {"Date": pd.NaT, data_col_lbl: np.nan}
                ],
            ).set_index("Date").dropna()
        dfs.append(df)
    if return_single:
        return dfs[0]
    return dfs

def attribute_dict():
    ad = {"SZA":
        {"value":"solar zenith angle",
        "units":"Degrees",
        "type":"Float",
        "QC":"Calc From Timestamp and Lat/Lon"},
        "dw_solar":
        {"value":"downwelling global solar",
        "units":"W/m^2",
        "type":"Float",
        "QC":"QC = Col 10"},
        "uw_solar":
        {"value":"upwelling global solar",
        "units":"W/m^2",
        "type":"Float",
        "QC":"QC = Col 12"},
        "Direct horizontal":
        {"value":"Direct horizontal solar",
        "units":"W/m^2",
        "type":"Float",
        "QC":"QC = Col 14, calculated as described below"},
        "Diffuse":
        {"value":"Downwelling diffuse solar",
        "units":"W/m^2",
        "type":"Float",
        "QC":"QC = Col 16, calculated as described below"},
        "dw_ir":
        {"value":"downwelling thermal infrared",
        "units":"W/m^2",
        "type":"Float",
        "QC":"QC = Col 18"},
        "DwCaseTemp":
        {"value":"downwelling IR case temp.",
        "units":"Kelvin",
        "type":"Float",
        "QC":"QC = Col 20"},
        "DwDomeTemp":
        {"value":"downwelling IR dome temp.",
        "units":"Kelvin",
        "type":"Float",
        "QC":"QC = Col 22"},
        "uw_ir":
        {"value":"upwelling thermal infrared",
        "units":"W/m^2",
        "type":"Float",
        "QC":"QC = Col 24"},
        "UwCaseTemp":
        {"value":"upwelling IR case temp.",
        "units":"Kelvin",
        "type":"Float",
        "QC":"QC = Col 26"},
        "UwDomeTemp":
        {"value":"upwelling IR dome temp.",
        "units":"Kelvin",
        "type":"Float",
        "QC":"QC = Col 28"},
        "UV":
        {"value":"global UVB",
        "units":"W/m^2",
        "type":"Float",
        "QC":"QC = Col 30, not usually measured with RADSYS"},
        "PAR":
        {"value":"photosynthetically active radiation",
        "units":"W/m^2",
        "type":"Float",
        "QC":"QC = Col 32, not usually measured with RADSYS"},
        "NetSolar":
        {"value":"net solar (dw_solar - uw_solar)",
        "units":"W/m^2",
        "type":"Float",
        "QC":"QC = Col 34"},
        "NetIR":
        {"value":"net infrared (dw_ir - uw_ir)",
        "units":"W/m^2",
        "type":"Float",
        "QC":"QC = Col 36"},
        "TotalNet":
        {"value":"net radiation (netsolar+netir)",
        "units":"W/m^2",
        "type":"Float",
        "QC":"QC = Col 38"},
        "AirTemp":
        {"value":"10-meter air temperature",
        "units":"Celcius",
        "type":"Float",
        "QC":"QC = Col 40"},
        "RH":
        {"value":"relative humidity",
        "units":"%",
        "type":"Float",
        "QC":"QC = Col 42"},
        "WindSpd":
        {"value":"wind speed",
        "units":"m/s",
        "type":"Float",
        "QC":"QC = Col 44"},
        "WindDir":
        {"value":"wind direction",
        "units":"Degrees",
        "type":"Float",
        "QC":"QC = Col 46"},
        "Baro":
        {"value":"station pressure",
        "units":"mBar",
        "type":"Float",
        "QC":"QC = Col 48"},
        "SPN1_total_Avg": 
        {"value":"Total irradiance as measured by SPN1",
        "units":"W/m^2",
        "type":"Float",
        "QC":"QC = Col 50"},
        "SPN1_diffuse_Avg":
        {"value":"Diffuse irradiance as measured by SPN1",
        "units":"W/m^2",
        "type":"Float",
        "QC":"QC = Col 52"}}
    return ad
