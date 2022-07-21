import pandas as pd
from nctoolkit.api import open_data, open_thredds
import xarray as xr
from nctoolkit.mp_utils import get_type
from nctoolkit.matchpoint import open_matchpoint


def match_points(ds=None, df = None, variables=None, depths = None, nan=None, top = False, tmean = False, regrid = "bil", max_extrap = 5):
    """
    Match netCDF data to a spatiotemporal points dataframe

    Parameters
    -------------
    ds: nctoolkit dataset or str/list of file paths
        Dataset or file(s) to match up with
    df: pandas dataframe containing the spatiotemporal points to match with.
        The column names must be made up of a subset of "lon", "lat", "year", "month", "day" and "depth"
    variables: str or list
        Str or list of variables. All variables are matched up if this is not supplied.
    depths:  nctoolkit dataset or list giving depths
        If each cell has different vertical levels, this must be provided as a dataset.
        If each cell has the same vertical levels, provide it as a list.
        If this is not supplied nctoolkit will try to figure out what they are.
        Only required if carrying out vertical matchups.
    tmean: bool
        Set to True or False, depending on whether you want temporal averaging at the temporal resolution given by df.
        For example, if you only had months in df, but had daily data in ds, you might want to calculate a daily average in the
        monthly dataset.
        This is equivalent to apply `ds.tmean(..)` to the dataset.
    nan: float or list
        Value or range of values to set to nan. Defaults to 0.
        Only required if values in dataset need changed to missing
    top: bool
        Set to True if you want only the top/surface level of the dataset to be selected for matching.
    regrid: str
        Regridding method. Defaults to "bil". Options available are those in nctoolkit regrid method.
        "nn" for nearest neighbour.
    max_extrap: float
        Maximum distance for vertical extrapolation of values 

    Returns
    ---------------
    matchpoints : pandas.DataFrame

    """

    mp = open_matchpoint()

    mp.add_data(x = ds, variables = variables, depths = depths, nan = nan, top = top)
    mp.add_points(df)
    mp.matchup(tmean = tmean, regrid = regrid, max_extrap = max_extrap)
    return mp.values


def add_data(self, x=None, variables=None, depths = None, nan=None, top = False):
    """
    Add dataset for matching

    Parameters
    -------------
    x: nctoolkit dataset or str/list of file paths
        Dataset or file(s) to match up with
    variables: str or list
        Str or list of variables. All variables are matched up if this is not supplied.
    depths:  nctoolkit dataset or list giving depths
        If each cell has different vertical levels, this must be provided as a dataset.
        If each cell has the same vertical levels, provide it as a list.
        If this is not supplied nctoolkit will try to figure out what they are.
        Only required if carrying out vertical matchups.
    nan: float or list
        Value or range of values to set to nan. Defaults to 0.
        Only required if values in dataset need changed to missing
    top: bool
        Set to True if you want only the top/surface level of the dataset to be selected for matching.

    """
    thredds = False
    try:
        if len(x.history) == 0:
            thredds = x._thredds
    except:
        thredds = False
    self.thredds = thredds

    if depths is not None:
        self.add_depths(depths)

     ##need to figure out what depths are if they are not provided.
    if depths is None:

        if thredds:
            ds = open_thredds(x, checks = False)
            ds = open_thredds(ds[0], checks = False)
        else:
            ds = open_data(x, checks = False)
            ds = open_data(ds[0])
        if len(ds.levels) > 1:
            if "e3t" in ds.variables:
                ds_depths = ds.copy()
                ds_depths.subset(time = 0)
                ds_depths.subset(variable = "e3t")
                ds1 = ds_depths.copy()
                ds_depths.vertical_cumsum()
                ds1.run()
                ds1.divide(2)
                ds_depths.subtract(ds1)
                self.depths = ds_depths.copy()
                self.depths.rename({"e3t":"depth"})
                print("Depths were derived from e3t variable.")
            else:
                try:
                    self.depths = ds.levels
                    print(f"Depths assumed to be {self.depths}")
                except:
                    raise ValueError("Unable to derive depths from the dataset! Please provide them.")


    if depths is None:
        if self.points is not None:
            if "depth" in self.points:
                raise ValueError("You cannot match depths without supplying dataset depths")

    self.top = top

    if variables is not None:
        if type(variables) is str:
            variables = [variables]

    if self.data is not None:
        raise ValueError("You have already added data!")

    if variables is None:
        print("All variables will be used")


    if thredds:
        self.data = open_thredds(x, checks = False)
        ds_vars = open_thredds(self.data[0], checks = False)
    else:
        self.data = open_data(x, checks = False)
        ds_vars = open_data(self.data[0])

    ds_variables = ds_vars.variables

    if type(variables) is list:
        for x in variables:
            if x not in ds_variables:
                raise ValueError(f"{x} is not a valid variable")

    if len(self.data) > 12:
        print("Checking file times. This could take a minute")

    self.data_nan = nan

    # figure out the time dim

    if thredds:
        ds1 = open_thredds(self.data[0], checks = False)
    else:
        ds1 = open_data(self.data[0])
    pos_times = [
        x
        for x in [
            x for x in list(ds1.to_xarray().dims) if x in list(ds1.to_xarray().coords)
        ]
        if "time" in x
    ]

    if len(pos_times) != 1:
        print("Unable to work out the name of time. Assuming no temporal matchups can occur.")
        self.temporal = False

    if self.temporal:

        if len(pos_times) == 1:
            time_name = pos_times[0]

        df_times = []

        for ff in self.data:
            if thredds:
                ds_ff = open_thredds(ff)
            else:
                ds_ff = open_data(ff)
            ds = ds_ff.to_xarray()
            times = ds[time_name]
            days = [int(x.dt.day) for x in times]
            months = [int(x.dt.month) for x in times]
            years = [int(x.dt.year) for x in times]
            df_times.append(
                pd.DataFrame({"day": days, "month": months, "year": years}).assign(path=ff)
            )
        df_times = pd.concat(df_times)

        x = list(set(df_times.path))

    if len(self.data) > 12:
        print("Finished checking file times.")

    if thredds:
        self.data = open_thredds(x, checks = False)
    else:
        self.data = open_data(x, checks = False)

    self.variables = variables

    if self.temporal:
        self.data_times = df_times
    else:
        self.data_times = None

    if self.temporal is False:
        if len(self.data) > 1:
            raise ValueError("You cannot provide more than one dataset without temporal information")



def add_depths(self, x=None):
    """
    Add depth

    Parameters
    -------------
    x:  nctoolkit dataset or list/iterable
        If each cell has different vertical levels, this must be provided as a dataset.
        If each cell has the same vertical levels, provide it as a list.

    """

    if self.depths is not None:
        raise ValueError("You have already provided depths")

    if "api.DataSet" not in str(type(x)):
        self.depths = [y for y in x]

    if type(x) != list:
        if len(x.variables) > 1:
            raise ValueError("Depths file should only have one variable")

        self.depths = x.copy()
        self.depths.rename({x.variables[0]: "depth"})
        self.depths.run()


def add_points(self, df=None):
    """
    Add point data

    Parameters
    -------------
    df: pandas dataframe containing the spatiotemporal points to match with.
        The column names must be made up of a subset of "lon", "lat", "year", "month", "day" and "depth"

    """

    self.points_temporal = False

    for x in df.columns:
        if x not in ["lon", "lat", "year", "month", "day", "depth"]:
            raise ValueError(f"{x} is not a valid column name")

    if len([x for x in df.columns if x in ["lon", "lat"]]) < 2:
        raise ValueError("You must provide lon and lat!")

    for x in ["year", "month", "day"]:
        if x in df.columns:
            self.points_temporal = True

    self.points = df

    self.points = self.points.dropna().drop_duplicates().reset_index(drop=True)

    if self.depths is None and self.data is not None:
        if self.points is not None:
            if "depth" in self.points:
                raise ValueError("You cannot match depths without supplying dataset depths")
