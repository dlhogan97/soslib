from soslib import funcs
import datetime as dt
import warnings


def test_radsys():
    assert len(funcs.get_daily_radsys_data('2022-01-13', '2022-01-17'))==47

def test_awdb():
    funcs.get_awdb_data('site_ids', element="WTEQ", sdate=dt.datetime(1899,10,1), edate=dt.datetime.now(), orient="records", server="https://api.snowdata.info/", sesh=None)


def test_sail():
    username = 'dlhogan@uw.edu'
    token = '7f1c805e6ae94c21'
    met ='gucmetM1.b1'
    start = '2021-11-15'
    end = '2021-11-17' 
    warnings.filterwarnings('ignore')
    assert len(funcs.get_sail_data(username, token, met,
                                start, end).time)==4320