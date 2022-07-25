from soslib import funcs
import datetime as dt

def test_radsys():
    assert len(funcs.get_daily_radsys_data('2022-01-13', '2022-01-17'))==47

def test_awdb():
    funcs.get_awdb_data('site_ids', element="WTEQ", sdate=dt.datetime(1899,10,1), edate=dt.datetime.now(), orient="records", server="https://api.snowdata.info/", sesh=None)
