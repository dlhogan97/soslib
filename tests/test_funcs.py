from soslib import funcs

def test_radsys():
    assert len(funcs.get_daily_radsys_data('2022-01-13', '2022-01-17'))==47