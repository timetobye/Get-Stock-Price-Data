import exchange_calendars as xcals


def is_market_open(**kwargs):
    xnys = xcals.get_calendar("XNYS")

    run_date = kwargs["data_interval_start"].to_date_string()
    open_status = xnys.is_session(run_date)  # True or False
    print(f"is market open : date is {run_date}, open_status : {open_status}")

    return "market_opened" if open_status else "market_closed"
