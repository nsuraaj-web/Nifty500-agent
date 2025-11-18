import datetime

def unix_to_iso_utc(unix_ts):
    """
    Convert a UNIX timestamp (seconds since epoch) to an ISO 8601 
    datetime string with UTC timezone.
    Return None if input is None or invalid.
    """
    if unix_ts is None:
        return None
    try:
        dt = datetime.datetime.fromtimestamp(unix_ts, tz=datetime.timezone.utc)
        return dt.isoformat()
    except Exception:
        return None
