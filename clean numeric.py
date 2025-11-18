def clean_numeric(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return value
    try:
        cleaned = value.replace(',', '').replace('%', '').replace('₹', '')
        return float(cleaned)
    except Exception:
        return None
