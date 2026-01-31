import pyreadstat

def convert_sav_to_csv(sav_path, csv_path):
    # Apply value labels directly
    df, meta = pyreadstat.read_sav(sav_path, apply_value_formats=True)
    df.to_csv(csv_path, index=False)
    return csv_path
