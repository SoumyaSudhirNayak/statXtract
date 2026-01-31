import pandas as pd

def convert_excel_to_csv(excel_path, csv_path):
    try:
        # Load the Excel file with no header to find the first non-empty row
        raw_df = pd.read_excel(excel_path, header=None, dtype=str)

        # Find the first row with at least 2 non-NaN values to use as header
        header_row = None
        for i, row in raw_df.iterrows():
            if row.notna().sum() >= 2:
                header_row = i
                break

        if header_row is None:
            raise ValueError("❌ No valid header row found")

        # Extract header from detected row
        headers = raw_df.iloc[header_row].fillna(f"unnamed")
        data = raw_df.iloc[header_row + 1:].copy()
        data.columns = headers

        # Convert all columns to string to preserve original formatting
        data = data.astype(str)

        # Clean empty rows/columns (optional)
        data.dropna(how='all', inplace=True)
        data.dropna(axis=1, how='all', inplace=True)

        # Save to CSV
        data.to_csv(csv_path, index=False)
        print(f"✅ Excel converted successfully using row {header_row} as headers → {csv_path}")
        return csv_path

    except Exception as e:
        raise Exception(f"❌ Excel to CSV failed: {e}")
