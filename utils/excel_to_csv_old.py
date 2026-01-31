# utils/excel_to_csv.py
import pandas as pd

def convert_excel_to_csv(excel_path, csv_path):
    try:
        df = pd.read_excel(excel_path, header=0)

        # Fallback: if most columns are unnamed, try second row as header
        unnamed_cols = [col for col in df.columns if "Unnamed" in str(col)]
        if len(unnamed_cols) > len(df.columns) / 2:
            df = pd.read_excel(excel_path, header=1)  # Try second row as header

        df.to_csv(csv_path, index=False)
        print(f"✅ Converted Excel to CSV: {csv_path}")
        return csv_path
    except Exception as e:
        raise Exception(f"❌ Failed to convert Excel to CSV: {e}")
