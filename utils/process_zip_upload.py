# utils/process_zip_upload.py

import os
import zipfile
import tempfile
from utils.csv_to_postgres import load_csv_to_postgres
from utils.sav_xml_to_csv import convert_sav_xml_to_csv
from utils.table_naming import get_safe_table_name

def process_zip_and_upload(zip_path, db_url):
    uploaded_tables = []

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        with tempfile.TemporaryDirectory() as extract_dir:
            zip_ref.extractall(extract_dir)

            files = os.listdir(extract_dir)
            sav_files = [f for f in files if f.endswith('.sav')]
            csv_files = [f for f in files if f.endswith('.csv')]
            xml_files = [f for f in files if f.endswith('.xml')]

            for csv_file in csv_files:
                full_path = os.path.join(extract_dir, csv_file)
                base_name = os.path.splitext(csv_file)[0]
                safe_table_name = get_safe_table_name(base_name, db_url)
                load_csv_to_postgres(full_path, safe_table_name, db_url)
                uploaded_tables.append(safe_table_name)

            for sav_file in sav_files:
                base_name = os.path.splitext(sav_file)[0]
                xml_match = f"{base_name}.xml"
                if xml_match in xml_files:
                    csv_out_path = os.path.join(extract_dir, f"{base_name}_converted.csv")
                    sav_path = os.path.join(extract_dir, sav_file)
                    xml_path = os.path.join(extract_dir, xml_match)

                    try:
                        convert_sav_xml_to_csv(sav_path, xml_path, csv_out_path)
                        safe_table_name = get_safe_table_name(base_name, db_url)
                        load_csv_to_postgres(csv_out_path, safe_table_name, db_url)
                        uploaded_tables.append(safe_table_name)
                    except Exception as e:
                        print(f"⚠️ Failed to convert and upload {sav_file}: {e}")
                else:
                    print(f"⚠️ Skipping {sav_file}: No matching XML file found.")

    return uploaded_tables
