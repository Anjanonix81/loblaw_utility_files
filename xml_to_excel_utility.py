import os
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime


def parse_single_xml(xml_path):
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()

        namespace = {"ns": root.tag.split("}")[0].strip("{")}
        data_rows = []

        # Job level details
        job_name = root.find("ns:job_name", namespace)
        source_tdpid = root.find("ns:source_tdpid", namespace)
        target_tdpid = root.find("ns:target_tdpid", namespace)
        force_utility = root.find("ns:force_utility", namespace)

        job_name = job_name.text if job_name is not None else None
        source_tdpid = source_tdpid.text if source_tdpid is not None else None
        target_tdpid = target_tdpid.text if target_tdpid is not None else None
        force_utility = force_utility.text if force_utility is not None else None

        # Loop databases
        for database in root.findall("ns:database", namespace):
            db_name = database.find("ns:name", namespace)
            db_name = db_name.text if db_name is not None else None

            for table in database.findall("ns:table", namespace):
                table_name = table.find("ns:name", namespace)
                target_db = table.find("ns:target_database/ns:name", namespace)
                target_name = table.find("ns:target_name", namespace)

                data_rows.append(
                    {
                        "xml_file": os.path.basename(xml_path),
                        "job_name": job_name,
                        "source_tdpid": source_tdpid,
                        "target_tdpid": target_tdpid,
                        "utility": force_utility,
                        "source_database": db_name,
                        "table_name": table_name.text
                        if table_name is not None
                        else None,
                        "target_database": target_db.text
                        if target_db is not None
                        else None,
                        "target_table": target_name.text
                        if target_name is not None
                        else None,
                    }
                )

        return data_rows

    except Exception as e:
        print(f"❌ Error processing {xml_path}: {e}")
        return []


def combine_all_xml(folder_path, output_directory):
    all_data = []

    # Validate folder
    if not os.path.exists(folder_path):
        print("❌ Input folder does not exist.")
        return

    # Create output directory if not exists
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    # Read XML files (including subfolders)
    for root_dir, _, files in os.walk(folder_path):
        for file in files:
            if file.lower().endswith(".xml"):
                full_path = os.path.join(root_dir, file)
                print(f"Processing: {full_path}")
                rows = parse_single_xml(full_path)
                all_data.extend(rows)

    if all_data:
        df = pd.DataFrame(all_data)

        # Generate timestamp file name
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(output_directory, f"xml_output_{timestamp}.xlsx")

        df.to_excel(output_file, index=False)

        print("\n✅ Excel file created successfully!")
        print(f"📁 Location: {output_file}")
    else:
        print("⚠️ No XML data found.")


# ---------------- RUN ----------------
if __name__ == "__main__":
    folder_path = r"C:\Users\anjaneswar.maddala\Downloads\code_presales-20260223T100611Z-1-001\code_presales\ETL_code_extracts"

    output_directory = r"C:\Users\anjaneswar.maddala\Downloads\xml_to_excel_utility"

    combine_all_xml(folder_path, output_directory)
