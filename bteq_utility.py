import os
import re
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# ================= CONFIGURATION =================
excel_input_folder = r"C:\Users\anjaneswar.maddala\Downloads\code_presales-20260223T100611Z-1-001\code_presales\Autosys_extracts"
bteq_folder = r"C:\Users\anjaneswar.maddala\Downloads\code_presales-20260223T100611Z-1-001\code_presales"
project_folder = r"C:\Users\anjaneswar.maddala\Downloads\bteq_project"
# =================================================


def extract_schedule(job_name: str) -> str:
    job_name = job_name.lower()
    if "dly" in job_name:
        return "Daily"
    elif "wkly" in job_name:
        return "Weekly"
    return "Unknown"


def extract_table_from_description(description: str):
    match = re.search(r"for\s+([a-zA-Z0-9_]+)\s+table", description, re.IGNORECASE)
    if match:
        return match.group(1)
    return None


def read_bteq_file(file_path):
    """
    Reads a BTEQ file and extracts:
    - All table names
    - Purpose
    """
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read().lower()

        # Extract purpose
        purpose_match = re.search(r"purpose\s*:?\s*(.*)", content, re.IGNORECASE)
        purpose = (
            purpose_match.group(1).strip() if purpose_match else "Purpose Not Found"
        )

        # Extract possible table names
        tables = re.findall(r"\b[a-zA-Z0-9_]+\b", content)

        return file_path, purpose, set(tables)

    except:
        return None


def build_bteq_index():
    """
    Multithreaded indexing of BTEQ files
    """
    print("Indexing BTEQ files...")

    bteq_files = []

    for root, _, files in os.walk(bteq_folder):
        for file in files:
            if file.lower().endswith(".bteq"):
                bteq_files.append(os.path.join(root, file))

    bteq_index = {}

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(read_bteq_file, file) for file in bteq_files]

        for future in as_completed(futures):
            result = future.result()
            if result:
                file_path, purpose, tables = result
                for table in tables:
                    if table not in bteq_index:
                        bteq_index[table] = (file_path, purpose)

    print(f"Indexed {len(bteq_files)} BTEQ files.")
    return bteq_index


def process_excels(bteq_index):

    final_data = []

    for file in os.listdir(excel_input_folder):
        if file.lower().endswith((".xlsx", ".xls", ".xlsm")):
            file_path = os.path.join(excel_input_folder, file)
            print(f"Processing: {file_path}")

            df = pd.read_excel(file_path)
            df.columns = df.columns.str.strip()

            for _, row in df.iterrows():
                job_name = str(row.get("Job Name", "")).strip()
                description = str(row.get("Description", "")).strip()

                if not job_name or not description:
                    continue

                schedule = extract_schedule(job_name)
                table_name = extract_table_from_description(description)

                if not table_name:
                    continue

                table_name_lower = table_name.lower()

                if table_name_lower in bteq_index:
                    bteq_path, purpose = bteq_index[table_name_lower]
                else:
                    bteq_path = "Not Found"
                    purpose = "BTEQ Not Found"

                final_data.append([job_name, purpose, schedule, table_name, bteq_path])

    final_df = pd.DataFrame(
        final_data,
        columns=["Job Name", "Purpose", "Schedule", "Table Name", "BTEQ File Path"],
    )

    final_df.drop_duplicates(inplace=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_excel = os.path.join(project_folder, f"BTEQ_Report_{timestamp}.xlsx")

    os.makedirs(project_folder, exist_ok=True)

    final_df.to_excel(output_excel, index=False, engine="openpyxl")

    print(f"\n✅ Report Generated Successfully at:\n{output_excel}")


if __name__ == "__main__":
    bteq_index = build_bteq_index()
    process_excels(bteq_index)
