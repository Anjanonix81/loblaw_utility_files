import os
import pandas as pd
from datetime import datetime


def extract_fex_details(input_folder, output_folder):
    data = []

    # Validate input folder
    if not os.path.exists(input_folder):
        print("❌ Input folder does not exist")
        return

    # Create output folder if not exists
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    print(f"🔍 Scanning Folder: {input_folder}")

    for root, dirs, files in os.walk(input_folder):
        for file in files:
            if file.lower().endswith((".fex", ".fexp")):
                file_path = os.path.join(root, file)

                try:
                    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                        lines = f.readlines()

                    purpose_lines = []
                    capture = False

                    for line in lines:
                        stripped = line.strip()

                        if stripped.startswith("/*") or stripped.startswith("*"):
                            clean = stripped.replace("/*", "").replace("*/", "")
                            clean = clean.lstrip("*").strip()

                            # Start capturing after Purpose :
                            if "Purpose" in clean and ":" in clean:
                                capture = True
                                purpose_text = clean.split(":", 1)[1].strip()
                                purpose_lines.append(purpose_text)
                                continue

                            # Continue capturing lines
                            if capture:
                                if any(
                                    keyword in clean
                                    for keyword in ["Name", "Created", "Changed"]
                                ):
                                    break
                                if clean != "":
                                    purpose_lines.append(clean)

                        else:
                            if capture:
                                break

                    purpose = " ".join(purpose_lines)

                    data.append(
                        {"File Name": file, "Full Path": file_path, "Purpose": purpose}
                    )

                    print("Processed:", file)

                except Exception as e:
                    print(f"❌ Error reading {file}: {e}")

    if data:
        df = pd.DataFrame(data)

        # Create timestamp file name
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(output_folder, f"fex_summary_{timestamp}.xlsx")

        df.to_excel(output_file, index=False)

        print("\n✅ Extraction complete")
        print("📁 Output File:", output_file)

    else:
        print("⚠ No FEX/FEXP files found")


# ---------------- RUN ----------------
if __name__ == "__main__":
    input_folder = r"C:\Users\anjaneswar.maddala\Downloads\code_presales-20260223T100611Z-1-001\code_presales"

    output_folder = r"C:\Users\anjaneswar.maddala\Downloads\fexp_to_excel_utility"

    extract_fex_details(input_folder, output_folder)
