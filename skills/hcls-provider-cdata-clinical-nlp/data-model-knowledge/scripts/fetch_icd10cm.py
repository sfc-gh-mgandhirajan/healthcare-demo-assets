import urllib.request
import zipfile
import io
import json
import os

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

url = "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Publications/ICD10CM/2026/icd10cm-Code%20Descriptions-2026.zip"
print(f"Downloading ICD-10-CM FY2026 from CDC...")
resp = urllib.request.urlopen(url)
data = resp.read()
print(f"Downloaded {len(data)} bytes")

zf = zipfile.ZipFile(io.BytesIO(data))
print(f"Files in zip: {zf.namelist()}")

order_file = None
for name in zf.namelist():
    if "order" in name.lower() and name.endswith(".txt"):
        order_file = name
        break

if not order_file:
    for name in zf.namelist():
        if name.endswith(".txt"):
            order_file = name
            break

print(f"Using file: {order_file}")
raw = zf.read(order_file).decode("utf-8", errors="replace")
lines = raw.strip().split("\n")
print(f"Total lines: {len(lines)}")

codes = []
for line in lines:
    if len(line) < 77:
        continue
    is_header = line[14:15].strip()
    if is_header == "0":
        continue
    code = line[6:13].strip()
    short_desc = line[16:77].strip()
    long_desc = line[77:].strip() if len(line) > 77 else short_desc
    if code and long_desc:
        codes.append((code, long_desc))

print(f"Billable codes extracted: {len(codes)}")
print(f"Sample: {codes[0]} ... {codes[-1]}")

BATCH = 500
batches = []
for i in range(0, len(codes), BATCH):
    batch = codes[i:i+BATCH]
    rows = []
    for code, desc in batch:
        safe_desc = desc.replace("'", "''")
        safe_code = code.replace("'", "''")
        rows.append(f"('{safe_code}', '{safe_desc}')")
    batches.append(rows)

print(f"Generated {len(batches)} batches of up to {BATCH} rows")

out_path = os.path.join(OUTPUT_DIR, "icd10cm_inserts.json")
with open(out_path, "w") as f:
    json.dump({"total_codes": len(codes), "batch_count": len(batches), "batches": [b for b in batches]}, f)
print(f"Wrote {out_path}")
