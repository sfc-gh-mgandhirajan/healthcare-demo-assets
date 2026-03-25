import json
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(SCRIPT_DIR, "icd10cm_inserts.json")) as f:
    data = json.load(f)

semantic_map = {
    "A": "DISEASE", "B": "DISEASE",
    "C": "TUMOR",
    "E": "DISEASE", "F": "DISEASE", "G": "DISEASE", "H": "DISEASE",
    "I": "DISEASE", "J": "DISEASE", "K": "DISEASE", "L": "DISEASE",
    "M": "DISEASE", "N": "DISEASE", "O": "DISEASE", "P": "DISEASE",
    "Q": "DISEASE",
    "R": "SYMPTOM",
    "S": "DISEASE", "T": "DISEASE",
    "V": "OTHER", "W": "OTHER", "X": "OTHER", "Y": "OTHER",
    "Z": "SOCIAL",
}

def get_sg(code):
    ch = code[0].upper()
    if ch == "D":
        num_part = code[1:3]
        if num_part.isdigit() and int(num_part) >= 50:
            return "DISEASE"
        return "TUMOR"
    return semantic_map.get(ch, "OTHER")

out_dir = os.path.join(SCRIPT_DIR, "icd10cm_batches")
os.makedirs(out_dir, exist_ok=True)

for i, batch in enumerate(data["batches"]):
    values = []
    for row_str in batch:
        code = row_str.split("'")[1]
        desc = row_str.split("', '")[1].rstrip("')")
        sg = get_sg(code)
        values.append(f"('ICD10CM-{code}', '{code}', 'ICD10CM', '{desc}', '{sg}')")
    sql = f"""INSERT INTO CONCEPT_DIMENSION (concept_id, code, code_system_id, display, semantic_group)
SELECT col1, col2, col3, col4, col5
FROM VALUES
{',\n'.join(values)};"""
    with open(os.path.join(out_dir, f"batch_{i:03d}.sql"), "w") as f:
        f.write(sql)

print(f"Generated {len(data['batches'])} SQL batch files in {out_dir}")
