#!/usr/bin/env python3
import urllib.request
import zipfile
import io
import csv
import json
import os
import ssl
import time
import sys

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "terminology_seed")
os.makedirs(OUTPUT_DIR, exist_ok=True)

BATCH_SIZE = 500

def escape_sql(s):
    if s is None:
        return "NULL"
    return "'" + s.replace("'", "''").replace("\\", "\\\\") + "'"

def write_sql_batches(code_system_id, rows, semantic_group_fn=None):
    if not rows:
        print(f"  WARNING: No rows for {code_system_id}")
        return []
    
    files = []
    batch_num = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i+BATCH_SIZE]
        batch_num += 1
        fname = f"{code_system_id.lower()}_batch_{batch_num:04d}.sql"
        fpath = os.path.join(OUTPUT_DIR, fname)
        
        values = []
        for r in batch:
            code = r["code"]
            display = r["display"]
            sg = semantic_group_fn(r) if semantic_group_fn else r.get("semantic_group", "OTHER")
            concept_id = f"{code_system_id}_{code}"
            values.append(
                f"({escape_sql(concept_id)}, {escape_sql(code)}, {escape_sql(code_system_id)}, {escape_sql(display)}, {escape_sql(sg)})"
            )
        
        sql = f"INSERT INTO UNSTRUCTURED_HEALTHDATA.DATA_MODEL_KNOWLEDGE.CONCEPT_DIMENSION (concept_id, code, code_system_id, display, semantic_group)\nSELECT * FROM VALUES\n"
        sql += ",\n".join(values) + ";"
        
        with open(fpath, "w") as f:
            f.write(sql)
        files.append(fpath)
    
    print(f"  Wrote {len(files)} batch files ({len(rows)} rows) for {code_system_id}")
    return files

def fetch_url(url, timeout=120):
    ctx = ssl.create_default_context()
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (clinical-nlp-seed)"})
    return urllib.request.urlopen(req, timeout=timeout, context=ctx)

# ============================================================
# 1. ICD-10-CM — CDC FTP fixed-width order file
# ============================================================
def load_icd10cm():
    print("\n=== ICD-10-CM (CDC FTP) ===")
    url = "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Publications/ICD10CM/2026/icd10cm-Code%20Descriptions-2026.zip"
    print(f"  Fetching {url}")
    resp = fetch_url(url)
    zdata = io.BytesIO(resp.read())
    
    rows = []
    with zipfile.ZipFile(zdata) as zf:
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
        
        if not order_file:
            print(f"  ERROR: No .txt file found in zip. Files: {zf.namelist()}")
            return []
        
        print(f"  Parsing {order_file}")
        with zf.open(order_file) as f:
            for line in f:
                line = line.decode("utf-8", errors="replace").rstrip("\n").rstrip("\r")
                if len(line) < 20:
                    continue
                # CDC fixed-width: cols 1-5=order, 6=blank, 7-13=code, 14=blank, 15=billable(0/1), 16=blank, 17-77=short_desc, 78-end=long_desc
                code = line[6:13].strip()
                is_billable = line[14:15].strip()
                long_desc = line[77:].strip() if len(line) > 77 else line[16:].strip()
                
                if not code or not long_desc:
                    continue
                if is_billable == "0":
                    continue
                
                code_formatted = code
                if len(code) > 3 and "." not in code:
                    code_formatted = code[:3] + "." + code[3:]
                
                rows.append({"code": code_formatted, "display": long_desc})
    
    def sg(r):
        c = r["code"]
        if c.startswith("Z"):
            if any(c.startswith(p) for p in ["Z55","Z56","Z57","Z58","Z59","Z60","Z62","Z63","Z64","Z65"]):
                return "SOCIAL"
            if any(c.startswith(p) for p in ["Z80","Z81","Z82","Z83","Z84"]):
                return "FAMILY"
            return "OTHER"
        if c[0] in ("A","B"):
            return "DISEASE"
        if c[0] == "C" or c.startswith("D0") or c.startswith("D1") or c.startswith("D2") or c.startswith("D3") or (c.startswith("D4") and c < "D49.9"):
            return "TUMOR"
        if c[0] in ("D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R"):
            first_two = c[:3].replace(".","")
            if c[0] == "R":
                return "SYMPTOM"
            return "DISEASE"
        if c[0] in ("S","T"):
            return "DISEASE"
        if c[0] in ("V","W","X","Y"):
            return "OTHER"
        return "OTHER"
    
    return write_sql_batches("ICD10CM", rows, sg)

# ============================================================
# 2. ICD-10-PCS — CMS.gov 
# ============================================================
def load_icd10pcs():
    print("\n=== ICD-10-PCS (CMS.gov) ===")
    url = "https://www.cms.gov/files/zip/2025-icd-10-pcs-order-file-long-and-abbreviated-titles-updated-12202024.zip"
    print(f"  Fetching {url}")
    try:
        resp = fetch_url(url)
    except Exception as e:
        print(f"  Error fetching: {e}")
        url2 = "https://www.cms.gov/files/zip/2025-icd-10-pcs-code-tables-and-index.zip"
        print(f"  Trying alternate: {url2}")
        try:
            resp = fetch_url(url2)
        except Exception as e2:
            print(f"  Error: {e2}. Skipping ICD-10-PCS.")
            return []
    
    zdata = io.BytesIO(resp.read())
    rows = []
    with zipfile.ZipFile(zdata) as zf:
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
        
        if not order_file:
            print(f"  No order txt found. Files: {zf.namelist()}")
            return []
        
        print(f"  Parsing {order_file}")
        with zf.open(order_file) as f:
            for line in f:
                line = line.decode("utf-8", errors="replace").rstrip("\n").rstrip("\r")
                if len(line) < 20:
                    continue
                code = line[6:13].strip()
                is_billable = line[14:15].strip()
                long_desc = line[77:].strip() if len(line) > 77 else line[16:].strip()
                
                if not code or not long_desc:
                    continue
                if is_billable == "0":
                    continue
                
                rows.append({"code": code, "display": long_desc, "semantic_group": "PROCEDURE"})
    
    return write_sql_batches("ICD10PCS", rows)

# ============================================================
# 3. LOINC — loinc.org (public CSV from GitHub mirror)
# ============================================================
def load_loinc():
    print("\n=== LOINC ===")
    url = "https://raw.githubusercontent.com/AcademicMedicalCenter/ICD-10-LOINC-data/main/loinc_top2000_common_lab_results.csv"
    print(f"  Trying LOINC top 2000 from GitHub...")
    try:
        resp = fetch_url(url)
        text = resp.read().decode("utf-8", errors="replace")
        reader = csv.DictReader(io.StringIO(text))
        rows = []
        for r in reader:
            code = r.get("LOINC_NUM") or r.get("loinc_num") or r.get("Loinc Num") or ""
            display = r.get("LONG_COMMON_NAME") or r.get("long_common_name") or r.get("Long Common Name") or ""
            if code and display:
                sg = "LAB"
                rows.append({"code": code.strip(), "display": display.strip(), "semantic_group": sg})
        if rows:
            return write_sql_batches("LOINC", rows)
    except Exception as e:
        print(f"  GitHub mirror failed: {e}")
    
    print("  Trying LOINC common lab codes from alternate sources...")
    loinc_vitals = [
        {"code": "8310-5", "display": "Body temperature", "semantic_group": "LAB"},
        {"code": "8867-4", "display": "Heart rate", "semantic_group": "LAB"},
        {"code": "9279-1", "display": "Respiratory rate", "semantic_group": "LAB"},
        {"code": "8480-6", "display": "Systolic blood pressure", "semantic_group": "LAB"},
        {"code": "8462-4", "display": "Diastolic blood pressure", "semantic_group": "LAB"},
        {"code": "2708-6", "display": "Oxygen saturation in Arterial blood", "semantic_group": "LAB"},
        {"code": "29463-7", "display": "Body weight", "semantic_group": "LAB"},
        {"code": "8302-2", "display": "Body height", "semantic_group": "LAB"},
        {"code": "39156-5", "display": "Body mass index (BMI)", "semantic_group": "SCORE"},
        {"code": "85354-9", "display": "Blood pressure panel with all children optional", "semantic_group": "LAB"},
        {"code": "2093-3", "display": "Cholesterol [Mass/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "2085-9", "display": "Cholesterol in HDL [Mass/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "2089-1", "display": "Cholesterol in LDL [Mass/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "2571-8", "display": "Triglyceride [Mass/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "2345-7", "display": "Glucose [Mass/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "4548-4", "display": "Hemoglobin A1c/Hemoglobin.total in Blood", "semantic_group": "LAB"},
        {"code": "2160-0", "display": "Creatinine [Mass/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "3094-0", "display": "Urea nitrogen [Mass/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "2951-2", "display": "Sodium [Moles/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "2823-3", "display": "Potassium [Moles/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "2075-0", "display": "Chloride [Moles/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "1963-8", "display": "Bicarbonate [Moles/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "17861-6", "display": "Calcium [Mass/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "1751-7", "display": "Albumin [Mass/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "2532-0", "display": "Lactate dehydrogenase [Enzymatic activity/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "1742-6", "display": "Alanine aminotransferase [Enzymatic activity/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "1920-8", "display": "Aspartate aminotransferase [Enzymatic activity/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "1975-2", "display": "Bilirubin.total [Mass/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "6768-6", "display": "Alkaline phosphatase [Enzymatic activity/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "718-7", "display": "Hemoglobin [Mass/volume] in Blood", "semantic_group": "LAB"},
        {"code": "4544-3", "display": "Hematocrit [Volume Fraction] of Blood", "semantic_group": "LAB"},
        {"code": "26464-8", "display": "Leukocytes [#/volume] in Blood", "semantic_group": "LAB"},
        {"code": "26515-7", "display": "Platelets [#/volume] in Blood", "semantic_group": "LAB"},
        {"code": "6690-2", "display": "Leukocytes [#/volume] in Blood by Automated count", "semantic_group": "LAB"},
        {"code": "789-8", "display": "Erythrocytes [#/volume] in Blood by Automated count", "semantic_group": "LAB"},
        {"code": "787-2", "display": "MCV [Entitic volume] by Automated count", "semantic_group": "LAB"},
        {"code": "785-6", "display": "MCH [Entitic mass] by Automated count", "semantic_group": "LAB"},
        {"code": "786-4", "display": "MCHC [Mass/volume] by Automated count", "semantic_group": "LAB"},
        {"code": "788-0", "display": "Erythrocyte distribution width [Ratio] by Automated count", "semantic_group": "LAB"},
        {"code": "770-8", "display": "Neutrophils/100 leukocytes in Blood by Automated count", "semantic_group": "LAB"},
        {"code": "736-9", "display": "Lymphocytes/100 leukocytes in Blood by Automated count", "semantic_group": "LAB"},
        {"code": "5905-5", "display": "Monocytes/100 leukocytes in Blood by Automated count", "semantic_group": "LAB"},
        {"code": "713-8", "display": "Eosinophils/100 leukocytes in Blood by Automated count", "semantic_group": "LAB"},
        {"code": "706-2", "display": "Basophils/100 leukocytes in Blood by Automated count", "semantic_group": "LAB"},
        {"code": "5902-2", "display": "Prothrombin time (PT)", "semantic_group": "LAB"},
        {"code": "3173-2", "display": "aPTT in Blood by Coagulation assay", "semantic_group": "LAB"},
        {"code": "48065-7", "display": "Fibrinogen [Mass/volume] in Platelet poor plasma by Coagulation assay", "semantic_group": "LAB"},
        {"code": "6598-7", "display": "Troponin T.cardiac [Mass/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "49563-0", "display": "Troponin I.cardiac [Mass/volume] in Serum or Plasma by High sensitivity method", "semantic_group": "LAB"},
        {"code": "33762-6", "display": "Natriuretic peptide.B prohormone N-Terminal [Mass/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "30934-4", "display": "Natriuretic peptide B [Mass/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "2276-4", "display": "Ferritin [Mass/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "2498-4", "display": "Iron [Mass/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "2500-7", "display": "Iron binding capacity [Mass/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "14927-8", "display": "Triglyceride [Moles/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "1988-5", "display": "C reactive protein [Mass/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "4537-7", "display": "Erythrocyte sedimentation rate", "semantic_group": "LAB"},
        {"code": "33959-8", "display": "Procalcitonin [Mass/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "14749-6", "display": "Glucose [Mass/volume] in Serum or Plasma --fasting", "semantic_group": "LAB"},
        {"code": "2339-0", "display": "Glucose [Mass/volume] in Blood", "semantic_group": "LAB"},
        {"code": "3084-1", "display": "Urate [Mass/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "2157-6", "display": "Creatine kinase [Enzymatic activity/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "2965-2", "display": "Specific gravity of Urine", "semantic_group": "LAB"},
        {"code": "5811-5", "display": "Specific gravity of Urine by Test strip", "semantic_group": "LAB"},
        {"code": "2514-8", "display": "Ketones [Presence] in Urine by Test strip", "semantic_group": "LAB"},
        {"code": "5770-3", "display": "Bilirubin.total [Presence] in Urine by Test strip", "semantic_group": "LAB"},
        {"code": "5794-3", "display": "Glucose [Presence] in Urine by Test strip", "semantic_group": "LAB"},
        {"code": "2887-8", "display": "Protein [Presence] in Urine by Test strip", "semantic_group": "LAB"},
        {"code": "5778-6", "display": "Color of Urine", "semantic_group": "LAB"},
        {"code": "5767-9", "display": "Appearance of Urine", "semantic_group": "LAB"},
        {"code": "2756-5", "display": "pH of Urine", "semantic_group": "LAB"},
        {"code": "20570-8", "display": "Hematocrit [Volume Fraction] of Urine", "semantic_group": "LAB"},
        {"code": "5799-2", "display": "Leukocyte esterase [Presence] in Urine by Test strip", "semantic_group": "LAB"},
        {"code": "5802-4", "display": "Nitrite [Presence] in Urine by Test strip", "semantic_group": "LAB"},
        {"code": "3016-3", "display": "Thyrotropin [Units/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "3051-0", "display": "Triiodothyronine (T3) free [Mass/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "3024-7", "display": "Thyroxine (T4) free [Mass/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "2132-9", "display": "Follitropin [Units/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "10501-5", "display": "Lutropin [Units/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "2986-8", "display": "Testosterone [Mass/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "2243-4", "display": "Estradiol (E2) [Mass/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "2236-8", "display": "Cortisol [Mass/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "49765-1", "display": "Calcium [Mass/volume] in Blood", "semantic_group": "LAB"},
        {"code": "2777-1", "display": "Phosphate [Mass/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "19123-9", "display": "Magnesium [Mass/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "2947-0", "display": "Sodium [Moles/volume] in Blood", "semantic_group": "LAB"},
        {"code": "6299-2", "display": "Urea nitrogen [Mass/volume] in Blood", "semantic_group": "LAB"},
        {"code": "33914-3", "display": "Glomerular filtration rate/1.73 sq M.predicted [Volume Rate/Area]", "semantic_group": "LAB"},
        {"code": "48642-3", "display": "Glomerular filtration rate/1.73 sq M.predicted among non-Blacks", "semantic_group": "LAB"},
        {"code": "48643-1", "display": "Glomerular filtration rate/1.73 sq M.predicted among Blacks", "semantic_group": "LAB"},
        {"code": "14682-9", "display": "Creatinine [Moles/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "2164-2", "display": "Creatinine renal clearance predicted by Cockcroft-Gault formula", "semantic_group": "LAB"},
        {"code": "5803-2", "display": "pH of Body fluid", "semantic_group": "LAB"},
        {"code": "2019-8", "display": "Carbon dioxide [Partial pressure] in Arterial blood", "semantic_group": "LAB"},
        {"code": "2703-7", "display": "Oxygen [Partial pressure] in Arterial blood", "semantic_group": "LAB"},
        {"code": "1960-4", "display": "Bicarbonate [Moles/volume] in Arterial blood", "semantic_group": "LAB"},
        {"code": "11555-0", "display": "Base excess in Blood by calculation", "semantic_group": "LAB"},
        {"code": "2744-1", "display": "pH of Arterial blood", "semantic_group": "LAB"},
        {"code": "59408-5", "display": "Oxygen saturation in Arterial blood by Pulse oximetry", "semantic_group": "LAB"},
        {"code": "2862-1", "display": "Albumin [Mass/volume] in Serum or Plasma by Bromocresol purple (BCP) dye binding method", "semantic_group": "LAB"},
        {"code": "10466-1", "display": "Anion gap 3 in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "33037-3", "display": "Anion gap in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "2028-9", "display": "Carbon dioxide, total [Moles/volume] in Serum or Plasma", "semantic_group": "LAB"},
        {"code": "72166-2", "display": "Tobacco smoking status", "semantic_group": "SOCIAL"},
        {"code": "11331-6", "display": "History of Tobacco use", "semantic_group": "SOCIAL"},
        {"code": "11332-4", "display": "History of Alcohol use", "semantic_group": "SOCIAL"},
        {"code": "74013-4", "display": "Alcoholic drinks per day", "semantic_group": "SOCIAL"},
        {"code": "93025-5", "display": "Protocol for Responding to and Assessing Patients Assets Risks and Experiences [PRAPARE]", "semantic_group": "SOCIAL"},
        {"code": "71802-3", "display": "Housing status", "semantic_group": "SOCIAL"},
        {"code": "63586-2", "display": "What is your race", "semantic_group": "SOCIAL"},
        {"code": "46098-0", "display": "Sex", "semantic_group": "SOCIAL"},
        {"code": "21112-8", "display": "Birth date", "semantic_group": "OTHER"},
        {"code": "72514-3", "display": "Pain severity - 0-10 verbal numeric rating", "semantic_group": "SCORE"},
        {"code": "38208-5", "display": "Pain mechanism", "semantic_group": "SCORE"},
        {"code": "75443-2", "display": "Glasgow coma scale total", "semantic_group": "SCORE"},
        {"code": "9269-2", "display": "Glasgow coma score", "semantic_group": "SCORE"},
        {"code": "72107-6", "display": "SOFA score", "semantic_group": "SCORE"},
        {"code": "75889-6", "display": "APACHE II score", "semantic_group": "SCORE"},
        {"code": "77576-7", "display": "Morse Fall Scale total score", "semantic_group": "SCORE"},
        {"code": "38221-8", "display": "Braden Scale total score", "semantic_group": "SCORE"},
        {"code": "72106-8", "display": "NEWS score", "semantic_group": "SCORE"},
        {"code": "71960-9", "display": "qSOFA score", "semantic_group": "SCORE"},
        {"code": "LL2421-8", "display": "Karnofsky Performance Status", "semantic_group": "SCORE"},
        {"code": "89555-7", "display": "PHQ-2 total score", "semantic_group": "SCORE"},
        {"code": "44261-6", "display": "PHQ-9 total score", "semantic_group": "SCORE"},
        {"code": "70274-6", "display": "GAD-7 total score", "semantic_group": "SCORE"},
    ]
    print(f"  Using curated LOINC set: {len(loinc_vitals)} codes (vitals, CBC, CMP, coag, cardiac, thyroid, UA, ABG, social, scores)")
    return write_sql_batches("LOINC", loinc_vitals)

# ============================================================
# 4. RxNorm — NLM (try RXNCONSO from prescribable subset)
# ============================================================
def load_rxnorm():
    print("\n=== RxNorm ===")
    url = "https://raw.githubusercontent.com/hms-dbmi/fhir-ig-data/main/rxnorm/rxnorm-top-drugs.csv"
    try:
        print(f"  Trying GitHub FHIR mirror...")
        resp = fetch_url(url, timeout=30)
        text = resp.read().decode("utf-8", errors="replace")
        if len(text) > 100:
            reader = csv.DictReader(io.StringIO(text))
            rows = []
            for r in reader:
                code = r.get("RXCUI") or r.get("rxcui") or ""
                display = r.get("Name") or r.get("name") or r.get("STR") or ""
                if code and display:
                    rows.append({"code": code.strip(), "display": display.strip(), "semantic_group": "MEDICATION"})
            if rows:
                return write_sql_batches("RXNORM", rows)
    except Exception as e:
        print(f"  Mirror failed: {e}")

    print("  Using curated RxNorm top prescribed medications...")
    rxnorm_drugs = []
    top_drugs = [
        ("1049221", "Acetaminophen 325 MG Oral Tablet"),
        ("197361", "Amlodipine 5 MG Oral Tablet"),
        ("1049223", "Acetaminophen 500 MG Oral Tablet"),
        ("310965", "Amoxicillin 500 MG Oral Capsule"),
        ("197381", "Atenolol 25 MG Oral Tablet"),
        ("197382", "Atenolol 50 MG Oral Tablet"),
        ("308182", "Azithromycin 250 MG Oral Tablet"),
        ("205532", "Carvedilol 12.5 MG Oral Tablet"),
        ("197512", "Cephalexin 500 MG Oral Capsule"),
        ("309362", "Ciprofloxacin 500 MG Oral Tablet"),
        ("197591", "Clonidine 0.1 MG Oral Tablet"),
        ("309845", "Diazepam 5 MG Oral Tablet"),
        ("310798", "Diltiazem Hydrochloride 120 MG Oral Tablet"),
        ("197770", "Doxycycline 100 MG Oral Capsule"),
        ("310429", "Furosemide 20 MG Oral Tablet"),
        ("310430", "Furosemide 40 MG Oral Tablet"),
        ("310437", "Gabapentin 300 MG Oral Capsule"),
        ("197884", "Hydrochlorothiazide 25 MG Oral Tablet"),
        ("856904", "Hydrocodone Bitartrate 5 MG / Acetaminophen 325 MG Oral Tablet"),
        ("311368", "Ibuprofen 200 MG Oral Tablet"),
        ("311369", "Ibuprofen 400 MG Oral Tablet"),
        ("311370", "Ibuprofen 600 MG Oral Tablet"),
        ("197953", "Isosorbide Mononitrate 30 MG Oral Tablet"),
        ("311995", "Levothyroxine Sodium 0.05 MG Oral Tablet"),
        ("311996", "Levothyroxine Sodium 0.075 MG Oral Tablet"),
        ("311998", "Levothyroxine Sodium 0.1 MG Oral Tablet"),
        ("197987", "Lisinopril 10 MG Oral Tablet"),
        ("197988", "Lisinopril 20 MG Oral Tablet"),
        ("314076", "Lisinopril 5 MG Oral Tablet"),
        ("312615", "Lorazepam 0.5 MG Oral Tablet"),
        ("312617", "Lorazepam 1 MG Oral Tablet"),
        ("861007", "Losartan Potassium 50 MG Oral Tablet"),
        ("861009", "Losartan Potassium 100 MG Oral Tablet"),
        ("860975", "Metformin hydrochloride 500 MG Oral Tablet"),
        ("860981", "Metformin hydrochloride 850 MG Oral Tablet"),
        ("860995", "Metformin hydrochloride 1000 MG Oral Tablet"),
        ("866924", "Metoprolol Succinate 25 MG Extended Release Oral Tablet"),
        ("866932", "Metoprolol Succinate 50 MG Extended Release Oral Tablet"),
        ("198006", "Metoprolol Tartrate 25 MG Oral Tablet"),
        ("198007", "Metoprolol Tartrate 50 MG Oral Tablet"),
        ("312289", "Metronidazole 500 MG Oral Tablet"),
        ("198211", "Montelukast 10 MG Oral Tablet"),
        ("198240", "Naproxen 500 MG Oral Tablet"),
        ("312961", "Omeprazole 20 MG Delayed Release Oral Capsule"),
        ("198283", "Ondansetron 4 MG Oral Tablet"),
        ("198284", "Ondansetron 8 MG Oral Tablet"),
        ("856903", "Oxycodone Hydrochloride 5 MG Oral Tablet"),
        ("313585", "Pantoprazole 40 MG Delayed Release Oral Tablet"),
        ("198299", "Potassium Chloride 10 MEQ Extended Release Oral Tablet"),
        ("198300", "Potassium Chloride 20 MEQ Extended Release Oral Tablet"),
        ("198211", "Prednisone 10 MG Oral Tablet"),
        ("312617", "Prednisone 20 MG Oral Tablet"),
        ("198240", "Prednisone 5 MG Oral Tablet"),
        ("311700", "Pregabalin 75 MG Oral Capsule"),
        ("998671", "Rosuvastatin calcium 10 MG Oral Tablet"),
        ("998675", "Rosuvastatin calcium 20 MG Oral Tablet"),
        ("200031", "Sertraline 50 MG Oral Tablet"),
        ("200032", "Sertraline 100 MG Oral Tablet"),
        ("314077", "Spironolactone 25 MG Oral Tablet"),
        ("198353", "Tamsulosin 0.4 MG Oral Capsule"),
        ("198369", "Trazodone hydrochloride 50 MG Oral Tablet"),
        ("198370", "Trazodone hydrochloride 100 MG Oral Tablet"),
        ("313406", "Tramadol Hydrochloride 50 MG Oral Tablet"),
        ("855319", "Warfarin Sodium 5 MG Oral Tablet"),
        ("855333", "Warfarin Sodium 2 MG Oral Tablet"),
        ("855343", "Warfarin Sodium 1 MG Oral Tablet"),
        ("259111", "Insulin Glargine 100 UNT/ML Injectable Solution"),
        ("847207", "Insulin Lispro 100 UNT/ML Injectable Solution"),
        ("1361615", "Insulin Aspart 100 UNT/ML Injectable Solution"),
        ("1660014", "Semaglutide 0.25 MG Pen Injector"),
        ("1545149", "Dulaglutide 0.75 MG/0.5 ML Auto-Injector"),
        ("1551291", "Empagliflozin 10 MG Oral Tablet"),
        ("1551295", "Empagliflozin 25 MG Oral Tablet"),
        ("1373463", "Dapagliflozin 10 MG Oral Tablet"),
        ("1992427", "Sacubitril 24.3 MG / Valsartan 25.7 MG Oral Tablet"),
        ("1992435", "Sacubitril 48.6 MG / Valsartan 51.4 MG Oral Tablet"),
        ("2002610", "Eliquis 5 MG Oral Tablet"),
        ("1364430", "Apixaban 2.5 MG Oral Tablet"),
        ("1364435", "Apixaban 5 MG Oral Tablet"),
        ("1037045", "Rivaroxaban 20 MG Oral Tablet"),
        ("1037042", "Rivaroxaban 10 MG Oral Tablet"),
        ("197361", "Atorvastatin 10 MG Oral Tablet"),
        ("259255", "Atorvastatin 20 MG Oral Tablet"),
        ("259256", "Atorvastatin 40 MG Oral Tablet"),
        ("262095", "Atorvastatin 80 MG Oral Tablet"),
        ("200345", "Simvastatin 20 MG Oral Tablet"),
        ("200346", "Simvastatin 40 MG Oral Tablet"),
        ("311700", "Pravastatin Sodium 40 MG Oral Tablet"),
        ("197454", "Clopidogrel 75 MG Oral Tablet"),
        ("318272", "Aspirin 81 MG Delayed Release Oral Tablet"),
        ("243670", "Aspirin 325 MG Oral Tablet"),
        ("283540", "Escitalopram 10 MG Oral Tablet"),
        ("283541", "Escitalopram 20 MG Oral Tablet"),
        ("312940", "Fluoxetine 20 MG Oral Capsule"),
        ("311984", "Paroxetine 20 MG Oral Tablet"),
        ("310385", "Duloxetine 30 MG Delayed Release Oral Capsule"),
        ("310386", "Duloxetine 60 MG Delayed Release Oral Capsule"),
        ("310437", "Venlafaxine 75 MG Oral Tablet"),
        ("313988", "Alprazolam 0.25 MG Oral Tablet"),
        ("313989", "Alprazolam 0.5 MG Oral Tablet"),
        ("198058", "Zolpidem Tartrate 5 MG Oral Tablet"),
        ("198059", "Zolpidem Tartrate 10 MG Oral Tablet"),
        ("1946825", "Tirzepatide 2.5 MG Auto-Injector"),
        ("2468232", "Semaglutide 7 MG Oral Tablet"),
        ("2468235", "Semaglutide 14 MG Oral Tablet"),
    ]
    for rxcui, name in top_drugs:
        rxnorm_drugs.append({"code": rxcui, "display": name, "semantic_group": "MEDICATION"})
    
    ingredients = [
        ("161", "Acetaminophen"), ("519", "Albuterol"), ("596", "Alendronate"),
        ("1191", "Aspirin"), ("1202", "Atenolol"), ("1232", "Amoxicillin"),
        ("1297", "Azithromycin"), ("2551", "Carvedilol"), ("2670", "Cephalexin"),
        ("2799", "Ciprofloxacin"), ("2812", "Clonidine"), ("3247", "Cyclobenzaprine"),
        ("3355", "Dexamethasone"), ("3393", "Diazepam"), ("3498", "Diltiazem"),
        ("3616", "Doxycycline"), ("4278", "Fluoxetine"), ("4337", "Furosemide"),
        ("4407", "Gabapentin"), ("4603", "Hydrochlorothiazide"), ("4636", "Hydrocodone"),
        ("5640", "Ibuprofen"), ("6387", "Levothyroxine"), ("6468", "Lisinopril"),
        ("6470", "Lorazepam"), ("6918", "Metformin"), ("6918", "Metoprolol"),
        ("6960", "Metronidazole"), ("7052", "Montelukast"), ("7212", "Naproxen"),
        ("7646", "Omeprazole"), ("7676", "Ondansetron"), ("7804", "Oxycodone"),
        ("7975", "Pantoprazole"), ("8356", "Potassium Chloride"), ("8640", "Prednisone"),
        ("8691", "Pregabalin"), ("9068", "Rosuvastatin"), ("9524", "Sertraline"),
        ("9822", "Spironolactone"), ("9979", "Tamsulosin"), ("10202", "Tramadol"),
        ("10324", "Trazodone"), ("10737", "Warfarin"), ("274783", "Insulin Glargine"),
        ("86009", "Insulin Lispro"), ("352385", "Insulin Aspart"),
        ("1991302", "Semaglutide"), ("1551291", "Empagliflozin"),
        ("1373463", "Dapagliflozin"), ("1364430", "Apixaban"),
        ("1037045", "Rivaroxaban"), ("83367", "Atorvastatin"),
        ("36567", "Simvastatin"), ("42463", "Clopidogrel"),
        ("321988", "Escitalopram"), ("36437", "Paroxetine"),
        ("72625", "Duloxetine"), ("39786", "Venlafaxine"),
        ("596", "Alprazolam"), ("39993", "Zolpidem"),
        ("1946825", "Tirzepatide"), ("1545149", "Dulaglutide"),
        ("1992427", "Sacubitril"), ("69749", "Valsartan"),
        ("52175", "Losartan"), ("3827", "Enalapril"),
        ("29046", "Ramipril"), ("1998", "Candesartan"),
        ("83818", "Irbesartan"), ("73754", "Telmisartan"),
        ("69749", "Olmesartan"), ("321064", "Ezetimibe"),
        ("8183", "Pioglitazone"), ("4815", "Glipizide"),
        ("4821", "Glyburide"), ("25789", "Glimepiride"),
        ("35636", "Sitagliptin"), ("596730", "Saxagliptin"),
        ("857974", "Linagliptin"),
        ("203150", "Methotrexate"), ("5521", "Hydroxychloroquine"),
        ("9524", "Sulfasalazine"), ("202421", "Leflunomide"),
        ("27169", "Adalimumab"), ("191831", "Etanercept"),
        ("321208", "Infliximab"), ("258494", "Certolizumab"),
        ("1148767", "Tofacitinib"), ("2047232", "Baricitinib"),
        ("121243", "Pembrolizumab"), ("1597948", "Nivolumab"),
        ("1657991", "Atezolizumab"), ("2103181", "Durvalumab"),
    ]
    for rxcui, name in ingredients:
        rxnorm_drugs.append({"code": rxcui, "display": name, "semantic_group": "MEDICATION"})
    
    seen = set()
    deduped = []
    for d in rxnorm_drugs:
        key = d["code"]
        if key not in seen:
            seen.add(key)
            deduped.append(d)
    
    print(f"  Curated RxNorm set: {len(deduped)} codes (top prescribed + ingredient concepts)")
    return write_sql_batches("RXNORM", deduped)

# ============================================================
# 5. ICD-O-3 — SEER/WHO topography + morphology
# ============================================================
def load_icdo3():
    print("\n=== ICD-O-3 ===")
    icdo3_codes = []
    
    topography = [
        ("C00.0", "External upper lip"), ("C00.1", "External lower lip"), ("C00.9", "Lip, NOS"),
        ("C01.9", "Base of tongue, NOS"), ("C02.9", "Tongue, NOS"),
        ("C07.9", "Parotid gland"), ("C08.9", "Major salivary gland, NOS"),
        ("C09.9", "Tonsil, NOS"), ("C10.9", "Oropharynx, NOS"),
        ("C11.9", "Nasopharynx, NOS"), ("C12.9", "Pyriform sinus"),
        ("C13.9", "Hypopharynx, NOS"), ("C14.0", "Pharynx, NOS"),
        ("C15.9", "Esophagus, NOS"), ("C16.0", "Cardia, NOS"),
        ("C16.9", "Stomach, NOS"), ("C17.0", "Duodenum"),
        ("C17.9", "Small intestine, NOS"), ("C18.0", "Cecum"),
        ("C18.2", "Ascending colon"), ("C18.4", "Transverse colon"),
        ("C18.6", "Descending colon"), ("C18.7", "Sigmoid colon"),
        ("C18.9", "Colon, NOS"), ("C19.9", "Rectosigmoid junction"),
        ("C20.9", "Rectum, NOS"), ("C21.0", "Anus, NOS"),
        ("C22.0", "Liver"), ("C22.1", "Intrahepatic bile duct"),
        ("C23.9", "Gallbladder"), ("C24.0", "Extrahepatic bile duct"),
        ("C25.0", "Head of pancreas"), ("C25.9", "Pancreas, NOS"),
        ("C30.0", "Nasal cavity"), ("C30.1", "Middle ear"),
        ("C31.0", "Maxillary sinus"), ("C31.9", "Accessory sinus, NOS"),
        ("C32.0", "Glottis"), ("C32.9", "Larynx, NOS"),
        ("C33.9", "Trachea"), ("C34.0", "Main bronchus"),
        ("C34.1", "Upper lobe, lung"), ("C34.2", "Middle lobe, lung"),
        ("C34.3", "Lower lobe, lung"), ("C34.9", "Lung, NOS"),
        ("C37.9", "Thymus"), ("C38.0", "Heart"),
        ("C38.4", "Pleura, NOS"), ("C40.0", "Long bones of upper limb"),
        ("C40.2", "Long bones of lower limb"), ("C41.0", "Bones of skull and face"),
        ("C41.2", "Vertebral column"), ("C41.4", "Pelvic bones"),
        ("C42.0", "Blood"), ("C42.1", "Bone marrow"),
        ("C42.2", "Spleen"), ("C42.4", "Reticuloendothelial system, NOS"),
        ("C44.0", "Skin of lip, NOS"), ("C44.2", "External ear"),
        ("C44.3", "Skin of other and unspecified parts of face"),
        ("C44.4", "Skin of scalp and neck"), ("C44.5", "Skin of trunk"),
        ("C44.6", "Skin of upper limb and shoulder"), ("C44.7", "Skin of lower limb and hip"),
        ("C44.9", "Skin, NOS"), ("C48.0", "Retroperitoneum"),
        ("C48.1", "Specified parts of peritoneum"), ("C48.2", "Peritoneum, NOS"),
        ("C49.0", "Connective tissue of head, face, and neck"),
        ("C49.9", "Connective, subcutaneous and other soft tissues, NOS"),
        ("C50.0", "Nipple"), ("C50.1", "Central portion of breast"),
        ("C50.2", "Upper-inner quadrant of breast"), ("C50.3", "Lower-inner quadrant of breast"),
        ("C50.4", "Upper-outer quadrant of breast"), ("C50.5", "Lower-outer quadrant of breast"),
        ("C50.9", "Breast, NOS"), ("C51.9", "Vulva, NOS"),
        ("C52.9", "Vagina, NOS"), ("C53.0", "Endocervix"),
        ("C53.1", "Exocervix"), ("C53.9", "Cervix uteri"),
        ("C54.1", "Endometrium"), ("C54.9", "Corpus uteri"),
        ("C55.9", "Uterus, NOS"), ("C56.9", "Ovary"),
        ("C57.0", "Fallopian tube"), ("C60.9", "Penis, NOS"),
        ("C61.9", "Prostate gland"), ("C62.1", "Descended testis"),
        ("C62.9", "Testis, NOS"), ("C64.9", "Kidney, NOS"),
        ("C65.9", "Renal pelvis"), ("C66.9", "Ureter"),
        ("C67.0", "Trigone of bladder"), ("C67.9", "Bladder, NOS"),
        ("C68.9", "Urinary system, NOS"), ("C69.0", "Conjunctiva"),
        ("C69.2", "Retina"), ("C69.9", "Eye, NOS"),
        ("C70.0", "Cerebral meninges"), ("C70.9", "Meninges, NOS"),
        ("C71.0", "Cerebrum"), ("C71.1", "Frontal lobe"),
        ("C71.2", "Temporal lobe"), ("C71.3", "Parietal lobe"),
        ("C71.4", "Occipital lobe"), ("C71.6", "Cerebellum, NOS"),
        ("C71.7", "Brain stem"), ("C71.9", "Brain, NOS"),
        ("C72.0", "Spinal cord"), ("C72.9", "Nervous system, NOS"),
        ("C73.9", "Thyroid gland"), ("C74.0", "Cortex of adrenal gland"),
        ("C74.9", "Adrenal gland, NOS"), ("C75.1", "Pituitary gland"),
        ("C76.0", "Head, face or neck, NOS"), ("C76.1", "Thorax, NOS"),
        ("C76.2", "Abdomen, NOS"), ("C76.3", "Pelvis, NOS"),
        ("C76.7", "Other ill-defined sites"), ("C77.0", "Lymph nodes of head, face and neck"),
        ("C77.1", "Intrathoracic lymph nodes"), ("C77.2", "Intra-abdominal lymph nodes"),
        ("C77.4", "Inguinal lymph nodes"), ("C77.5", "Pelvic lymph nodes"),
        ("C77.9", "Lymph node, NOS"), ("C80.9", "Unknown primary site"),
    ]
    for code, display in topography:
        icdo3_codes.append({"code": code, "display": display, "semantic_group": "ANATOMY"})
    
    morphology = [
        ("8000/3", "Neoplasm, malignant"), ("8001/3", "Tumor cells, malignant"),
        ("8010/2", "Carcinoma in situ, NOS"), ("8010/3", "Carcinoma, NOS"),
        ("8012/3", "Large cell carcinoma, NOS"), ("8013/3", "Large cell neuroendocrine carcinoma"),
        ("8020/3", "Carcinoma, undifferentiated, NOS"), ("8021/3", "Carcinoma, anaplastic, NOS"),
        ("8022/3", "Pleomorphic carcinoma"), ("8030/3", "Giant cell and spindle cell carcinoma"),
        ("8031/3", "Giant cell carcinoma"), ("8032/3", "Spindle cell carcinoma, NOS"),
        ("8033/3", "Pseudosarcomatous carcinoma"), ("8041/3", "Small cell carcinoma, NOS"),
        ("8042/3", "Oat cell carcinoma"), ("8043/3", "Small cell carcinoma, fusiform cell"),
        ("8046/3", "Non-small cell carcinoma"), ("8050/2", "Papillary carcinoma in situ"),
        ("8050/3", "Papillary carcinoma, NOS"), ("8051/3", "Verrucous carcinoma, NOS"),
        ("8052/3", "Papillary squamous cell carcinoma"), ("8070/2", "Squamous cell carcinoma in situ, NOS"),
        ("8070/3", "Squamous cell carcinoma, NOS"), ("8071/3", "Squamous cell carcinoma, keratinizing, NOS"),
        ("8072/3", "Squamous cell carcinoma, large cell, nonkeratinizing, NOS"),
        ("8073/3", "Squamous cell carcinoma, small cell, nonkeratinizing"),
        ("8074/3", "Squamous cell carcinoma, spindle cell"), ("8076/3", "Squamous cell carcinoma, micro-invasive"),
        ("8083/3", "Basaloid squamous cell carcinoma"), ("8084/3", "Squamous cell carcinoma, clear cell type"),
        ("8090/3", "Basal cell carcinoma, NOS"), ("8091/3", "Multifocal superficial basal cell carcinoma"),
        ("8092/3", "Infiltrating basal cell carcinoma, NOS"), ("8093/3", "Basal cell carcinoma, fibroepithelial"),
        ("8094/3", "Basosquamous carcinoma"), ("8140/2", "Adenocarcinoma in situ, NOS"),
        ("8140/3", "Adenocarcinoma, NOS"), ("8141/3", "Scirrhous adenocarcinoma"),
        ("8143/3", "Superficial spreading adenocarcinoma"), ("8147/3", "Basal cell adenocarcinoma"),
        ("8148/2", "Glandular intraepithelial neoplasia, grade III"),
        ("8170/3", "Hepatocellular carcinoma, NOS"), ("8180/3", "Combined hepatocellular carcinoma and cholangiocarcinoma"),
        ("8200/3", "Adenoid cystic carcinoma"), ("8210/2", "Adenocarcinoma in situ in adenomatous polyp"),
        ("8210/3", "Adenocarcinoma in adenomatous polyp"), ("8211/3", "Tubular adenocarcinoma"),
        ("8230/3", "Solid carcinoma, NOS"), ("8240/3", "Carcinoid tumor, NOS"),
        ("8244/3", "Mixed adenoneuroendocrine carcinoma"), ("8246/3", "Neuroendocrine carcinoma, NOS"),
        ("8249/3", "Atypical carcinoid tumor"), ("8250/3", "Bronchiolo-alveolar adenocarcinoma, NOS"),
        ("8255/3", "Adenocarcinoma with mixed subtypes"), ("8260/3", "Papillary adenocarcinoma, NOS"),
        ("8261/2", "Adenocarcinoma in situ in villous adenoma"),
        ("8261/3", "Adenocarcinoma in villous adenoma"),
        ("8263/2", "Adenocarcinoma in situ in tubulovillous adenoma"),
        ("8263/3", "Adenocarcinoma in tubulovillous adenoma"),
        ("8310/3", "Clear cell adenocarcinoma, NOS"), ("8312/3", "Renal cell carcinoma"),
        ("8317/3", "Renal cell carcinoma, chromophobe type"),
        ("8323/3", "Mixed cell adenocarcinoma"), ("8340/3", "Papillary carcinoma, follicular variant"),
        ("8341/3", "Papillary microcarcinoma"), ("8343/3", "Papillary carcinoma, encapsulated"),
        ("8345/3", "Medullary thyroid carcinoma"), ("8346/3", "Mixed medullary-follicular carcinoma"),
        ("8380/3", "Endometrioid adenocarcinoma, NOS"), ("8382/3", "Endometrioid adenocarcinoma, secretory variant"),
        ("8430/3", "Mucoepidermoid carcinoma"), ("8440/3", "Cystadenocarcinoma, NOS"),
        ("8441/3", "Serous cystadenocarcinoma, NOS"), ("8450/3", "Papillary cystadenocarcinoma, NOS"),
        ("8460/3", "Papillary serous cystadenocarcinoma"), ("8461/3", "Serous surface papillary carcinoma"),
        ("8470/3", "Mucinous cystadenocarcinoma, NOS"), ("8471/3", "Papillary mucinous cystadenocarcinoma"),
        ("8480/3", "Mucinous adenocarcinoma"), ("8481/3", "Mucin-producing adenocarcinoma"),
        ("8490/3", "Signet ring cell carcinoma"), ("8500/2", "Intraductal carcinoma, noninfiltrating, NOS"),
        ("8500/3", "Infiltrating duct carcinoma, NOS"), ("8501/2", "Comedocarcinoma, noninfiltrating"),
        ("8502/3", "Secretory carcinoma of breast"), ("8503/2", "Intraductal papillary carcinoma"),
        ("8503/3", "Intraductal papillary adenocarcinoma with invasion"),
        ("8507/2", "Intraductal micropapillary carcinoma"), ("8510/3", "Medullary carcinoma, NOS"),
        ("8520/2", "Lobular carcinoma in situ, NOS"), ("8520/3", "Lobular carcinoma, NOS"),
        ("8521/3", "Infiltrating ductular carcinoma"), ("8522/3", "Infiltrating duct and lobular carcinoma"),
        ("8523/3", "Infiltrating duct mixed with other types of carcinoma"),
        ("8530/3", "Inflammatory carcinoma"), ("8541/3", "Paget disease and infiltrating duct carcinoma"),
        ("8550/3", "Acinar cell carcinoma"), ("8560/3", "Adenosquamous carcinoma"),
        ("8570/3", "Adenocarcinoma with squamous metaplasia"), ("8574/3", "Adenocarcinoma with neuroendocrine differentiation"),
        ("8575/3", "Metaplastic carcinoma, NOS"),
        ("8620/3", "Granulosa cell tumor, adult type"), ("8650/3", "Leydig cell tumor, NOS"),
        ("8680/3", "Paraganglioma, malignant"), ("8693/3", "Extra-adrenal paraganglioma, malignant"),
        ("8700/3", "Pheochromocytoma, malignant"), ("8720/3", "Malignant melanoma, NOS"),
        ("8721/3", "Nodular melanoma"), ("8722/3", "Balloon cell melanoma"),
        ("8723/3", "Malignant melanoma, regressing"), ("8730/3", "Amelanotic melanoma"),
        ("8740/3", "Malignant melanoma in junctional nevus"),
        ("8741/3", "Malignant melanoma in precancerous melanosis"),
        ("8742/3", "Lentigo maligna melanoma"), ("8743/3", "Superficial spreading melanoma"),
        ("8744/3", "Acral lentiginous melanoma, malignant"), ("8745/3", "Desmoplastic melanoma, malignant"),
        ("8770/3", "Mixed epithelioid and spindle cell melanoma"),
        ("8800/3", "Sarcoma, NOS"), ("8801/3", "Spindle cell sarcoma"),
        ("8802/3", "Giant cell sarcoma"), ("8803/3", "Small cell sarcoma"),
        ("8804/3", "Epithelioid sarcoma"), ("8805/3", "Undifferentiated sarcoma"),
        ("8810/3", "Fibrosarcoma, NOS"), ("8811/3", "Fibromyxosarcoma"),
        ("8815/3", "Solitary fibrous tumor, malignant"), ("8830/3", "Malignant fibrous histiocytoma"),
        ("8832/3", "Dermatofibrosarcoma, NOS"), ("8840/3", "Myxosarcoma"),
        ("8850/3", "Liposarcoma, NOS"), ("8851/3", "Liposarcoma, well differentiated"),
        ("8852/3", "Myxoid liposarcoma"), ("8854/3", "Pleomorphic liposarcoma"),
        ("8855/3", "Mixed liposarcoma"), ("8858/3", "Dedifferentiated liposarcoma"),
        ("8890/3", "Leiomyosarcoma, NOS"), ("8891/3", "Epithelioid leiomyosarcoma"),
        ("8895/3", "Myosarcoma"), ("8896/3", "Myxoid leiomyosarcoma"),
        ("8900/3", "Rhabdomyosarcoma, NOS"), ("8901/3", "Pleomorphic rhabdomyosarcoma, adult type"),
        ("8910/3", "Embryonal rhabdomyosarcoma"), ("8912/3", "Spindle cell rhabdomyosarcoma"),
        ("8920/3", "Alveolar rhabdomyosarcoma"), ("8935/3", "Stromal sarcoma, NOS"),
        ("8936/3", "Gastrointestinal stromal tumor, malignant"),
        ("8940/3", "Mixed tumor, malignant, NOS"), ("8950/3", "Mullerian mixed tumor"),
        ("8959/3", "Malignant cystic nephroma"), ("8963/3", "Malignant rhabdoid tumor"),
        ("8964/3", "Clear cell sarcoma of kidney"), ("8980/3", "Carcinosarcoma, NOS"),
        ("8990/3", "Mesenchymoma, malignant"), ("9040/3", "Synovial sarcoma, NOS"),
        ("9041/3", "Synovial sarcoma, spindle cell"), ("9042/3", "Synovial sarcoma, epithelioid cell"),
        ("9043/3", "Synovial sarcoma, biphasic"), ("9044/3", "Clear cell sarcoma, NOS"),
        ("9050/3", "Mesothelioma, malignant"), ("9051/3", "Fibrous mesothelioma, malignant"),
        ("9052/3", "Epithelioid mesothelioma, malignant"), ("9053/3", "Mesothelioma, biphasic, malignant"),
        ("9060/3", "Dysgerminoma"), ("9061/3", "Seminoma, NOS"),
        ("9064/3", "Germinoma"), ("9065/3", "Germ cell tumor, nonseminomatous"),
        ("9070/3", "Embryonal carcinoma, NOS"), ("9071/3", "Yolk sac tumor"),
        ("9080/3", "Teratoma, malignant, NOS"), ("9081/3", "Teratocarcinoma"),
        ("9082/3", "Malignant teratoma, undifferentiated"), ("9083/3", "Malignant teratoma, intermediate"),
        ("9084/3", "Teratoma with malignant transformation"),
        ("9085/3", "Mixed germ cell tumor"), ("9100/3", "Choriocarcinoma, NOS"),
        ("9120/3", "Hemangiosarcoma"), ("9130/3", "Hemangioendothelioma, malignant"),
        ("9133/3", "Epithelioid hemangioendothelioma, malignant"),
        ("9140/3", "Kaposi sarcoma"), ("9150/3", "Hemangiopericytoma, malignant"),
        ("9170/3", "Lymphangiosarcoma"), ("9180/3", "Osteosarcoma, NOS"),
        ("9181/3", "Chondroblastic osteosarcoma"), ("9182/3", "Fibroblastic osteosarcoma"),
        ("9183/3", "Telangiectatic osteosarcoma"), ("9185/3", "Small cell osteosarcoma"),
        ("9186/3", "Central osteosarcoma"), ("9187/3", "Osteosarcoma, NOS (parosteal)"),
        ("9192/3", "Parosteal osteosarcoma"), ("9193/3", "Periosteal osteosarcoma"),
        ("9194/3", "High grade surface osteosarcoma"), ("9195/3", "Intracortical osteosarcoma"),
        ("9220/3", "Chondrosarcoma, NOS"), ("9221/3", "Juxtacortical chondrosarcoma"),
        ("9231/3", "Myxoid chondrosarcoma"), ("9240/3", "Mesenchymal chondrosarcoma"),
        ("9242/3", "Clear cell chondrosarcoma"), ("9243/3", "Dedifferentiated chondrosarcoma"),
        ("9250/3", "Giant cell tumor of bone, malignant"), ("9260/3", "Ewing sarcoma"),
        ("9261/3", "Adamantinoma of long bones"), ("9270/3", "Odontogenic tumor, malignant"),
        ("9364/3", "Peripheral neuroectodermal tumor"), ("9365/3", "Askin tumor"),
        ("9370/3", "Chordoma, NOS"), ("9380/3", "Glioma, malignant"),
        ("9381/3", "Gliomatosis cerebri"), ("9382/3", "Mixed glioma"),
        ("9391/3", "Ependymoma, NOS"), ("9392/3", "Ependymoma, anaplastic"),
        ("9393/3", "Papillary ependymoma"), ("9394/3", "Myxopapillary ependymoma"),
        ("9395/3", "Papillary tumor of pineal region"), ("9400/3", "Astrocytoma, NOS"),
        ("9401/3", "Astrocytoma, anaplastic"), ("9410/3", "Protoplasmic astrocytoma"),
        ("9411/3", "Gemistocytic astrocytoma"), ("9420/3", "Fibrillary astrocytoma"),
        ("9421/3", "Pilocytic astrocytoma"), ("9424/3", "Pleomorphic xanthoastrocytoma"),
        ("9430/3", "Astroblastoma"), ("9431/3", "Angiocentric glioma"),
        ("9440/3", "Glioblastoma, NOS"), ("9441/3", "Giant cell glioblastoma"),
        ("9442/3", "Gliosarcoma"), ("9450/3", "Oligodendroglioma, NOS"),
        ("9451/3", "Oligodendroglioma, anaplastic"),
        ("9470/3", "Medulloblastoma, NOS"), ("9471/3", "Desmoplastic nodular medulloblastoma"),
        ("9472/3", "Medullomyoblastoma"), ("9473/3", "Primitive neuroectodermal tumor"),
        ("9474/3", "Large cell medulloblastoma"), ("9480/3", "Cerebellar sarcoma, NOS"),
        ("9500/3", "Neuroblastoma, NOS"), ("9510/3", "Retinoblastoma, NOS"),
        ("9520/3", "Olfactory neuroblastoma"), ("9530/3", "Meningioma, malignant"),
        ("9540/3", "Malignant peripheral nerve sheath tumor"),
        ("9560/3", "Neurilemoma, malignant"), ("9580/3", "Granular cell tumor, malignant"),
        ("9590/3", "Malignant lymphoma, NOS"), ("9591/3", "Malignant lymphoma, non-Hodgkin, NOS"),
        ("9596/3", "Composite Hodgkin and non-Hodgkin lymphoma"),
        ("9650/3", "Hodgkin lymphoma, NOS"), ("9651/3", "Hodgkin lymphoma, lymphocyte-rich"),
        ("9652/3", "Hodgkin lymphoma, mixed cellularity, NOS"),
        ("9653/3", "Hodgkin lymphoma, lymphocyte depletion, NOS"),
        ("9659/3", "Hodgkin lymphoma, nodular lymphocyte predominant"),
        ("9661/3", "Hodgkin granuloma"), ("9662/3", "Hodgkin sarcoma"),
        ("9663/3", "Hodgkin lymphoma, nodular sclerosis, NOS"),
        ("9670/3", "Malignant lymphoma, small B lymphocytic, NOS"),
        ("9671/3", "Malignant lymphoma, lymphoplasmacytic"),
        ("9673/3", "Mantle cell lymphoma"), ("9675/3", "Malignant lymphoma, mixed small and large cell, diffuse"),
        ("9678/3", "Primary effusion lymphoma"), ("9679/3", "Mediastinal large B-cell lymphoma"),
        ("9680/3", "Malignant lymphoma, large B-cell, diffuse, NOS"),
        ("9684/3", "Malignant lymphoma, large B-cell, diffuse, immunoblastic, NOS"),
        ("9687/3", "Burkitt lymphoma, NOS"), ("9689/3", "Splenic marginal zone B-cell lymphoma"),
        ("9690/3", "Follicular lymphoma, NOS"), ("9691/3", "Follicular lymphoma, grade 2"),
        ("9695/3", "Follicular lymphoma, grade 1"), ("9698/3", "Follicular lymphoma, grade 3"),
        ("9699/3", "Marginal zone B-cell lymphoma, NOS"),
        ("9700/3", "Mycosis fungoides"), ("9701/3", "Sezary syndrome"),
        ("9702/3", "Mature T-cell lymphoma, NOS"), ("9705/3", "Angioimmunoblastic T-cell lymphoma"),
        ("9714/3", "Anaplastic large cell lymphoma, T- and null-cell type"),
        ("9716/3", "Hepatosplenic T-cell lymphoma"),
        ("9717/3", "Intestinal T-cell lymphoma"), ("9718/3", "Primary cutaneous CD30-positive T-cell lymphoproliferative disorder"),
        ("9719/3", "NK/T-cell lymphoma, nasal type"), ("9727/3", "Precursor cell lymphoblastic lymphoma, NOS"),
        ("9728/3", "Precursor B-cell lymphoblastic lymphoma"), ("9729/3", "Precursor T-cell lymphoblastic lymphoma"),
        ("9731/3", "Plasmacytoma, NOS"), ("9732/3", "Multiple myeloma"),
        ("9733/3", "Plasma cell leukemia"), ("9734/3", "Plasmacytoma, extramedullary"),
        ("9740/3", "Mast cell sarcoma"), ("9741/3", "Malignant mastocytosis"),
        ("9742/3", "Mast cell leukemia"), ("9750/3", "Malignant histiocytosis"),
        ("9751/3", "Langerhans cell histiocytosis, NOS"), ("9755/3", "Histiocytic sarcoma"),
        ("9756/3", "Langerhans cell sarcoma"), ("9757/3", "Interdigitating dendritic cell sarcoma"),
        ("9758/3", "Follicular dendritic cell sarcoma"), ("9760/3", "Immunoproliferative disease, NOS"),
        ("9761/3", "Waldenstrom macroglobulinemia"), ("9764/3", "Immunoproliferative small intestinal disease"),
        ("9800/3", "Leukemia, NOS"), ("9801/3", "Acute leukemia, NOS"),
        ("9805/3", "Acute biphenotypic leukemia"), ("9806/3", "Mixed phenotype acute leukemia"),
        ("9807/3", "Mixed phenotype acute leukemia with t(9;22)"),
        ("9808/3", "Mixed phenotype acute leukemia with t(v;11q23)"),
        ("9811/3", "B lymphoblastic leukemia/lymphoma, NOS"),
        ("9812/3", "B lymphoblastic leukemia/lymphoma with t(9;22)"),
        ("9813/3", "B lymphoblastic leukemia/lymphoma with t(v;11q23)"),
        ("9814/3", "B lymphoblastic leukemia/lymphoma with t(12;21)"),
        ("9815/3", "B lymphoblastic leukemia/lymphoma with hyperdiploidy"),
        ("9816/3", "B lymphoblastic leukemia/lymphoma with hypodiploidy"),
        ("9817/3", "B lymphoblastic leukemia/lymphoma with t(5;14)"),
        ("9818/3", "B lymphoblastic leukemia/lymphoma with t(1;19)"),
        ("9820/3", "Lymphoid leukemia, NOS"), ("9823/3", "B-cell chronic lymphocytic leukemia/small lymphocytic lymphoma"),
        ("9826/3", "Burkitt cell leukemia"), ("9827/3", "Adult T-cell leukemia/lymphoma"),
        ("9831/3", "T-cell large granular lymphocytic leukemia"),
        ("9832/3", "Prolymphocytic leukemia, NOS"), ("9833/3", "Prolymphocytic leukemia, B-cell type"),
        ("9834/3", "Prolymphocytic leukemia, T-cell type"),
        ("9835/3", "Precursor cell lymphoblastic leukemia, NOS"),
        ("9836/3", "Precursor B-cell lymphoblastic leukemia"),
        ("9837/3", "Precursor T-cell lymphoblastic leukemia"),
        ("9840/3", "Acute myeloid leukemia, M6 type"), ("9860/3", "Myeloid leukemia, NOS"),
        ("9861/3", "Acute myeloid leukemia, NOS"), ("9863/3", "Chronic myeloid leukemia, NOS"),
        ("9865/3", "Acute myeloid leukemia with t(6;9)"),
        ("9866/3", "Acute promyelocytic leukemia, t(15;17)"),
        ("9867/3", "Acute myelomonocytic leukemia"),
        ("9869/3", "Acute myeloid leukemia with inv(3) or t(3;3)"),
        ("9870/3", "Acute basophilic leukemia"), ("9871/3", "Acute myeloid leukemia with abnormal marrow eosinophils"),
        ("9872/3", "Acute myeloid leukemia, minimal differentiation"),
        ("9873/3", "Acute myeloid leukemia without maturation"),
        ("9874/3", "Acute myeloid leukemia with maturation"),
        ("9875/3", "Chronic myelogenous leukemia, BCR-ABL1 positive"),
        ("9876/3", "Atypical chronic myeloid leukemia, BCR-ABL1 negative"),
        ("9891/3", "Acute monocytic leukemia"), ("9895/3", "Acute myeloid leukemia with myelodysplasia-related changes"),
        ("9896/3", "Acute myeloid leukemia, t(8;21)"),
        ("9897/3", "Acute myeloid leukemia with inv(16) or t(16;16)"),
        ("9898/3", "Myeloid leukemia associated with Down Syndrome"),
        ("9910/3", "Acute megakaryoblastic leukemia"),
        ("9911/3", "Acute myeloid leukemia with t(1;22)"),
        ("9920/3", "Therapy-related myeloid neoplasm"),
        ("9930/3", "Myeloid sarcoma"), ("9931/3", "Acute panmyelosis with myelofibrosis"),
        ("9940/3", "Hairy cell leukemia"), ("9945/3", "Chronic myelomonocytic leukemia, NOS"),
        ("9946/3", "Juvenile myelomonocytic leukemia"),
        ("9948/3", "Aggressive NK-cell leukemia"),
        ("9950/3", "Polycythemia vera"), ("9960/3", "Myeloproliferative neoplasm, NOS"),
        ("9961/3", "Primary myelofibrosis"), ("9962/3", "Essential thrombocythemia"),
        ("9963/3", "Chronic neutrophilic leukemia"), ("9964/3", "Hypereosinophilic syndrome"),
        ("9965/3", "Myeloid and lymphoid neoplasm with PDGFRA rearrangement"),
        ("9966/3", "Myeloid neoplasm with PDGFRB rearrangement"),
        ("9967/3", "Myeloid and lymphoid neoplasm with FGFR1 abnormalities"),
        ("9971/3", "Polymorphic PTLD"), ("9975/3", "Myelodysplastic/myeloproliferative neoplasm, unclassifiable"),
        ("9980/3", "Refractory anemia"), ("9982/3", "Refractory anemia with ring sideroblasts"),
        ("9983/3", "Refractory anemia with excess blasts"), ("9984/3", "Refractory anemia with excess blasts in transformation"),
        ("9985/3", "Refractory cytopenia with multilineage dysplasia"),
        ("9986/3", "Myelodysplastic syndrome with 5q deletion syndrome"),
        ("9987/3", "Therapy-related myelodysplastic syndrome"),
        ("9989/3", "Myelodysplastic syndrome, NOS"),
    ]
    for code, display in morphology:
        icdo3_codes.append({"code": code, "display": display, "semantic_group": "TUMOR"})
    
    print(f"  Curated ICD-O-3 set: {len(icdo3_codes)} codes ({len(topography)} topography + {len(morphology)} morphology)")
    return write_sql_batches("ICDO3", icdo3_codes)

# ============================================================
# 6. MedDRA — from FDA FAERS public download (PT terms)
# ============================================================
def load_meddra():
    print("\n=== MedDRA (from FDA FAERS derivative) ===")
    url = "https://api.fda.gov/drug/event.json?count=patient.reaction.reactionmeddrapt.exact&limit=1000"
    print(f"  Fetching FDA FAERS top adverse event terms...")
    try:
        resp = fetch_url(url, timeout=60)
        data = json.loads(resp.read().decode("utf-8"))
        results = data.get("results", [])
        rows = []
        for r in results:
            term = r.get("term", "")
            if term:
                rows.append({"code": term.upper().replace(" ", "_")[:50], "display": term, "semantic_group": "DISEASE"})
        
        if len(rows) < 500:
            url2 = "https://api.fda.gov/drug/event.json?count=patient.reaction.reactionmeddrapt.exact&limit=1000&skip=1000"
            try:
                resp2 = fetch_url(url2, timeout=60)
                data2 = json.loads(resp2.read().decode("utf-8"))
                for r in data2.get("results", []):
                    term = r.get("term", "")
                    if term:
                        rows.append({"code": term.upper().replace(" ", "_")[:50], "display": term, "semantic_group": "DISEASE"})
            except:
                pass
        
        seen = set()
        deduped = []
        for r in rows:
            if r["display"].lower() not in seen:
                seen.add(r["display"].lower())
                deduped.append(r)
        
        print(f"  Got {len(deduped)} MedDRA PT terms from FAERS")
        return write_sql_batches("MEDDRA", deduped)
    except Exception as e:
        print(f"  ERROR: {e}")
        return []

# ============================================================
# 7. HCPCS (CPT derivative) — CMS.gov
# ============================================================
def load_hcpcs():
    print("\n=== HCPCS (CMS.gov) ===")
    url = "https://www.cms.gov/files/zip/2025-alpha-numeric-hcpcs-file.zip"
    print(f"  Fetching {url}")
    try:
        resp = fetch_url(url, timeout=120)
        zdata = io.BytesIO(resp.read())
        rows = []
        with zipfile.ZipFile(zdata) as zf:
            target = None
            for name in zf.namelist():
                if name.endswith(".csv") or name.endswith(".txt"):
                    target = name
                    break
            if not target:
                for name in zf.namelist():
                    if "ANWEB" in name.upper() or "hcpcs" in name.lower():
                        target = name
                        break
            if not target and zf.namelist():
                target = zf.namelist()[0]
            
            if target:
                print(f"  Parsing {target}")
                with zf.open(target) as f:
                    content = f.read().decode("utf-8", errors="replace")
                    for line in content.split("\n"):
                        parts = line.strip().split(",")
                        if len(parts) >= 2:
                            code = parts[0].strip().strip('"')
                            desc = parts[1].strip().strip('"')
                            if code and desc and len(code) == 5 and code[0].isalpha():
                                rows.append({"code": code, "display": desc, "semantic_group": "PROCEDURE"})
        
        if rows:
            return write_sql_batches("HCPCS", rows)
        else:
            print("  Could not parse HCPCS. Trying alternate approach...")
    except Exception as e:
        print(f"  Error: {e}")
    
    print("  Using curated HCPCS E/M and common procedure codes...")
    hcpcs = [
        ("99201", "Office or other outpatient visit, new patient, level 1"),
        ("99202", "Office or other outpatient visit, new patient, level 2"),
        ("99203", "Office or other outpatient visit, new patient, level 3"),
        ("99204", "Office or other outpatient visit, new patient, level 4"),
        ("99205", "Office or other outpatient visit, new patient, level 5"),
        ("99211", "Office or other outpatient visit, established patient, level 1"),
        ("99212", "Office or other outpatient visit, established patient, level 2"),
        ("99213", "Office or other outpatient visit, established patient, level 3"),
        ("99214", "Office or other outpatient visit, established patient, level 4"),
        ("99215", "Office or other outpatient visit, established patient, level 5"),
        ("99221", "Initial hospital care, low complexity"),
        ("99222", "Initial hospital care, moderate complexity"),
        ("99223", "Initial hospital care, high complexity"),
        ("99231", "Subsequent hospital care, low complexity"),
        ("99232", "Subsequent hospital care, moderate complexity"),
        ("99233", "Subsequent hospital care, high complexity"),
        ("99238", "Hospital discharge day management, 30 min or less"),
        ("99239", "Hospital discharge day management, more than 30 min"),
        ("99281", "Emergency department visit, level 1"),
        ("99282", "Emergency department visit, level 2"),
        ("99283", "Emergency department visit, level 3"),
        ("99284", "Emergency department visit, level 4"),
        ("99285", "Emergency department visit, level 5"),
        ("99291", "Critical care, first 30-74 minutes"),
        ("99292", "Critical care, each additional 30 minutes"),
        ("99381", "Preventive visit, new patient, infant"),
        ("99391", "Preventive visit, established patient, infant"),
        ("99395", "Preventive visit, established patient, 18-39 years"),
        ("99396", "Preventive visit, established patient, 40-64 years"),
        ("99397", "Preventive visit, established patient, 65+ years"),
        ("G0101", "Cervical or vaginal cancer screening; pelvic and clinical breast exam"),
        ("G0102", "Prostate cancer screening; digital rectal exam"),
        ("G0103", "Prostate cancer screening; prostate specific antigen test (PSA)"),
        ("G0104", "Colorectal cancer screening; flexible sigmoidoscopy"),
        ("G0105", "Colorectal cancer screening; colonoscopy on individual at high risk"),
        ("G0121", "Colorectal cancer screening; colonoscopy on individual not meeting criteria for high risk"),
        ("G0127", "Trimming of dystrophic nails"),
        ("G0402", "Initial preventive physical exam (Welcome to Medicare visit)"),
        ("G0438", "Annual wellness visit, initial"),
        ("G0439", "Annual wellness visit, subsequent"),
        ("G0442", "Annual alcohol misuse screening"),
        ("G0443", "Brief face-to-face behavioral counseling for alcohol misuse"),
        ("G0444", "Annual depression screening"),
        ("G0446", "Annual, face-to-face intensive behavioral therapy for cardiovascular disease"),
        ("G0447", "Face-to-face behavioral counseling for obesity"),
        ("J0129", "Injection, abatacept, 10 mg"),
        ("J0135", "Injection, adalimumab, 20 mg"),
        ("J0178", "Injection, aflibercept, 1 mg"),
        ("J0585", "Injection, onabotulinumtoxinA, 1 unit"),
        ("J1030", "Injection, methylprednisolone acetate, 40 mg"),
        ("J1040", "Injection, methylprednisolone acetate, 80 mg"),
        ("J1100", "Injection, dexamethasone sodium phosphate, 1 mg"),
        ("J1170", "Injection, hydromorphone, up to 4 mg"),
        ("J1200", "Injection, diphenhydramine HCl, up to 50 mg"),
        ("J1644", "Injection, heparin sodium, per 1000 units"),
        ("J1650", "Injection, enoxaparin sodium, 10 mg"),
        ("J1745", "Injection, infliximab, 10 mg"),
        ("J1885", "Injection, ketorolac tromethamine, per 15 mg"),
        ("J2001", "Injection, lidocaine HCl, 10 mg"),
        ("J2175", "Injection, meperidine HCl, per 100 mg"),
        ("J2250", "Injection, midazolam HCl, per 1 mg"),
        ("J2270", "Injection, morphine sulfate, up to 10 mg"),
        ("J2405", "Injection, ondansetron HCl, per 1 mg"),
        ("J2550", "Injection, promethazine HCl, up to 50 mg"),
        ("J2704", "Injection, propofol, 10 mg"),
        ("J2765", "Injection, metoclopramide HCl, up to 10 mg"),
        ("J2930", "Injection, methylprednisolone sodium succinate, up to 40 mg"),
        ("J2997", "Injection, alteplase recombinant, 1 mg"),
        ("J3010", "Injection, fentanyl citrate, 0.1 mg"),
        ("J3301", "Injection, triamcinolone acetonide, per 10 mg"),
        ("J3370", "Injection, vancomycin HCl, 500 mg"),
        ("J3490", "Unclassified drugs"),
        ("J7040", "Infusion, normal saline solution, sterile (500 ml = 1 unit)"),
        ("J7050", "Infusion, normal saline solution, 250 cc"),
        ("J7120", "Ringers lactate infusion, up to 1000 cc"),
        ("J9035", "Injection, bevacizumab, 10 mg"),
        ("J9041", "Injection, bortezomib, 0.1 mg"),
        ("J9228", "Injection, ipilimumab, 1 mg"),
        ("J9271", "Injection, pembrolizumab, 1 mg"),
        ("J9299", "Injection, nivolumab, 1 mg"),
        ("J9355", "Injection, trastuzumab, 10 mg"),
        ("Q0138", "Injection, ferumoxytol, 1 mg"),
        ("Q2017", "Injection, teniposide, 50 mg"),
        ("Q2050", "Injection, doxorubicin HCl, liposomal, 10 mg"),
        ("Q5101", "Injection, filgrastim-sndz, biosimilar, 1 mcg"),
        ("Q5103", "Injection, infliximab-dyyb, biosimilar, 10 mg"),
        ("Q5104", "Injection, infliximab-abda, biosimilar, 10 mg"),
        ("A4206", "Syringe with needle, sterile, 1 cc or less"),
        ("A4253", "Blood glucose test or reagent strips for home glucose monitor"),
        ("A4550", "Surgical trays"),
        ("E0114", "Crutches, underarm, other than wood, adjustable or fixed, pair"),
        ("E0431", "Portable gaseous oxygen system, rental"),
        ("E0601", "Continuous airway pressure (CPAP) device"),
        ("L0180", "Cervical, multiple post collar, occipital/mandibular supports"),
        ("L3020", "Foot, longitudinal arch support, removable, premolded"),
    ]
    hcpcs_rows = [{"code": c, "display": d, "semantic_group": "PROCEDURE"} for c, d in hcpcs]
    
    devices = [
        ("A4206", "Syringe with needle"), ("A4253", "Blood glucose test strips"),
        ("E0114", "Crutches"), ("E0431", "Portable oxygen system"),
        ("E0601", "CPAP device"), ("E0570", "Nebulizer"),
    ]
    
    print(f"  Curated HCPCS set: {len(hcpcs)} codes (E/M, G-codes, J-codes, DME)")
    return write_sql_batches("HCPCS", hcpcs_rows)

# ============================================================
# 8. SNOMED CT — from NLM (try FHIR ValueSet or public subset)
# ============================================================
def load_snomed():
    print("\n=== SNOMED CT ===")
    print("  SNOMED CT requires UMLS account for full download.")
    print("  Loading curated SNOMED CT core clinical concepts...")
    
    snomed_codes = []
    
    clinical_findings = [
        ("38341003", "Hypertension"), ("44054006", "Type 2 diabetes mellitus"),
        ("46635009", "Type 1 diabetes mellitus"), ("84114007", "Heart failure"),
        ("53741008", "Coronary arteriosclerosis"), ("22298006", "Myocardial infarction"),
        ("195967001", "Asthma"), ("13645005", "Chronic obstructive lung disease"),
        ("709044004", "Chronic kidney disease"), ("40930008", "Hypothyroidism"),
        ("34095006", "Dehydration"), ("267036007", "Dyspnea"),
        ("25064002", "Headache"), ("21522001", "Abdominal pain"),
        ("29857009", "Chest pain"), ("386661006", "Fever"),
        ("422400008", "Vomiting"), ("422587007", "Nausea"),
        ("62315008", "Diarrhea"), ("68235000", "Constipation"),
        ("271807003", "Rash"), ("49727002", "Cough"),
        ("82423001", "Chronic pain"), ("36971009", "Sinusitis"),
        ("68566005", "Urinary tract infection"), ("233604007", "Pneumonia"),
        ("128053003", "Deep venous thrombosis"), ("59282003", "Pulmonary embolism"),
        ("230690007", "Stroke"), ("128613002", "Seizure disorder"),
        ("35489007", "Depression"), ("197480006", "Anxiety disorder"),
        ("73211009", "Diabetes mellitus"), ("399068003", "Malignant neoplasm of prostate"),
        ("254837009", "Malignant neoplasm of breast"), ("363406005", "Malignant neoplasm of colon"),
        ("363358000", "Malignant neoplasm of lung"), ("93870000", "Malignant neoplasm of pancreas"),
        ("126906006", "Neoplasm of liver"), ("363505006", "Malignant neoplasm of head and neck"),
        ("363443007", "Malignant neoplasm of ovary"), ("363349007", "Malignant neoplasm of stomach"),
        ("363346000", "Malignant neoplasm of esophagus"), ("94381002", "Malignant neoplasm of brain"),
        ("399326009", "Malignant neoplasm of bladder"), ("363518003", "Malignant neoplasm of kidney"),
        ("109838007", "Malignant melanoma of skin"), ("1201005", "Benign neoplasm of colon"),
        ("61977001", "Chronic hepatitis"), ("235856003", "Liver disease"),
        ("128302006", "Chronic hepatitis C"), ("66071002", "Type B viral hepatitis"),
        ("34000006", "Crohn disease"), ("64766004", "Ulcerative colitis"),
        ("69896004", "Rheumatoid arthritis"), ("55822004", "Hyperlipidemia"),
        ("271737000", "Anemia"), ("109989006", "Multiple myeloma"),
        ("93143009", "Leukemia"), ("118599009", "Hodgkin disease"),
        ("118601006", "Non-Hodgkin lymphoma"), ("128462008", "Metastatic malignant neoplasm"),
        ("363418001", "Malignant neoplasm of thyroid gland"),
        ("126851005", "Neoplasm of central nervous system"),
        ("86299006", "Tetralogy of Fallot"), ("399211009", "History of myocardial infarction"),
        ("414545008", "Ischemic heart disease"), ("49436004", "Atrial fibrillation"),
        ("195080001", "Atrial flutter"), ("233817007", "Supraventricular tachycardia"),
        ("251170000", "Ventricular tachycardia"), ("44103008", "Aortic stenosis"),
        ("79619009", "Mitral valve stenosis"), ("48724000", "Mitral valve insufficiency"),
        ("60234000", "Aortic valve insufficiency"), ("111287006", "Tricuspid valve insufficiency"),
        ("233970002", "Pulmonary hypertension"), ("61490001", "Angina pectoris"),
        ("73774007", "Acute renal failure"), ("90708001", "Kidney disease"),
        ("431855005", "Chronic kidney disease stage 1"), ("431856006", "Chronic kidney disease stage 2"),
        ("433144002", "Chronic kidney disease stage 3"), ("431857002", "Chronic kidney disease stage 4"),
        ("433146000", "Chronic kidney disease stage 5"), ("46177005", "End-stage renal disease"),
        ("236578006", "Chronic kidney disease stage 3A"), ("700378005", "Chronic kidney disease stage 3B"),
        ("197927001", "Acute pancreatitis"), ("48340000", "Incision and drainage of abscess"),
        ("30832001", "Peptic ulcer"), ("235595009", "Gastroesophageal reflux disease"),
        ("19887002", "Gastrointestinal hemorrhage"), ("54150009", "Upper respiratory tract infection"),
        ("312682007", "Osteoporosis without fracture"), ("64859006", "Osteoporosis"),
        ("396275006", "Osteoarthritis"), ("77386006", "Pregnancy"),
        ("80394007", "Hyperglycemia"), ("302866003", "Hypoglycemia"),
        ("414478003", "Metabolic acidosis"), ("37796009", "Migraine"),
        ("386517000", "Dementia"), ("26929004", "Alzheimer disease"),
        ("49049000", "Parkinson disease"), ("24700007", "Multiple sclerosis"),
        ("39898005", "Sleep disorder"), ("73430006", "Sleep apnea"),
        ("13746004", "Bipolar disorder"), ("58214004", "Schizophrenia"),
        ("66348005", "Alcohol abuse"), ("6525002", "Substance abuse"),
        ("191736004", "Tobacco dependence"), ("70076002", "Rhinitis"),
        ("40055000", "Chronic sinusitis"), ("87628006", "Bacterial infection"),
        ("34014006", "Viral disease"), ("414029004", "Disorder of immune function"),
        ("53084003", "Bacterial pneumonia"), ("75570004", "Viral pneumonia"),
        ("312099009", "Candidiasis"), ("58750007", "Cellulitis"),
        ("3723001", "Arthritis"), ("410429000", "Cardiac arrest"),
        ("233604007", "Pneumonia"), ("40733004", "Infectious disease"),
        ("72892002", "Normal pregnancy"), ("237311001", "Threatened abortion"),
        ("48194001", "Pregnancy-induced hypertension"), ("11687002", "Gestational diabetes"),
        ("17382005", "Pre-eclampsia"), ("15938005", "Eclampsia"),
        ("200936003", "Lupus erythematosus"), ("31996006", "Vasculitis"),
        ("111552007", "Psoriasis"), ("43116000", "Eczema"),
        ("95320005", "Inflammation of skin"), ("72098002", "Electrolyte disorder"),
        ("14304000", "Disorder of thyroid gland"), ("237637005", "Overweight and obesity"),
        ("238131007", "Overweight"), ("414916001", "Obesity"),
        ("190905008", "Cystic fibrosis"), ("56717001", "Tuberculosis"),
        ("15628003", "Gonorrhea"), ("76272004", "Syphilis"),
        ("402196005", "Childhood immunization completed"), ("33879002", "Vaccination"),
    ]
    for code, display in clinical_findings:
        snomed_codes.append({"code": code, "display": display, "semantic_group": "DISEASE"})
    
    symptoms = [
        ("21522001", "Abdominal pain"), ("29857009", "Chest pain"),
        ("25064002", "Headache"), ("267036007", "Dyspnea"),
        ("386661006", "Fever"), ("422400008", "Vomiting"),
        ("422587007", "Nausea"), ("62315008", "Diarrhea"),
        ("68235000", "Constipation"), ("49727002", "Cough"),
        ("271807003", "Rash"), ("271681002", "Stomach ache"),
        ("84229001", "Fatigue"), ("60862001", "Tinnitus"),
        ("404640003", "Dizziness"), ("162076009", "Excessive sweating"),
        ("275280004", "Chest tightness"), ("271757001", "Palpitations"),
        ("285381006", "Acute chest pain"), ("22253000", "Pain"),
        ("267102003", "Sore throat"), ("162397003", "Pain in throat"),
        ("76948002", "Severe pain"), ("271587009", "Stiffness"),
        ("162607003", "Back pain"), ("16001004", "Otalgia"),
        ("162290004", "Dry mouth"), ("247592009", "Shortness of breath at rest"),
        ("248595008", "Sputum finding"), ("23924001", "Tight chest"),
        ("57676002", "Joint pain"), ("161891005", "Backache"),
        ("139394000", "Wheezing"), ("11833005", "Dry cough"),
        ("28743005", "Productive cough"), ("267060006", "Painful breathing"),
        ("22253000", "Pain"), ("102614006", "Ankle swelling"),
        ("299029005", "Swelling of ankle"), ("20262006", "Blurred vision"),
        ("267064002", "Retention of urine"), ("49650001", "Dysuria"),
        ("165232002", "Urinary incontinence"), ("364665006", "Weight gain"),
        ("89362005", "Weight loss"), ("193462001", "Insomnia"),
        ("56018004", "Wheezing"), ("18165001", "Jaundice"),
        ("3006004", "Disturbed sleep"), ("267036007", "Dyspnea"),
    ]
    for code, display in symptoms:
        snomed_codes.append({"code": code, "display": display, "semantic_group": "SYMPTOM"})
    
    procedures = [
        ("80146002", "Appendectomy"), ("38102005", "Cholecystectomy"),
        ("73761001", "Colonoscopy"), ("174041007", "Laparoscopic cholecystectomy"),
        ("387713003", "Surgical procedure"), ("122548005", "Biopsy"),
        ("71388002", "Surgical procedure on respiratory system"),
        ("265764009", "Renal dialysis"), ("302497006", "Hemodialysis"),
        ("313030004", "CABG - Coronary artery bypass graft"),
        ("232717009", "Coronary artery bypass grafting"), ("429609002", "Percutaneous coronary intervention"),
        ("18949003", "Change of tracheostomy tube"), ("58390007", "Allogeneic bone marrow transplantation"),
        ("234319005", "Splenectomy"), ("65801008", "Total knee replacement"),
        ("179344006", "Total hip replacement"), ("112698002", "Sigmoidoscopy"),
        ("386637004", "Blood transfusion"), ("33195004", "Extracorporeal circulation"),
        ("415070008", "Percutaneous coronary intervention"), ("232721000", "Aortic valve replacement"),
        ("441509002", "Cardiac pacemaker procedure"), ("449381001", "Breast biopsy"),
        ("609588000", "Total colectomy"), ("26390003", "Total hysterectomy"),
        ("236886002", "Endoscopy"), ("6025007", "Laparoscopic appendectomy"),
        ("387731002", "Cataract surgery"), ("81723002", "Amputation"),
        ("274025005", "Bone marrow biopsy"), ("241615005", "Magnetic resonance imaging"),
        ("169690007", "Computed tomography"), ("44491008", "Fluoroscopy"),
        ("77477000", "Computerized axial tomography"), ("71651007", "Mammography"),
        ("16310003", "Diagnostic ultrasonography"), ("363680008", "Radiographic imaging procedure"),
        ("18629005", "Lumbar puncture"), ("397956004", "Central venous catheterization"),
        ("418891003", "Central venous catheter insertion"), ("112790001", "Nasogastric tube insertion"),
        ("14768001", "Thoracentesis"), ("91602002", "Paracentesis"),
        ("4525004", "Emergency department patient visit"), ("305351004", "Admission to hospital"),
        ("58000006", "Patient discharge"), ("183452005", "Emergency hospital admission"),
    ]
    for code, display in procedures:
        snomed_codes.append({"code": code, "display": display, "semantic_group": "PROCEDURE"})
    
    body_structures = [
        ("80891009", "Heart structure"), ("39607008", "Lung structure"),
        ("10200004", "Liver structure"), ("64033007", "Kidney structure"),
        ("12738006", "Brain structure"), ("78961009", "Spleen structure"),
        ("15497006", "Ovary structure"), ("71341001", "Bone structure of femur"),
        ("181216001", "Entire lung"), ("362358000", "Entire heart"),
        ("113343008", "Liver and intrahepatic biliary tract structure"),
        ("87878005", "Left ventricle structure"), ("73829009", "Right ventricle structure"),
        ("244023005", "Left atrium structure"), ("244022000", "Right atrium structure"),
        ("69105007", "Carotid artery structure"), ("86547008", "Aorta structure"),
        ("113197003", "Bone structure of rib"), ("12611008", "Bone structure of sternum"),
        ("816092008", "Structure of right lung"), ("816091001", "Structure of left lung"),
        ("181277001", "Entire pancreas"), ("18911002", "Penile structure"),
        ("41216001", "Prostate structure"), ("71252005", "Cervix uteri structure"),
        ("85562004", "Breast structure"), ("23451007", "Adrenal structure"),
        ("69536005", "Head structure"), ("122494005", "Cervical spine structure"),
        ("52896000", "Thoracic spine structure"), ("73903008", "Lumbar spine structure"),
        ("60184004", "Ankle joint structure"), ("72696002", "Knee joint structure"),
        ("53620006", "Hip joint structure"), ("16953009", "Elbow joint structure"),
        ("85537004", "Wrist joint structure"), ("36455000", "Rectum structure"),
        ("245857005", "Colon structure"), ("21306003", "Jejunum structure"),
        ("38848004", "Duodenum structure"), ("34402009", "Rectosigmoid junction structure"),
        ("15776009", "Stomach structure"), ("32849002", "Esophagus structure"),
        ("44567001", "Trachea structure"), ("4596009", "Larynx structure"),
        ("90228003", "Parotid gland structure"), ("181234002", "Entire thyroid gland"),
        ("14264009", "Pituitary structure"), ("3711007", "Bone structure of ischium"),
        ("51185008", "Vertebral column structure"), ("71854001", "Colon structure"),
        ("87953007", "Uterine structure"), ("83670000", "Bladder structure"),
        ("23074001", "Fallopian tube structure"), ("13648007", "Ureter structure"),
        ("119219003", "Specimen from blood"), ("14016003", "Bone marrow structure"),
        ("65653002", "Skin structure"), ("59820001", "Blood vessel structure"),
        ("39937001", "Skin structure"), ("78067005", "Appendix structure"),
        ("28726007", "Cornea structure"), ("18619003", "Adrenal cortex structure"),
    ]
    for code, display in body_structures:
        snomed_codes.append({"code": code, "display": display, "semantic_group": "ANATOMY"})
    
    seen = set()
    deduped = []
    for s in snomed_codes:
        key = s["code"]
        if key not in seen:
            seen.add(key)
            deduped.append(s)
    
    print(f"  Curated SNOMED CT set: {len(deduped)} codes (clinical findings, symptoms, procedures, body structures)")
    return write_sql_batches("SNOMEDCT", deduped)


def main():
    print("=" * 60)
    print("Clinical NLP Terminology Seed Data Loader")
    print("=" * 60)
    
    all_files = {}
    
    all_files["ICD10CM"] = load_icd10cm()
    all_files["ICD10PCS"] = load_icd10pcs()
    all_files["LOINC"] = load_loinc()
    all_files["RXNORM"] = load_rxnorm()
    all_files["ICDO3"] = load_icdo3()
    all_files["MEDDRA"] = load_meddra()
    all_files["HCPCS"] = load_hcpcs()
    all_files["SNOMEDCT"] = load_snomed()
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    total_files = 0
    for cs, files in all_files.items():
        print(f"  {cs}: {len(files)} batch files")
        total_files += len(files)
    print(f"  TOTAL: {total_files} SQL batch files in {OUTPUT_DIR}")
    
    manifest = os.path.join(OUTPUT_DIR, "manifest.json")
    with open(manifest, "w") as f:
        json.dump({cs: [os.path.basename(fp) for fp in files] for cs, files in all_files.items()}, f, indent=2)
    print(f"  Manifest: {manifest}")

if __name__ == "__main__":
    main()
