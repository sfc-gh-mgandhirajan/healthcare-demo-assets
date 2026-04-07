import csv
import random
from datetime import datetime, timedelta
import uuid

random.seed(42)

NUM_CLAIMS = 100
BASE_DATE = datetime(2024, 1, 1)

PAYERS = ["BCBS", "Aetna", "Cigna", "UnitedHealthcare", "Humana", "Medicare", "Medicaid"]
PROVIDERS = [
    ("P001", "City General Hospital", "Hospital"),
    ("P002", "Regional Medical Center", "Hospital"),
    ("P003", "Primary Care Associates", "Clinic"),
    ("P004", "Specialty Care Partners", "Clinic"),
    ("P005", "Community Health Center", "Clinic"),
]
PROCEDURE_CODES = [
    ("99213", "Office visit - established patient", 85.00, 150.00),
    ("99214", "Office visit - established patient (moderate)", 120.00, 200.00),
    ("99215", "Office visit - established patient (high)", 175.00, 280.00),
    ("99203", "Office visit - new patient", 100.00, 175.00),
    ("99204", "Office visit - new patient (moderate)", 150.00, 250.00),
    ("36415", "Venipuncture", 15.00, 35.00),
    ("80053", "Comprehensive metabolic panel", 25.00, 75.00),
    ("85025", "Complete blood count", 18.00, 50.00),
    ("71046", "Chest X-ray", 75.00, 150.00),
    ("73030", "Shoulder X-ray", 65.00, 130.00),
    ("29881", "Knee arthroscopy", 800.00, 2500.00),
    ("43239", "Upper GI endoscopy", 600.00, 1800.00),
    ("45380", "Colonoscopy with biopsy", 900.00, 2800.00),
    ("90834", "Psychotherapy 45 min", 100.00, 180.00),
    ("90837", "Psychotherapy 60 min", 140.00, 220.00),
]
DIAGNOSIS_CODES = [
    ("I10", "Essential hypertension"),
    ("E11.9", "Type 2 diabetes mellitus"),
    ("J06.9", "Acute upper respiratory infection"),
    ("M54.5", "Low back pain"),
    ("K21.0", "Gastro-esophageal reflux disease"),
    ("F32.9", "Major depressive disorder"),
    ("J45.909", "Asthma, unspecified"),
    ("M17.11", "Primary osteoarthritis, right knee"),
    ("N39.0", "Urinary tract infection"),
    ("R51", "Headache"),
]
STATUS_OPTIONS = ["PAID", "PENDING", "DENIED", "ADJUSTED"]

def generate_member_id():
    return f"MEM{random.randint(100000, 999999)}"

def generate_claims():
    claims = []
    lines = []
    
    for i in range(NUM_CLAIMS):
        claim_id = f"CLM{str(i+1).zfill(8)}"
        member_id = generate_member_id()
        payer = random.choice(PAYERS)
        provider_id, provider_name, provider_type = random.choice(PROVIDERS)
        
        service_date = BASE_DATE + timedelta(days=random.randint(0, 365))
        submitted_date = service_date + timedelta(days=random.randint(1, 7))
        
        primary_dx = random.choice(DIAGNOSIS_CODES)
        secondary_dx = random.choice([dx for dx in DIAGNOSIS_CODES if dx != primary_dx]) if random.random() > 0.3 else None
        
        status = random.choices(STATUS_OPTIONS, weights=[70, 15, 10, 5])[0]
        
        num_lines = random.randint(1, 5)
        total_billed = 0
        total_allowed = 0
        total_paid = 0
        
        selected_procedures = random.sample(PROCEDURE_CODES, min(num_lines, len(PROCEDURE_CODES)))
        
        for line_num, proc in enumerate(selected_procedures, 1):
            proc_code, proc_desc, min_cost, max_cost = proc
            billed_amount = round(random.uniform(min_cost * 1.2, max_cost), 2)
            allowed_amount = round(billed_amount * random.uniform(0.6, 0.9), 2)
            
            if status == "PAID":
                paid_amount = round(allowed_amount * random.uniform(0.7, 0.95), 2)
            elif status == "DENIED":
                paid_amount = 0.0
            else:
                paid_amount = round(allowed_amount * random.uniform(0.3, 0.7), 2)
            
            quantity = random.choice([1, 1, 1, 2, 2, 3])
            
            line = {
                "line_id": f"{claim_id}-{line_num}",
                "claim_id": claim_id,
                "line_number": line_num,
                "procedure_code": proc_code,
                "procedure_description": proc_desc,
                "modifier": random.choice(["", "", "", "25", "59", "76"]),
                "quantity": quantity,
                "billed_amount": billed_amount * quantity,
                "allowed_amount": allowed_amount * quantity,
                "paid_amount": paid_amount * quantity,
                "service_from_date": service_date.strftime("%Y-%m-%d"),
                "service_to_date": service_date.strftime("%Y-%m-%d"),
                "place_of_service": "11" if provider_type == "Clinic" else "21",
            }
            lines.append(line)
            
            total_billed += line["billed_amount"]
            total_allowed += line["allowed_amount"]
            total_paid += line["paid_amount"]
        
        claim = {
            "claim_id": claim_id,
            "member_id": member_id,
            "payer_name": payer,
            "provider_id": provider_id,
            "provider_name": provider_name,
            "provider_type": provider_type,
            "service_date": service_date.strftime("%Y-%m-%d"),
            "submitted_date": submitted_date.strftime("%Y-%m-%d"),
            "primary_diagnosis_code": primary_dx[0],
            "primary_diagnosis_description": primary_dx[1],
            "secondary_diagnosis_code": secondary_dx[0] if secondary_dx else "",
            "secondary_diagnosis_description": secondary_dx[1] if secondary_dx else "",
            "total_billed_amount": round(total_billed, 2),
            "total_allowed_amount": round(total_allowed, 2),
            "total_paid_amount": round(total_paid, 2),
            "claim_status": status,
            "num_lines": num_lines,
        }
        claims.append(claim)
    
    return claims, lines

def write_csv(filename, data, fieldnames):
    with open(filename, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

if __name__ == "__main__":
    claims, lines = generate_claims()
    
    claim_fields = [
        "claim_id", "member_id", "payer_name", "provider_id", "provider_name",
        "provider_type", "service_date", "submitted_date", "primary_diagnosis_code",
        "primary_diagnosis_description", "secondary_diagnosis_code", "secondary_diagnosis_description",
        "total_billed_amount", "total_allowed_amount", "total_paid_amount", "claim_status", "num_lines"
    ]
    
    line_fields = [
        "line_id", "claim_id", "line_number", "procedure_code", "procedure_description",
        "modifier", "quantity", "billed_amount", "allowed_amount", "paid_amount",
        "service_from_date", "service_to_date", "place_of_service"
    ]
    
    write_csv("/Users/mgandhirajan/Documents/CoCo/HCLS/coco-healthcare-skills/claims_data/claims_header.csv", claims, claim_fields)
    write_csv("/Users/mgandhirajan/Documents/CoCo/HCLS/coco-healthcare-skills/claims_data/claims_lines.csv", lines, line_fields)
    
    print(f"Generated {len(claims)} claims and {len(lines)} claim lines")
