import json
import random
import string
import os
from datetime import datetime, timedelta
from decimal import Decimal

random.seed(42)

NUM_PROVIDERS = 500
NUM_BENEFICIARIES = 30000
NUM_CLAIMS = 50000
ERROR_RATE = 0.15

VALID_DRG_CODES = [str(i).zfill(3) for i in range(1, 999)]
MATERNITY_DRGS = ['765', '766', '767', '768', '774', '775']
PEDS_DRGS = ['789', '790', '791', '793', '794', '795']
VALID_REVENUE_CODES = ['0100', '0110', '0120', '0130', '0150', '0200', '0210', '0250', '0260',
                       '0270', '0280', '0300', '0301', '0310', '0320', '0350', '0360', '0370',
                       '0400', '0410', '0420', '0450', '0460', '0500', '0510', '0520', '0530',
                       '0540', '0550', '0600', '0610', '0620', '0630', '0700', '0710', '0720',
                       '0730', '0800', '0801', '0900', '0910', '0920', '0940', '0960', '0001']
VALID_HCPCS = ['99213', '99214', '99215', '99222', '99223', '99231', '99232', '99233',
               '99238', '99239', '99281', '99282', '99283', '99284', '99285',
               '36415', '36416', '70553', '71046', '71250', '72148', '73721',
               '74177', '76856', '80053', '85025', '93000', '93306', '94640',
               '96372', '96374', '97110', '97140', '97530', '99291', '99292',
               'G0008', 'G0009', 'G0010', 'G0101', 'G0102', 'G0103', 'G0104',
               'J0129', 'J1100', 'J1644', 'J1745', 'J2001', 'J3301', 'J7040']

FACILITY_TYPE_CODES = ['1', '2', '3', '4', '5', '6', '7', '8']
CLAIM_FREQ_CODES = ['1', '7', '8']
PATIENT_STATUS_CODES = ['01', '02', '03', '04', '05', '06', '07', '20', '30', '43', '50', '51', '61', '62', '63', '65', '66']
ADMISSION_SOURCE_CODES = ['1', '2', '3', '4', '5', '6', '7', '8', '9']
TOB_CODES = ['111', '112', '113', '114', '117', '118', '121', '122', '131', '141', '211', '212', '721', '731']

MBI_CHARS = 'ACDEFGHJKMNPQRTUVWXY'
MBI_ALPHANUM = MBI_CHARS + '0123456789'

def generate_mbi():
    return (random.choice('123456789') +
            random.choice(MBI_CHARS) +
            random.choice(MBI_ALPHANUM) +
            random.choice('0123456789') +
            random.choice(MBI_CHARS) +
            random.choice(MBI_ALPHANUM) +
            random.choice('0123456789') +
            random.choice(MBI_CHARS) +
            random.choice(MBI_CHARS) +
            random.choice('0123456789') +
            random.choice('0123456789'))

def generate_npi():
    return ''.join([random.choice('0123456789') for _ in range(10)])

def generate_icd10():
    letter = random.choice('ABCDEFGHJKLMNPQRST')
    num = random.randint(0, 99)
    suffix = ''.join([random.choice('0123456789') for _ in range(random.randint(0, 4))])
    return f"{letter}{num:02d}" + (f".{suffix}" if suffix else "")

def random_date(start, end):
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, max(delta, 1)))

providers = []
statuses = ['Active'] * 400 + ['Inactive'] * 60 + ['Revoked'] * 40
random.shuffle(statuses)
for i in range(NUM_PROVIDERS):
    npi = generate_npi()
    eff_date = random_date(datetime(2015, 1, 1), datetime(2023, 12, 31))
    providers.append({
        'PROVIDER_NPI': npi,
        'PROVIDER_NAME': f'Provider_{i+1}',
        'PROVIDER_TYPE': random.choice(['Hospital', 'SNF', 'HHA', 'Hospice', 'LTCH', 'IRF', 'IPF']),
        'PECOS_STATUS': statuses[i],
        'PECOS_EFFECTIVE_DATE': eff_date.strftime('%Y-%m-%d'),
        'STATE': random.choice(['CA', 'TX', 'FL', 'NY', 'PA', 'IL', 'OH', 'GA', 'NC', 'MI',
                                'NJ', 'VA', 'WA', 'AZ', 'MA', 'TN', 'IN', 'MO', 'MD', 'WI']),
        'PROVIDER_SPECIALTY': random.choice(['General', 'Cardiology', 'Orthopedics', 'Oncology', 'Neurology', 'Pulmonology'])
    })

beneficiaries = []
for i in range(NUM_BENEFICIARIES):
    hic = generate_mbi()
    dob = random_date(datetime(1930, 1, 1), datetime(2006, 12, 31))
    sex = random.choices(['M', 'F'], weights=[48, 52])[0]
    part_a_start = random_date(datetime(2018, 1, 1), datetime(2024, 6, 1))
    status = random.choices(['Active', 'Terminated'], weights=[95, 5])[0]
    part_a_end = None
    if status == 'Terminated':
        part_a_end = random_date(part_a_start, datetime(2025, 12, 31))
    beneficiaries.append({
        'BENEFICIARY_HIC': hic,
        'FIRST_NAME': f'First_{i+1}',
        'LAST_NAME': f'Last_{i+1}',
        'DATE_OF_BIRTH': dob.strftime('%Y-%m-%d'),
        'PATIENT_SEX': sex,
        'PART_A_STATUS': status,
        'PART_A_EFFECTIVE_DATE': part_a_start.strftime('%Y-%m-%d'),
        'PART_A_TERMINATION_DATE': part_a_end.strftime('%Y-%m-%d') if part_a_end else None,
        'STATE': random.choice(['CA', 'TX', 'FL', 'NY', 'PA', 'IL', 'OH', 'GA', 'NC', 'MI'])
    })

active_providers = [p for p in providers if p['PECOS_STATUS'] == 'Active']
active_beneficiaries = [b for b in beneficiaries if b['PART_A_STATUS'] == 'Active']

claim_headers = []
service_lines = []
line_id_counter = 1

error_types = [
    'invalid_npi', 'invalid_mbi', 'invalid_tob', 'discharge_before_admission',
    'future_date', 'invalid_icd10', 'charges_out_of_range', 'null_fields',
    'duplicate_claim', 'inactive_provider', 'terminated_beneficiary',
    'mue_violation', 'ncci_violation', 'timely_filing', 'drg_sex_mismatch',
    'financial_imbalance', 'missing_admission_source'
]
num_error_claims = int(NUM_CLAIMS * ERROR_RATE)
error_claim_indices = set(random.sample(range(NUM_CLAIMS), num_error_claims))

duplicate_source_claims = []

for i in range(NUM_CLAIMS):
    is_error = i in error_claim_indices
    error_type = random.choice(error_types) if is_error else None

    claim_id = f'CLM{i+1:07d}'
    dcn = f'DCN{random.randint(100000000, 999999999)}'

    if error_type == 'inactive_provider':
        prov = random.choice([p for p in providers if p['PECOS_STATUS'] != 'Active'])
    else:
        prov = random.choice(active_providers)

    if error_type == 'terminated_beneficiary':
        bene = random.choice([b for b in beneficiaries if b['PART_A_STATUS'] == 'Terminated'])
    else:
        bene = random.choice(active_beneficiaries)

    provider_npi = prov['PROVIDER_NPI']
    beneficiary_hic = bene['BENEFICIARY_HIC']
    patient_sex = bene['PATIENT_SEX']

    admission_date = random_date(datetime(2024, 1, 1), datetime(2025, 11, 30))
    los = random.randint(1, 14)
    discharge_date = admission_date + timedelta(days=los)
    submission_date = discharge_date + timedelta(days=random.randint(5, 60))

    drg_code = random.choice(VALID_DRG_CODES)
    principal_dx = generate_icd10()
    total_charges = round(random.uniform(500, 150000), 2)
    tob = random.choice(TOB_CODES)
    facility_type = tob[0]
    freq_code = random.choice(CLAIM_FREQ_CODES)
    patient_status = random.choice(PATIENT_STATUS_CODES)
    admission_source = random.choice(ADMISSION_SOURCE_CODES) if tob.startswith('1') else None

    if error_type == 'invalid_npi':
        provider_npi = 'ABC' + provider_npi[3:]
    elif error_type == 'invalid_mbi':
        beneficiary_hic = '0XX' + beneficiary_hic[3:]
    elif error_type == 'invalid_tob':
        tob = 'XYZ'
    elif error_type == 'discharge_before_admission':
        discharge_date = admission_date - timedelta(days=random.randint(1, 5))
    elif error_type == 'future_date':
        admission_date = datetime(2027, 6, 15)
        discharge_date = datetime(2027, 6, 20)
    elif error_type == 'invalid_icd10':
        principal_dx = '123INVALID'
    elif error_type == 'charges_out_of_range':
        total_charges = random.choice([-500.0, 0.0, 15000000.0])
    elif error_type == 'null_fields':
        null_field = random.choice(['npi', 'hic', 'admission', 'drg'])
        if null_field == 'npi': provider_npi = None
        elif null_field == 'hic': beneficiary_hic = None
        elif null_field == 'admission': admission_date = None
        elif null_field == 'drg': drg_code = None
    elif error_type == 'duplicate_claim' and duplicate_source_claims:
        src = random.choice(duplicate_source_claims)
        provider_npi = src['PROVIDER_NPI']
        beneficiary_hic = src['BENEFICIARY_HIC']
        admission_date = datetime.strptime(src['ADMISSION_DATE'], '%Y-%m-%d') if src['ADMISSION_DATE'] else admission_date
        total_charges = src['TOTAL_CHARGES']
    elif error_type == 'timely_filing':
        admission_date = random_date(datetime(2022, 1, 1), datetime(2022, 12, 31))
        discharge_date = admission_date + timedelta(days=los)
        submission_date = discharge_date + timedelta(days=random.randint(400, 600))
    elif error_type == 'drg_sex_mismatch':
        drg_code = random.choice(MATERNITY_DRGS)
        patient_sex = 'M'
    elif error_type == 'financial_imbalance':
        pass
    elif error_type == 'missing_admission_source':
        tob = '111'
        admission_source = None

    header = {
        'CLAIM_ID': claim_id,
        'DCN': dcn,
        'BENEFICIARY_HIC': beneficiary_hic,
        'PROVIDER_NPI': provider_npi,
        'FACILITY_TYPE_CODE': facility_type,
        'CLAIM_FREQUENCY_CODE': freq_code,
        'ADMISSION_DATE': admission_date.strftime('%Y-%m-%d') if admission_date else None,
        'DISCHARGE_DATE': discharge_date.strftime('%Y-%m-%d') if discharge_date else None,
        'PATIENT_STATUS_CODE': patient_status,
        'DRG_CODE': drg_code,
        'PRINCIPAL_DIAGNOSIS_CODE': principal_dx,
        'TOTAL_CHARGES': total_charges,
        'TYPE_OF_BILL': tob,
        'CLAIM_SUBMISSION_DATE': submission_date.strftime('%Y-%m-%d'),
        'ADMISSION_SOURCE_CODE': admission_source,
        'PATIENT_SEX': patient_sex
    }
    claim_headers.append(header)

    if i < 200:
        duplicate_source_claims.append(header)

    num_lines = random.randint(1, 8)
    line_charges_list = []
    for j in range(num_lines):
        rev_code = random.choice(VALID_REVENUE_CODES)
        hcpcs = random.choice(VALID_HCPCS)
        units = random.randint(1, 10)
        line_charge = round(random.uniform(50, 25000), 2)
        line_charges_list.append(line_charge)

        if is_error and error_type == 'mue_violation' and j == 0:
            units = random.randint(50, 200)

        service_line = {
            'LINE_ID': f'LN{line_id_counter:08d}',
            'CLAIM_ID': claim_id,
            'LINE_NUMBER': j + 1,
            'REVENUE_CODE': rev_code,
            'HCPCS_CODE': hcpcs,
            'MODIFIER_1': random.choice([None, '26', '59', 'TC', 'LT', 'RT']),
            'MODIFIER_2': None,
            'SERVICE_DATE': (admission_date if admission_date else datetime(2024, 6, 1)).strftime('%Y-%m-%d'),
            'UNITS': units,
            'LINE_CHARGES': line_charge
        }
        line_id_counter += 1
        service_lines.append(service_line)

    if is_error and error_type == 'financial_imbalance':
        pass
    elif not is_error:
        header['TOTAL_CHARGES'] = round(sum(line_charges_list), 2)

    if is_error and error_type == 'ncci_violation' and num_lines >= 2:
        service_lines[-1]['HCPCS_CODE'] = '93000'
        service_lines[-2]['HCPCS_CODE'] = '93005'

mue_limits = []
for hcpcs in VALID_HCPCS[:50]:
    mue_limits.append({
        'HCPCS_CODE': hcpcs,
        'MAX_UNITS_PER_DAY': random.choice([1, 2, 3, 4, 5, 6, 8, 10, 12]),
        'MUE_RATIONALE': random.choice(['Clinical', 'CMS Policy', 'Nature of Service', 'Data']),
        'EFFECTIVE_DATE': '2024-01-01',
        'TERMINATION_DATE': None
    })

ncci_pairs = []
pair_set = set()
for _ in range(500):
    c1 = random.choice(VALID_HCPCS)
    c2 = random.choice(VALID_HCPCS)
    if c1 != c2 and (c1, c2) not in pair_set:
        pair_set.add((c1, c2))
        ncci_pairs.append({
            'COLUMN_1_CODE': c1,
            'COLUMN_2_CODE': c2,
            'MODIFIER_INDICATOR': random.choice(['0', '1', '9']),
            'EFFECTIVE_DATE': '2024-01-01',
            'TERMINATION_DATE': None
        })
ncci_pairs.append({'COLUMN_1_CODE': '93000', 'COLUMN_2_CODE': '93005',
                   'MODIFIER_INDICATOR': '0', 'EFFECTIVE_DATE': '2024-01-01', 'TERMINATION_DATE': None})

freq_limits = []
for hcpcs in random.sample(VALID_HCPCS, min(100, len(VALID_HCPCS))):
    freq_limits.append({
        'HCPCS_CODE': hcpcs,
        'MAX_PER_PERIOD': random.choice([1, 2, 4, 6, 12]),
        'PERIOD_DAYS': random.choice([30, 60, 90, 180, 365]),
        'EFFECTIVE_DATE': '2024-01-01',
        'TERMINATION_DATE': None
    })

OUTPUT_DIR = os.path.expanduser('~/documents/CoCo/HCLS/healthcare-demo-assets/synthetic_data')
os.makedirs(OUTPUT_DIR, exist_ok=True)

datasets = {
    'claim_headers.json': claim_headers,
    'service_lines.json': service_lines,
    'providers.json': providers,
    'beneficiaries.json': beneficiaries,
    'mue_limits.json': mue_limits,
    'ncci_code_pairs.json': ncci_pairs,
    'frequency_limits.json': freq_limits,
}

for filename, data in datasets.items():
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, 'w') as f:
        for record in data:
            f.write(json.dumps(record, default=str) + '\n')
    print(f"  {filename}: {len(data)} records")

print(f"\nTotal claims: {len(claim_headers)}")
print(f"Total service lines: {len(service_lines)}")
print(f"Error claims: {num_error_claims} ({ERROR_RATE*100:.0f}%)")
print(f"Output: {OUTPUT_DIR}")
