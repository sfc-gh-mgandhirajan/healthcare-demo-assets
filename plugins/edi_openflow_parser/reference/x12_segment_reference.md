# X12 Segment Quick Reference

Common X12 segments and their meaning. Use this when building field maps for new transaction types.

## Entity Identification Segments

| Segment | Purpose | Key Elements |
|---------|---------|--------------|
| NM1 | Name | 01=entity ID code, 02=entity type (1=person/2=org), 03=last name, 04=first name, 08=ID qualifier, 09=ID |
| N3 | Address | 01=line 1, 02=line 2 |
| N4 | Geographic | 01=city, 02=state, 03=zip, 04=country |
| PER | Contact | 01=contact function, 02=name, 03=comm qualifier, 04=comm number |
| DMG | Demographics | 01=date format, 02=DOB, 03=gender, 04=marital status |

## NM1 Entity Identifier Codes (Qualifier in position 01)

| Code | Entity |
|------|--------|
| 85 | Billing Provider |
| 87 | Pay-To Provider |
| 82 | Rendering Provider |
| IL | Insured/Subscriber |
| QC | Patient |
| PR | Payer |
| DN | Referring Provider |
| 1P | Provider |
| 41 | Submitter |
| 40 | Receiver |
| 77 | Service Location |
| TT | Crossover Carrier |
| 70 | Prior Name |
| 03 | Dependent |

## Transaction-Specific Segments

| Segment | Transaction | Purpose | Key Elements |
|---------|-------------|---------|--------------|
| CLM | 837 | Claim header | 01=claim ID, 02=amount, 05=POS |
| CLP | 835 | Claim payment | 01=claim ID, 03=charge, 04=payment |
| INS | 834 | Enrollment | 01=benefit status, 02=relationship |
| HL | 270/271/276/277/278 | Hierarchy | 01=ID, 02=parent, 03=level, 04=child |
| SV1 | 837 | Service line (professional) | 01=procedure, 02=charge, 04=units |
| SV2 | 837 | Service line (institutional) | 01=revenue code, 02=procedure |
| SVC | 835 | Service payment | 01=procedure, 02=charge, 03=payment |
| EB | 271 | Eligibility/benefit | 01=info code, 03=service type, 07=amount |
| EQ | 270 | Eligibility inquiry | 01=service type |
| UM | 278 | Health care review | 01=request type, 02=certification type |
| HCR | 278 | Review decision | 01=action code, 02=review ID |
| STC | 277 | Status information | 01=status code, 03=action, 04=amount |

## Date/Time Segments

| Segment | Qualifier | Meaning |
|---------|-----------|---------|
| DTP_472 | 472 | Service date |
| DTP_431 | 431 | Onset of current illness |
| DTP_435 | 435 | Admission date |
| DTP_096 | 096 | Discharge date |
| DTP_291 | 291 | Plan begin |
| DTP_307 | 307 | Eligibility date |
| DTP_336 | 336 | Employment date |
| DTP_348 | 348 | Coverage start |
| DTP_349 | 349 | Coverage end |
| DTP_303 | 303 | Maintenance effective |
| DTM_232 | 232 | Claim statement period start |
| DTM_233 | 233 | Claim statement period end |
| DTM_036 | 036 | Expiration date |

## Reference Segments

| Segment | Qualifier | Meaning |
|---------|-----------|---------|
| REF_EA | EA | Patient account number |
| REF_D9 | D9 | Claim reference ID |
| REF_1K | 1K | Payer claim number |
| REF_BLT | BLT | Batch number |
| REF_6R | 6R | Subscriber ID |

## Financial Segments

| Segment | Purpose | Key Elements |
|---------|---------|--------------|
| CAS | Claim adjustment | 01=group code, 02=reason, 03=amount |
| AMT | Monetary amount | 01=qualifier, 02=amount |
| AMT_AU | AU | Allowed amount |
| AMT_D8 | D8 | Discount amount |
| AMT_T3 | T3 | Total claim charge |

## Record Boundary Segments (per transaction type)

| Transaction | Boundary | Meaning |
|-------------|----------|---------|
| 837 | CLM | Each CLM starts a new claim |
| 835 | CLP | Each CLP starts a new remittance |
| 834 | INS | Each INS starts a new member |
| 270/271 | HL | Each HL starts a new hierarchy level |
| 276/277 | HL | Each HL starts a new status request/response |
| 278 | HL | Each HL starts a new auth request/response |
| 820 | ENT | Each ENT starts a new payment entity |

## X12 Delimiters (from ISA segment)

| Position | Default | Purpose |
|----------|---------|---------|
| ISA[3] | `*` | Element separator |
| ISA[104] | `:` | Sub-element (composite) separator |
| ISA[105] | `~` | Segment terminator |

The ISA segment is ALWAYS exactly 106 characters (fixed width, space-padded).
