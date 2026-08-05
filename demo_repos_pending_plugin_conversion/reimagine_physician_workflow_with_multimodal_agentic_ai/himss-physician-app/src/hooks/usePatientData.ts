import { useState, useCallback } from "react";
import type { PatientDetail, MedicalImage } from "../types";

const TOKEN = import.meta.env.VITE_SNOWFLAKE_PAT as string | undefined;
const SQL_API = "/api/v2/statements";
const SF_DATABASE = import.meta.env.VITE_SNOWFLAKE_DATABASE as string || "DEMO_DB";
const SF_SCHEMA = import.meta.env.VITE_SNOWFLAKE_SCHEMA as string || "HIMSS_DEMO";
const SF_WAREHOUSE = import.meta.env.VITE_SNOWFLAKE_WAREHOUSE as string || "HIMSS_INTERACTIVE_WH";

interface UsePatientDataReturn {
  patientData: PatientDetail | null;
  isLoading: boolean;
  error: string | null;
  loadPatient: (patientId: string) => Promise<void>;
}

async function runSQL(sql: string, warehouse = SF_WAREHOUSE): Promise<{ columns: string[]; rows: (string | number | null)[][] }> {
  console.log("[runSQL] Executing:", sql.substring(0, 80));
  const resp = await fetch(SQL_API, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${TOKEN}`,
      "X-Snowflake-Authorization-Token-Type": "PROGRAMMATIC_ACCESS_TOKEN",
      Accept: "application/json",
    },
    body: JSON.stringify({
      statement: sql,
      warehouse,
      database: SF_DATABASE,
      schema: SF_SCHEMA,
      timeout: 60,
    }),
  });

  if (!resp.ok) {
    const txt = await resp.text();
    throw new Error(`SQL API ${resp.status}: ${txt}`);
  }

  const body = await resp.json();
  const meta = body.resultSetMetaData;
  const columns = (meta?.rowType as { name: string }[])?.map((c) => c.name) || [];
  const rows = (body.data as (string | number | null)[][]) || [];
  console.log("[runSQL] Success:", sql.substring(0, 40), "rows:", rows.length);
  return { columns, rows };
}

function colIdx(columns: string[], name: string): number {
  return columns.findIndex((c) => c === name);
}

function val(row: (string | number | null)[], columns: string[], name: string): string {
  const i = colIdx(columns, name);
  const raw = i >= 0 ? row[i] : null;
  if (raw == null) return "";
  const s = String(raw);
  if (/^-?\d{1,}(\.\d+)?$/.test(s) && (name.includes("DATE") || name.includes("RECORDED") || name.includes("IMAGE") || name.includes("START") || name.includes("ONSET") || name.includes("BIRTH") || name.includes("ENCOUNTER"))) {
    const num = parseFloat(s);
    if (Math.abs(num) < 100000) {
      const d = new Date(num * 86400000);
      if (!isNaN(d.getTime())) return d.toISOString().split("T")[0];
    } else if (Math.abs(num) < 3e10) {
      const d = new Date(num * 1000);
      if (!isNaN(d.getTime())) return d.toISOString().split("T")[0];
    }
  }
  return s;
}

function numVal(row: (string | number | null)[], columns: string[], name: string): number {
  const v = val(row, columns, name);
  return v ? Number(v) : 0;
}

export function usePatientData(): UsePatientDataReturn {
  const [patientData, setPatientData] = useState<PatientDetail | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const loadPatient = useCallback(async (patientId: string) => {
    if (!TOKEN) {
      setError("Missing VITE_SNOWFLAKE_PAT");
      return;
    }

    setIsLoading(true);
    setError(null);

    try {
      console.log("[usePatientData] loadPatient called for:", patientId, "TOKEN present:", !!TOKEN);
      const esc = patientId.replace(/'/g, "''");

      const [pRes, cRes, mRes, vRes, eRes, iRes] = await Promise.all([
        runSQL(`SELECT * FROM PATIENTS_IT WHERE PATIENT_ID = '${esc}'`),
        runSQL(`SELECT * FROM CONDITIONS_IT WHERE PATIENT_ID = '${esc}' ORDER BY ONSET_DATE DESC`),
        runSQL(`SELECT * FROM MEDICATIONS_IT WHERE PATIENT_ID = '${esc}' AND STATUS = 'active' ORDER BY MEDICATION_CLASS`),
        runSQL(`SELECT * FROM VITALS_IT WHERE PATIENT_ID = '${esc}' ORDER BY RECORDED_DATE DESC LIMIT 6`),
        runSQL(`SELECT * FROM ENCOUNTERS_IT WHERE PATIENT_ID = '${esc}' ORDER BY ENCOUNTER_DATE DESC`),
        runSQL(`SELECT * FROM MEDICAL_IMAGES_IT WHERE PATIENT_ID = '${esc}' ORDER BY IMAGE_DATE DESC`),
      ]);

      if (pRes.rows.length === 0) {
        setError(`Patient ${patientId} not found`);
        setIsLoading(false);
        return;
      }

      const p = pRes.rows[0];
      const pc = pRes.columns;

      const images: MedicalImage[] = iRes.rows.map((r) => ({
        imageId: val(r, iRes.columns, "IMAGE_ID"),
        patientId: val(r, iRes.columns, "PATIENT_ID"),
        modality: val(r, iRes.columns, "MODALITY"),
        bodyPart: val(r, iRes.columns, "BODY_PART"),
        imageDate: val(r, iRes.columns, "IMAGE_DATE"),
        imageFile: val(r, iRes.columns, "IMAGE_FILE"),
        stagePath: val(r, iRes.columns, "STAGE_PATH") || null,
        description: val(r, iRes.columns, "DESCRIPTION"),
        orderingPhysician: val(r, iRes.columns, "ORDERING_PHYSICIAN"),
        clinicalIndication: val(r, iRes.columns, "CLINICAL_INDICATION"),
        status: val(r, iRes.columns, "STATUS"),
        findings: val(r, iRes.columns, "FINDINGS") || null,
        encounterId: val(r, iRes.columns, "ENCOUNTER_ID") || null,
      }));

      for (const img of images) {
        img.presignedUrl = `/images/${img.imageFile}`;
      }

      const detail: PatientDetail = {
        demographics: {
          patientId: val(p, pc, "PATIENT_ID"),
          firstName: val(p, pc, "FIRST_NAME"),
          lastName: val(p, pc, "LAST_NAME"),
          dob: val(p, pc, "DATE_OF_BIRTH"),
          gender: val(p, pc, "GENDER"),
          race: val(p, pc, "RACE"),
          ethnicity: val(p, pc, "ETHNICITY"),
          address: val(p, pc, "ADDRESS"),
          city: val(p, pc, "CITY"),
          state: val(p, pc, "STATE"),
          zip: val(p, pc, "ZIP"),
          phone: val(p, pc, "PHONE"),
          insurance: val(p, pc, "INSURANCE_PROVIDER"),
          primaryPhysician: val(p, pc, "PRIMARY_PHYSICIAN"),
          bloodType: val(p, pc, "BLOOD_TYPE"),
          heightCm: numVal(p, pc, "HEIGHT_CM"),
          bmi: numVal(p, pc, "BMI"),
        },
        conditions: cRes.rows.map((r) => ({
          id: val(r, cRes.columns, "CONDITION_ID"),
          code: val(r, cRes.columns, "CONDITION_CODE"),
          name: val(r, cRes.columns, "CONDITION_NAME"),
          onsetDate: val(r, cRes.columns, "ONSET_DATE"),
          status: val(r, cRes.columns, "STATUS"),
          severity: val(r, cRes.columns, "SEVERITY"),
        })),
        medications: mRes.rows.map((r) => ({
          id: val(r, mRes.columns, "MEDICATION_ID"),
          name: val(r, mRes.columns, "MEDICATION_NAME"),
          dosage: val(r, mRes.columns, "DOSAGE"),
          frequency: val(r, mRes.columns, "FREQUENCY"),
          route: val(r, mRes.columns, "ROUTE"),
          status: val(r, mRes.columns, "STATUS"),
          medicationClass: val(r, mRes.columns, "MEDICATION_CLASS"),
        })),
        vitals: vRes.rows.map((r) => ({
          id: val(r, vRes.columns, "VITAL_ID"),
          recordedDate: val(r, vRes.columns, "RECORDED_DATE"),
          heartRate: numVal(r, vRes.columns, "HEART_RATE"),
          systolicBp: numVal(r, vRes.columns, "SYSTOLIC_BP"),
          diastolicBp: numVal(r, vRes.columns, "DIASTOLIC_BP"),
          respiratoryRate: numVal(r, vRes.columns, "RESPIRATORY_RATE"),
          temperatureF: numVal(r, vRes.columns, "TEMPERATURE_F"),
          spo2: numVal(r, vRes.columns, "SPO2"),
          notes: val(r, vRes.columns, "NOTES"),
        })),
        encounters: eRes.rows.map((r) => ({
          id: val(r, eRes.columns, "ENCOUNTER_ID"),
          date: val(r, eRes.columns, "ENCOUNTER_DATE"),
          type: val(r, eRes.columns, "ENCOUNTER_TYPE"),
          department: val(r, eRes.columns, "DEPARTMENT"),
          provider: val(r, eRes.columns, "PROVIDER_NAME"),
          chiefComplaint: val(r, eRes.columns, "CHIEF_COMPLAINT"),
          diagnosisName: val(r, eRes.columns, "DIAGNOSIS_NAME"),
          disposition: val(r, eRes.columns, "DISCHARGE_DISPOSITION"),
          notes: val(r, eRes.columns, "NOTES"),
        })),
        medicalImages: images,
      };

      console.log("[usePatientData] setPatientData called, demographics:", detail.demographics.patientId);
      setPatientData(detail);
    } catch (err) {
      console.error("[usePatientData] loadPatient error:", err);
      setError(err instanceof Error ? err.message : "Failed to load patient data");
    } finally {
      setIsLoading(false);
    }
  }, []);

  return { patientData, isLoading, error, loadPatient };
}
