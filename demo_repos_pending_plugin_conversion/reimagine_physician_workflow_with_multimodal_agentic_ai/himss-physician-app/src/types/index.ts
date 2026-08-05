export interface Patient {
  id: string;
  name: string;
  age: number;
  gender: string;
  primaryCondition: string;
  status: "stable" | "critical" | "monitoring";
  physician: string;
  imageCount: number;
}

export interface PatientDetail {
  demographics: {
    patientId: string;
    firstName: string;
    lastName: string;
    dob: string;
    gender: string;
    race: string;
    ethnicity: string;
    address: string;
    city: string;
    state: string;
    zip: string;
    phone: string;
    insurance: string;
    primaryPhysician: string;
    bloodType: string;
    heightCm: number;
    bmi: number;
  };
  conditions: {
    id: string;
    code: string;
    name: string;
    onsetDate: string;
    status: string;
    severity: string;
  }[];
  medications: {
    id: string;
    name: string;
    dosage: string;
    frequency: string;
    route: string;
    status: string;
    medicationClass: string;
  }[];
  vitals: {
    id: string;
    recordedDate: string;
    heartRate: number;
    systolicBp: number;
    diastolicBp: number;
    respiratoryRate: number;
    temperatureF: number;
    spo2: number;
    notes: string;
  }[];
  encounters: {
    id: string;
    date: string;
    type: string;
    department: string;
    provider: string;
    chiefComplaint: string;
    diagnosisName: string;
    disposition: string;
    notes: string;
  }[];
  medicalImages: MedicalImage[];
}

export interface MedicalImage {
  imageId: string;
  patientId: string;
  modality: string;
  bodyPart: string;
  imageDate: string;
  imageFile: string;
  stagePath: string | null;
  description: string;
  orderingPhysician: string;
  clinicalIndication: string;
  status: string;
  findings: string | null;
  encounterId: string | null;
  presignedUrl?: string;
}

export interface ChatMessage {
  id: string;
  role: "user" | "assistant";
  content: string;
  imageUrl?: string;
  imageInterpretation?: ImageInterpretation;
  sqlResults?: SqlResult;
  toolCalls?: ToolCall[];
  isStreaming?: boolean;
  timestamp: Date;
}

export interface ImageInterpretation {
  imageId: string;
  patientName: string;
  modality: string;
  bodyPart: string;
  imageDate: string;
  description: string;
  indication: string;
  imageUrl: string;
  interpretation?: string;
  status: "success" | "error" | "pending";
}

export interface SqlResult {
  sql: string;
  columns: string[];
  rows: (string | number | null)[][];
}

export interface ToolCall {
  name: string;
  input?: Record<string, unknown>;
  output?: string;
  status?: "running" | "complete" | "error";
}

export interface SSEEvent {
  event: string;
  data: Record<string, unknown>;
}
