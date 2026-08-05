import { useState } from "react";
import type { PatientDetail, MedicalImage } from "../types";

const modalityIcons: Record<string, string> = {
  ECG: "M4 16l4.586-4.586a2 2 0 012.828 0L16 16m-2-2l1.586-1.586a2 2 0 012.828 0L20 14",
  "Chest X-Ray": "M9 3v2m6-2v2M9 19v2m6-2v2M5 9H3m2 6H3m18-6h-2m2 6h-2M7 19h10a2 2 0 002-2V7a2 2 0 00-2-2H7a2 2 0 00-2 2v10a2 2 0 002 2z",
  Echocardiogram: "M4.318 6.318a4.5 4.5 0 000 6.364L12 20.364l7.682-7.682a4.5 4.5 0 00-6.364-6.364L12 7.636l-1.318-1.318a4.5 4.5 0 00-6.364 0z",
  "Coronary Angiogram": "M19.428 15.428a2 2 0 00-1.022-.547l-2.387-.477a6 6 0 00-3.86.517l-.318.158a6 6 0 01-3.86.517L6.05 15.21a2 2 0 00-1.806.547M8 4h8l-1 1v5.172a2 2 0 00.586 1.414l5 5c1.26 1.26.367 3.414-1.415 3.414H4.828c-1.782 0-2.674-2.154-1.414-3.414l5-5A2 2 0 009 10.172V5L8 4z",
  "Holter Monitor": "M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z",
  "Cardiac MRI": "M21 12a9 9 0 11-18 0 9 9 0 0118 0z",
};

const modalityColors: Record<string, string> = {
  ECG: "text-cyan-400",
  "Chest X-Ray": "text-blue-400",
  Echocardiogram: "text-purple-400",
  "Coronary Angiogram": "text-rose-400",
  "Holter Monitor": "text-emerald-400",
  "Cardiac MRI": "text-amber-400",
};

interface Props {
  data: PatientDetail;
  isLoading: boolean;
  onAskAgent: (prompt: string) => void;
}

export default function PatientDetailPanel({ data, isLoading, onAskAgent }: Props) {
  const [selectedImage, setSelectedImage] = useState<MedicalImage | null>(null);
  const d = data.demographics;
  const latestVital = data.vitals[0];

  const imagesByModality: Record<string, MedicalImage[]> = {};
  for (const img of data.medicalImages) {
    if (!imagesByModality[img.modality]) imagesByModality[img.modality] = [];
    imagesByModality[img.modality].push(img);
  }

  if (isLoading) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="flex items-center gap-3 text-text-muted">
          <svg className="w-5 h-5 animate-spin" fill="none" viewBox="0 0 24 24">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
          </svg>
          <span className="text-sm">Loading patient record...</span>
        </div>
      </div>
    );
  }

  return (
    <div className="flex-1 overflow-y-auto">
      <div className="px-6 py-4 bg-navy-800 border-b border-card-border sticky top-0 z-10">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            <div className="w-12 h-12 rounded-xl bg-cyan-400/10 border border-cyan-400/20 flex items-center justify-center text-lg font-bold text-cyan-400">
              {d.firstName[0]}{d.lastName[0]}
            </div>
            <div>
              <h2 className="text-lg font-bold text-text-primary">{d.firstName} {d.lastName}</h2>
              <div className="flex items-center gap-3 text-xs text-text-muted">
                <span>{d.patientId}</span>
                <span>·</span>
                <span>DOB: {d.dob?.split("T")[0] || d.dob}</span>
                <span>·</span>
                <span>{d.gender}</span>
                <span>·</span>
                <span>Blood: {d.bloodType}</span>
                <span>·</span>
                <span>{d.primaryPhysician}</span>
              </div>
            </div>
          </div>

        </div>
      </div>

      <div className="p-6 space-y-6">
        {latestVital && (
          <section>
            <SectionHeader icon="M4.318 6.318a4.5 4.5 0 000 6.364L12 20.364l7.682-7.682a4.5 4.5 0 00-6.364-6.364L12 7.636l-1.318-1.318a4.5 4.5 0 00-6.364 0z" title="Latest Vitals" subtitle={latestVital.recordedDate.split("T")[0] || latestVital.recordedDate.split(" ")[0]} />
            <div className="grid grid-cols-6 gap-3 mt-3">
              <VitalCard label="Heart Rate" value={`${latestVital.heartRate}`} unit="bpm" color={latestVital.heartRate > 100 ? "text-red-400" : latestVital.heartRate < 60 ? "text-amber-400" : "text-emerald-400"} />
              <VitalCard label="Blood Pressure" value={`${latestVital.systolicBp}/${latestVital.diastolicBp}`} unit="mmHg" color={latestVital.systolicBp > 140 ? "text-red-400" : "text-emerald-400"} />
              <VitalCard label="SpO2" value={`${latestVital.spo2}`} unit="%" color={latestVital.spo2 < 95 ? "text-red-400" : "text-emerald-400"} />
              <VitalCard label="Resp Rate" value={`${latestVital.respiratoryRate}`} unit="/min" color={latestVital.respiratoryRate > 20 ? "text-amber-400" : "text-emerald-400"} />
              <VitalCard label="Temp" value={`${latestVital.temperatureF}`} unit="°F" color={latestVital.temperatureF > 100.4 ? "text-red-400" : "text-emerald-400"} />
              <VitalCard label="BMI" value={`${d.bmi}`} unit="" color="text-text-primary" />
            </div>
          </section>
        )}

        <section>
          <SectionHeader icon="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" title="Active Conditions" subtitle={`${data.conditions.length} conditions`} />
          <div className="mt-3 grid grid-cols-2 gap-2">
            {data.conditions.map((c) => (
              <div key={c.id} className="rounded-lg bg-navy-800 border border-card-border p-3">
                <div className="flex items-start justify-between">
                  <div className="min-w-0 flex-1">
                    <p className="text-xs font-medium text-text-primary leading-snug">{c.name}</p>
                    <p className="text-[10px] text-text-muted mt-1">ICD-10: {c.code} · Onset: {c.onsetDate}</p>
                  </div>
                  <span className={`ml-2 flex-shrink-0 text-[10px] px-1.5 py-0.5 rounded font-medium ${
                    c.status === "active" ? "bg-red-500/10 text-red-400" :
                    c.status === "chronic" ? "bg-amber-500/10 text-amber-400" :
                    "bg-emerald-500/10 text-emerald-400"
                  }`}>
                    {c.severity}
                  </span>
                </div>
              </div>
            ))}
          </div>
        </section>

        <section>
          <SectionHeader icon="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10" title="Active Medications" subtitle={`${data.medications.length} medications`} />
          <div className="mt-3 overflow-hidden rounded-lg border border-card-border">
            <table className="w-full text-xs">
              <thead>
                <tr className="bg-navy-700">
                  <th className="px-3 py-2 text-left text-[10px] font-semibold uppercase tracking-wider text-cyan-400">Medication</th>
                  <th className="px-3 py-2 text-left text-[10px] font-semibold uppercase tracking-wider text-cyan-400">Dosage</th>
                  <th className="px-3 py-2 text-left text-[10px] font-semibold uppercase tracking-wider text-cyan-400">Frequency</th>
                  <th className="px-3 py-2 text-left text-[10px] font-semibold uppercase tracking-wider text-cyan-400">Route</th>
                  <th className="px-3 py-2 text-left text-[10px] font-semibold uppercase tracking-wider text-cyan-400">Class</th>
                </tr>
              </thead>
              <tbody>
                {data.medications.map((m) => (
                  <tr key={m.id} className="border-t border-card-border hover:bg-navy-800 transition-colors">
                    <td className="px-3 py-2 text-text-primary font-medium">{m.name}</td>
                    <td className="px-3 py-2 text-text-secondary">{m.dosage}</td>
                    <td className="px-3 py-2 text-text-secondary">{m.frequency}</td>
                    <td className="px-3 py-2 text-text-secondary">{m.route}</td>
                    <td className="px-3 py-2"><span className="px-1.5 py-0.5 rounded bg-navy-600 text-text-muted text-[10px]">{m.medicationClass}</span></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>

        <section>
          <SectionHeader icon="M4 16l4.586-4.586a2 2 0 012.828 0L16 16m-2-2l1.586-1.586a2 2 0 012.828 0L20 14m-6-6h.01M6 20h12a2 2 0 002-2V6a2 2 0 00-2-2H6a2 2 0 00-2 2v12a2 2 0 002 2z" title="Medical Images" subtitle={`${data.medicalImages.length} studies across ${Object.keys(imagesByModality).length} modalities`} />
          <div className="mt-3 grid grid-cols-3 gap-2">
            {data.medicalImages.map((img) => (
              <button
                key={img.imageId}
                onClick={() => setSelectedImage(img)}
                className="group rounded-lg border border-card-border bg-navy-800 overflow-hidden hover:border-cyan-400/30 transition-all"
              >
                <div className="aspect-[4/3] bg-black/50 relative overflow-hidden">
                  {img.presignedUrl ? (
                    <img
                      src={img.presignedUrl}
                      alt={img.description}
                      className="w-full h-full object-cover opacity-90 group-hover:opacity-100 transition-opacity"
                      onError={(e) => { (e.target as HTMLImageElement).style.display = "none"; }}
                    />
                  ) : (
                    <div className="w-full h-full flex items-center justify-center text-text-muted">
                      <svg className="w-8 h-8" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d={modalityIcons[img.modality] || modalityIcons.ECG} />
                      </svg>
                    </div>
                  )}
                  <div className="absolute top-1 left-1 px-1.5 py-0.5 rounded bg-black/60 flex items-center gap-1">
                    <svg className={`w-3 h-3 ${modalityColors[img.modality] || "text-text-muted"}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d={modalityIcons[img.modality] || modalityIcons.ECG} />
                    </svg>
                    <span className="text-[9px] text-white font-medium">{img.modality}</span>
                  </div>
                  {img.status === "pending_review" && (
                    <div className="absolute top-1 right-1 px-1.5 py-0.5 rounded bg-amber-500/20 text-amber-400 text-[9px] font-medium">
                      PENDING
                    </div>
                  )}
                </div>
                <div className="px-2 py-1.5">
                  <p className="text-[10px] text-text-secondary truncate">{img.description}</p>
                  <p className="text-[9px] text-text-muted">{img.imageDate.split("T")[0] || img.imageDate.split(" ")[0]}</p>
                </div>
              </button>
            ))}
          </div>
        </section>

        <section>
          <SectionHeader icon="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" title="Recent Encounters" subtitle={`${data.encounters.length} encounters`} />
          <div className="mt-3 space-y-2">
            {data.encounters.slice(0, 4).map((e) => (
              <div key={e.id} className="rounded-lg bg-navy-800 border border-card-border p-3">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <span className={`px-1.5 py-0.5 rounded text-[10px] font-medium ${
                      e.type === "Emergency" ? "bg-red-500/10 text-red-400" :
                      e.type === "Inpatient" ? "bg-amber-500/10 text-amber-400" :
                      e.type.includes("Discharge") ? "bg-emerald-500/10 text-emerald-400" :
                      "bg-blue-500/10 text-blue-400"
                    }`}>{e.type}</span>
                    <span className="text-xs text-text-primary font-medium">{e.department}</span>
                  </div>
                  <span className="text-[10px] text-text-muted">{e.date}</span>
                </div>
                <p className="text-[11px] text-text-secondary mt-1.5">{e.chiefComplaint}</p>
                {e.notes && <p className="text-[10px] text-text-muted mt-1 leading-relaxed">{e.notes}</p>}
              </div>
            ))}
          </div>
        </section>
      </div>

      {selectedImage && (
        <ImageModal
          image={selectedImage}
          onClose={() => setSelectedImage(null)}
          onInterpret={(img) => {
            setSelectedImage(null);
            onAskAgent(
              `Interpret the ${img.modality} image ${img.imageId} for patient ${img.patientId}. Description: ${img.description}. Clinical indication: ${img.clinicalIndication}.${img.findings ? ` Previous findings: ${img.findings}.` : ""}`
            );
          }}
        />
      )}
    </div>
  );
}

function SectionHeader({ icon, title, subtitle }: { icon: string; title: string; subtitle: string }) {
  return (
    <div className="flex items-center gap-2">
      <svg className="w-4 h-4 text-cyan-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d={icon} />
      </svg>
      <h3 className="text-sm font-semibold text-text-primary">{title}</h3>
      <span className="text-[10px] text-text-muted">{subtitle}</span>
    </div>
  );
}

function VitalCard({ label, value, unit, color }: { label: string; value: string; unit: string; color: string }) {
  return (
    <div className="rounded-lg bg-navy-800 border border-card-border p-3 text-center">
      <p className="text-[10px] text-text-muted uppercase tracking-wider">{label}</p>
      <p className={`text-lg font-bold mt-1 ${color}`}>{value}</p>
      <p className="text-[10px] text-text-muted">{unit}</p>
    </div>
  );
}

function ImageModal({ image, onClose, onInterpret }: { image: MedicalImage; onClose: () => void; onInterpret: (img: MedicalImage) => void }) {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm" onClick={onClose}>
      <div className="bg-navy-800 border border-card-border rounded-xl max-w-3xl w-full mx-4 max-h-[90vh] overflow-y-auto" onClick={(e) => e.stopPropagation()}>
        <div className="px-5 py-3 border-b border-card-border flex items-center justify-between">
          <div>
            <h3 className="text-sm font-semibold text-text-primary">{image.description}</h3>
            <p className="text-[11px] text-text-muted mt-0.5">{image.modality} · {image.imageDate.split("T")[0] || image.imageDate.split(" ")[0]} · {image.orderingPhysician}</p>
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={() => onInterpret(image)}
              className="px-3 py-1.5 rounded-lg bg-cyan-400/10 border border-cyan-400/20 text-cyan-400 text-xs font-medium hover:bg-cyan-400/20 transition-colors flex items-center gap-1.5"
            >
              <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" />
              </svg>
              AI Interpret
            </button>
            <button onClick={onClose} className="p-1.5 rounded-lg hover:bg-navy-700 text-text-muted transition-colors">
              <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          </div>
        </div>

        {image.presignedUrl && (
          <div className="p-4">
            <div className="rounded-lg overflow-hidden bg-black">
              <img src={image.presignedUrl} alt={image.description} className="w-full max-h-[50vh] object-contain" />
            </div>
          </div>
        )}

        <div className="px-5 py-4 border-t border-card-border space-y-3">
          <div className="grid grid-cols-2 gap-3">
            <div>
              <p className="text-[10px] text-text-muted uppercase tracking-wider">Clinical Indication</p>
              <p className="text-xs text-text-primary mt-0.5">{image.clinicalIndication}</p>
            </div>
            <div>
              <p className="text-[10px] text-text-muted uppercase tracking-wider">Status</p>
              <p className={`text-xs mt-0.5 font-medium ${image.status === "reviewed" ? "text-emerald-400" : "text-amber-400"}`}>
                {image.status === "reviewed" ? "Reviewed" : "Pending Review"}
              </p>
            </div>
          </div>
          {image.findings && (
            <div>
              <p className="text-[10px] text-text-muted uppercase tracking-wider">Findings</p>
              <p className="text-xs text-text-primary mt-0.5 leading-relaxed">{image.findings}</p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
