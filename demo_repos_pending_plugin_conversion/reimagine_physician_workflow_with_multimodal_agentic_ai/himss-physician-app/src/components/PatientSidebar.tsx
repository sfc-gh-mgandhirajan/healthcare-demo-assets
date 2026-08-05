import type { Patient } from "../types";

const PATIENTS: Patient[] = [
  {
    id: "P-1001",
    name: "Maria Santos",
    age: 68,
    gender: "F",
    primaryCondition: "Post-MI Recovery (LAD STEMI)",
    status: "stable",
    physician: "Dr. Sarah Chen",
    imageCount: 3,
  },
  {
    id: "P-1002",
    name: "James Wilson",
    age: 72,
    gender: "M",
    primaryCondition: "Acute STEMI (RCA)",
    status: "critical",
    physician: "Dr. Sarah Chen",
    imageCount: 3,
  },
  {
    id: "P-1003",
    name: "Aisha Rahman",
    age: 57,
    gender: "F",
    primaryCondition: "Supraventricular Tachycardia",
    status: "monitoring",
    physician: "Dr. Michael Torres",
    imageCount: 3,
  },
];

const statusConfig = {
  stable: { bg: "bg-emerald-500/10", text: "text-emerald-400", dot: "bg-emerald-400", label: "Stable" },
  critical: { bg: "bg-red-500/10", text: "text-red-400", dot: "bg-red-400", label: "Critical" },
  monitoring: { bg: "bg-amber-500/10", text: "text-amber-400", dot: "bg-amber-400", label: "Monitoring" },
};

interface Props {
  selectedPatient: string | null;
  onSelectPatient: (id: string) => void;
}

export default function PatientSidebar({ selectedPatient, onSelectPatient }: Props) {
  return (
    <aside className="w-64 bg-navy-950 border-r border-card-border flex flex-col h-full flex-shrink-0">
      <div className="px-4 py-4 border-b border-card-border">
        <div className="flex items-center gap-3">
          <div className="w-9 h-9 rounded-lg bg-cyan-400/10 border border-cyan-400/20 flex items-center justify-center">
            <svg className="w-5 h-5 text-cyan-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4.318 6.318a4.5 4.5 0 000 6.364L12 20.364l7.682-7.682a4.5 4.5 0 00-6.364-6.364L12 7.636l-1.318-1.318a4.5 4.5 0 00-6.364 0z" />
            </svg>
          </div>
          <div>
            <h1 className="text-base font-bold text-text-primary tracking-tight">PhysicianAssist</h1>
          </div>
        </div>
      </div>

      <div className="px-3 pt-4 pb-1">
        <div className="flex items-center gap-2 px-1 mb-2">
          <svg className="w-3.5 h-3.5 text-text-muted" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0z" />
          </svg>
          <span className="text-[10px] font-semibold uppercase tracking-widest text-text-muted">AI Powered Patient Panel</span>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto px-2 space-y-1">
        {PATIENTS.map((p) => {
          const sc = statusConfig[p.status];
          const isSelected = selectedPatient === p.id;
          return (
            <button
              key={p.id}
              onClick={() => onSelectPatient(p.id)}
              className={`w-full text-left rounded-lg p-3 transition-all duration-200 border ${
                isSelected
                  ? "border-cyan-400/30 bg-cyan-400/5"
                  : "border-transparent hover:bg-navy-800 hover:border-card-border"
              }`}
            >
              <div className="flex items-center gap-2.5">
                <div className={`w-8 h-8 rounded-lg flex items-center justify-center text-xs font-bold ${
                  isSelected
                    ? "bg-cyan-400/15 text-cyan-400 border border-cyan-400/20"
                    : "bg-navy-700 text-text-secondary border border-card-border"
                }`}>
                  {p.name.split(" ").map((n) => n[0]).join("")}
                </div>
                <div className="min-w-0">
                  <div className={`font-semibold text-sm truncate ${isSelected ? "text-text-primary" : "text-text-secondary"}`}>{p.name}</div>
                  <div className="text-[11px] text-text-muted">{p.age}y {p.gender} · {p.id}</div>
                </div>
              </div>
              <div className="mt-2 ml-[42px] flex items-center gap-2">
                <span className={`inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-medium ${sc.bg} ${sc.text}`}>
                  <span className={`w-1 h-1 rounded-full ${sc.dot}`}></span>
                  {sc.label}
                </span>
                <span className="text-[10px] text-text-muted">{p.imageCount} images</span>
              </div>
              <p className="text-[11px] text-text-muted mt-1 ml-[42px] leading-snug truncate">{p.primaryCondition}</p>
            </button>
          );
        })}
      </div>

      <div className="px-4 py-3 border-t border-card-border">
        <div className="flex items-center gap-2 text-[10px] text-text-muted">
          <div className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse"></div>
          <span>Cortex Agent + MedGemma</span>
        </div>
      </div>
    </aside>
  );
}
