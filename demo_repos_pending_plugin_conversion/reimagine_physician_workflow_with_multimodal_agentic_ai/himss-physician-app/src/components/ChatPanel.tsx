import { useState, useRef, useEffect } from "react";
import type { ChatMessage } from "../types";
import ChatBubble from "./ChatBubble";

interface Props {
  messages: ChatMessage[];
  isLoading: boolean;
  error: string | null;
  onSend: (text: string, patientContext?: { patientId: string; patientName: string }) => void;
  selectedPatient: string | null;
  patientName: string | null;
  onClose: () => void;
}

export default function ChatPanel({ messages, isLoading, error, onSend, selectedPatient, patientName, onClose }: Props) {
  const [input, setInput] = useState("");
  const [showQuickActions, setShowQuickActions] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);


  const handleSubmit = () => {
    const text = input.trim();
    if (!text || isLoading) return;
    setInput("");
    onSend(text, selectedPatient && patientName ? { patientId: selectedPatient, patientName } : undefined);
    if (inputRef.current) inputRef.current.style.height = "auto";
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSubmit();
    }
  };

  const handleTextareaInput = () => {
    if (inputRef.current) {
      inputRef.current.style.height = "auto";
      inputRef.current.style.height = Math.min(inputRef.current.scrollHeight, 120) + "px";
    }
  };

  return (
    <div className="w-[420px] flex-shrink-0 flex flex-col h-full bg-navy-900 border-l border-card-border">
      <div className="px-4 py-3 bg-navy-800 border-b border-card-border flex items-center justify-between">
        <div>
          <div className="flex items-center gap-2">
            <svg className="w-4 h-4 text-cyan-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 10h.01M12 10h.01M16 10h.01M9 16H5a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v8a2 2 0 01-2 2h-5l-5 5v-5z" />
            </svg>
            <h2 className="text-sm font-semibold text-text-primary">Agentic Physician Assistant</h2>
          </div>
          <p className="text-[10px] text-text-muted mt-0.5">
            {patientName ? `Context: ${patientName}` : "Cortex Agent + MedGemma"}
          </p>
        </div>
        <div className="flex items-center gap-2">
          {error && (
            <div className="flex items-center gap-1 px-2 py-1 rounded bg-red-500/10 border border-red-500/20">
              <span className="text-[10px] text-red-400 max-w-32 truncate">{error}</span>
            </div>
          )}

          <button onClick={onClose} className="p-1 rounded hover:bg-navy-700 text-text-muted transition-colors">
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto px-4 py-3">
        {messages.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-full text-center px-4">
            <div className="w-12 h-12 rounded-xl bg-cyan-400/10 border border-cyan-400/20 flex items-center justify-center mb-4">
              <svg className="w-6 h-6 text-cyan-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" />
              </svg>
            </div>
            <h3 className="text-sm font-semibold text-text-primary mb-1">Agentic Physician Assistant</h3>
            <p className="text-xs text-text-muted leading-relaxed mb-4">
              Ask questions about {patientName || "patients"}, request medical interpretations, or explore multi-modal clinical data.
            </p>
            <div className="w-full space-y-2">
              {[
                { icon: "🔬", label: "Clinical Summary", prompt: "Give me a comprehensive clinical summary including active conditions, current medications, and recent vitals." },
                { icon: "🫀", label: "ECG Interpretation", prompt: "Interpret the most recent ECG for this patient and provide clinical recommendations." },
                { icon: "💊", label: "Medication Review", prompt: "Review all active medications for potential interactions and flag any concerns given the patient's conditions." },
                { icon: "📊", label: "Risk Assessment", prompt: "Based on this patient's vitals trends, conditions, and imaging results, what are the key clinical risks I should monitor?" },
              ].map((q) => (
                <button
                  key={q.label}
                  onClick={() => {
                    onSend(q.prompt, selectedPatient && patientName ? { patientId: selectedPatient, patientName } : undefined);
                  }}
                  disabled={!selectedPatient}
                  className="w-full text-left px-3 py-2.5 rounded-lg border border-card-border bg-navy-800/50 hover:bg-navy-700 hover:border-cyan-400/30 transition-all group disabled:opacity-40 disabled:cursor-not-allowed"
                >
                  <div className="flex items-start gap-2.5">
                    <span className="text-sm mt-0.5">{q.icon}</span>
                    <div>
                      <div className="text-[11px] font-semibold text-text-primary group-hover:text-cyan-400 transition-colors">{q.label}</div>
                      <div className="text-[10px] text-text-muted leading-snug mt-0.5">{q.prompt}</div>
                    </div>
                  </div>
                </button>
              ))}
            </div>
          </div>
        ) : (
          <>
            {messages.map((msg) => (
              <ChatBubble key={msg.id} message={msg} />
            ))}
            <div ref={messagesEndRef} />
          </>
        )}
      </div>

      {messages.length > 0 && showQuickActions && (
        <div className="px-3 py-2 border-t border-card-border bg-navy-800/80 space-y-1.5 max-h-48 overflow-y-auto">
          {[
            { icon: "🔬", label: "Clinical Summary", prompt: "Give me a comprehensive clinical summary including active conditions, current medications, and recent vitals." },
            { icon: "🫀", label: "ECG Interpretation", prompt: "Interpret the most recent ECG for this patient and provide clinical recommendations." },
            { icon: "💊", label: "Medication Review", prompt: "Review all active medications for potential interactions and flag any concerns given the patient's conditions." },
            { icon: "📊", label: "Risk Assessment", prompt: "Based on this patient's vitals trends, conditions, and imaging results, what are the key clinical risks I should monitor?" },
          ].map((q) => (
            <button
              key={q.label}
              onClick={() => {
                setShowQuickActions(false);
                onSend(q.prompt, selectedPatient && patientName ? { patientId: selectedPatient, patientName } : undefined);
              }}
              disabled={!selectedPatient || isLoading}
              className="w-full text-left px-2.5 py-2 rounded-lg border border-card-border bg-navy-800/50 hover:bg-navy-700 hover:border-cyan-400/30 transition-all group disabled:opacity-40 disabled:cursor-not-allowed"
            >
              <div className="flex items-center gap-2">
                <span className="text-xs">{q.icon}</span>
                <span className="text-[11px] font-medium text-text-primary group-hover:text-cyan-400 transition-colors">{q.label}</span>
              </div>
            </button>
          ))}
        </div>
      )}

      <div className="px-4 py-3 bg-navy-800 border-t border-card-border">
        <div className="flex items-end gap-2">
          {messages.length > 0 && (
            <button
              onClick={() => setShowQuickActions((v) => !v)}
              className={`flex-shrink-0 w-9 h-9 rounded-lg border flex items-center justify-center transition-colors ${
                showQuickActions
                  ? "bg-cyan-400/10 border-cyan-400/30 text-cyan-400"
                  : "border-card-border bg-navy-700 text-text-muted hover:text-cyan-400 hover:border-cyan-400/30"
              }`}
              title="Quick actions"
            >
              <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6h16M4 12h16M4 18h7" />
              </svg>
            </button>
          )}
          <div className="flex-1">
            <textarea
              ref={inputRef}
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onInput={handleTextareaInput}
              onKeyDown={handleKeyDown}
              placeholder={selectedPatient ? `Ask about ${patientName || selectedPatient}...` : "Ask a clinical question..."}
              rows={1}
              className="w-full resize-none rounded-lg border border-card-border bg-navy-700 px-3 py-2.5 text-xs text-text-primary placeholder:text-text-muted focus:outline-none focus:ring-1 focus:ring-cyan-400/50 focus:border-cyan-400/50 transition-all"
              disabled={isLoading}
            />
          </div>
          <button
            onClick={handleSubmit}
            disabled={!input.trim() || isLoading}
            className="flex-shrink-0 w-9 h-9 rounded-lg bg-cyan-400 text-navy-900 flex items-center justify-center hover:bg-cyan-500 disabled:opacity-30 disabled:cursor-not-allowed transition-colors font-bold"
          >
            {isLoading ? (
              <svg className="w-4 h-4 animate-spin" fill="none" viewBox="0 0 24 24">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
              </svg>
            ) : (
              <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2.5} d="M12 19l9 2-9-18-9 18 9-2zm0 0v-8" />
              </svg>
            )}
          </button>
        </div>
        <p className="text-center text-[9px] text-text-muted mt-1.5">
          AI insights require physician review
        </p>
      </div>
    </div>
  );
}
