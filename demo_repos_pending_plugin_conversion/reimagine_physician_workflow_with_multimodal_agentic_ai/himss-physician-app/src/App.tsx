import { useState, useCallback, useEffect } from "react";
import PatientSidebar from "./components/PatientSidebar";
import PatientDetailPanel from "./components/PatientDetailPanel";
import ChatPanel from "./components/ChatPanel";
import { useAgentChat } from "./hooks/useAgentChat";
import { usePatientData } from "./hooks/usePatientData";

export default function App() {
  const [selectedPatient, setSelectedPatient] = useState<string | null>(null);
  const agentChat = useAgentChat();
  const { patientData, isLoading: patientLoading, loadPatient } = usePatientData();
  const [chatOpen, setChatOpen] = useState(false);

  const { messages, isLoading, error } = agentChat;

  const handleSelectPatient = useCallback(
    (id: string) => {
      setSelectedPatient(id);
      if (messages.length > 0) {
        agentChat.clearMessages();
      }
      loadPatient(id);
    },
    [messages.length, agentChat.clearMessages, loadPatient]
  );

  const patientName = patientData ? `${patientData.demographics.firstName} ${patientData.demographics.lastName}` : null;

  const handleQuickAction = useCallback(
    (prompt: string) => {
      setChatOpen(true);
      agentChat.sendMessage(prompt, selectedPatient && patientName ? { patientId: selectedPatient, patientName } : undefined);
    },
    [agentChat.sendMessage, selectedPatient, patientName]
  );

  const [userClosed, setUserClosed] = useState(false);

  useEffect(() => {
    if (messages.length > 0 && !chatOpen && !userClosed) setChatOpen(true);
  }, [messages.length, chatOpen, userClosed]);

  useEffect(() => {
    if (chatOpen) setUserClosed(false);
  }, [chatOpen]);

  return (
    <div className="h-screen flex overflow-hidden bg-navy-900">
      <PatientSidebar
        selectedPatient={selectedPatient}
        onSelectPatient={handleSelectPatient}
      />
      <div className="flex-1 flex overflow-hidden">
        {selectedPatient && patientData ? (
          <PatientDetailPanel
            data={patientData}
            isLoading={patientLoading}
            onAskAgent={handleQuickAction}
          />
        ) : (
          <div className="flex-1 flex items-center justify-center">
            <div className="text-center">
              <div className="w-20 h-20 rounded-2xl bg-cyan-400/10 border border-cyan-400/20 flex items-center justify-center mx-auto mb-6">
                <svg className="w-10 h-10 text-cyan-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M4.318 6.318a4.5 4.5 0 000 6.364L12 20.364l7.682-7.682a4.5 4.5 0 00-6.364-6.364L12 7.636l-1.318-1.318a4.5 4.5 0 00-6.364 0z" />
                </svg>
              </div>
              <h2 className="text-xl font-bold text-text-primary mb-2">PhysicianAssist</h2>
              <p className="text-sm text-text-muted max-w-md">
                Select a patient from the panel to view their clinical record, medical images, and AI-powered insights.
              </p>
            </div>
          </div>
        )}
        {chatOpen && (
          <ChatPanel
            messages={messages}
            isLoading={isLoading}
            error={error}
            onSend={agentChat.sendMessage}
            selectedPatient={selectedPatient}
            patientName={patientName}
            onClose={() => { setChatOpen(false); setUserClosed(true); }}
          />
        )}
      </div>
      {!chatOpen && selectedPatient && (
        <button
          onClick={() => setChatOpen(true)}
          className="fixed bottom-6 right-6 w-14 h-14 rounded-full bg-cyan-400 text-navy-950 flex items-center justify-center shadow-lg shadow-cyan-400/30 hover:bg-cyan-300 transition-colors z-50"
        >
          <svg className="w-6 h-6" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M8 10h.01M12 10h.01M16 10h.01M9 16H5a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v8a2 2 0 01-2 2h-5l-5 5v-5z" />
          </svg>
        </button>
      )}
    </div>
  );
}
