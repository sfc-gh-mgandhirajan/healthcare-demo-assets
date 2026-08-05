import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import type { ChatMessage } from "../types";

interface Props {
  message: ChatMessage;
}

function cleanMedgemmaContent(text: string): string {
  return text
    .replace(/\*?\*?Image URL\*?\*?:?\s*\S*/gi, '')
    .replace(/\*?\*?Presigned URL\*?\*?:?\s*\S*/gi, '')
    .replace(/\*?\*?Scoped URL\*?\*?:?\s*\S*/gi, '')
    .replace(/\*?\*?Stage Path\*?\*?:?\s*\S*/gi, '')
    .replace(/\*?\*?Image File\*?\*?:?\s*\S*/gi, '')
    .replace(/\*?\*?File Path\*?\*?:?\s*\S*/gi, '')
    .replace(/\*?\*?image_url\*?\*?:?\s*\S*/gi, '')
    .replace(/\*?\*?stage_path\*?\*?:?\s*\S*/gi, '')
    .replace(/\*?\*?Scoped URL for Image\*?\*?:?\s*\S*/gi, '')
    .replace(/https?:\/\/[^\s)]+\.(?:png|jpg|jpeg|gif|bmp|tiff|dcm)\b[^\s)]*/gi, '')
    .replace(/ecg_images\/\S+/gi, '')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

export default function ChatBubble({ message }: Props) {
  const isUser = message.role === "user";

  return (
    <div className={`flex ${isUser ? "justify-end" : "justify-start"} mb-4`}>
      <div className={`max-w-[85%] ${isUser ? "order-2" : "order-1"}`}>
        {!isUser && (
          <div className="flex items-center gap-2 mb-1.5">
            <div className="w-6 h-6 rounded-md bg-gradient-to-br from-cyan-400 to-cyan-600 flex items-center justify-center shadow-sm shadow-cyan-400/20">
              <svg className="w-3.5 h-3.5 text-navy-950" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" />
              </svg>
            </div>
            <span className="text-[11px] font-medium text-text-muted">Agentic Physician Assistant</span>
          </div>
        )}

        {message.toolCalls && message.toolCalls.length > 0 && !isUser && (
          <div className="mb-2 space-y-1">
            {message.toolCalls.map((tool, i) => (
              <div key={i} className="flex items-center gap-2 text-xs">
                <span className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md font-medium border ${
                  tool.name === "MEDGEMMA_MEDICAL_INTERPRETER"
                    ? "bg-purple-900/20 text-purple-300 border-purple-500/30"
                    : "bg-cyan-900/20 text-cyan-300 border-cyan-500/30"
                }`}>
                  {tool.name === "MEDGEMMA_MEDICAL_INTERPRETER" ? (
                    <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2" />
                    </svg>
                  ) : (
                    <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4" />
                    </svg>
                  )}
                  {tool.name}
                </span>
                {tool.status === "running" && (
                  <span className="flex gap-0.5">
                    <span className="typing-dot w-1 h-1 bg-cyan-400 rounded-full"></span>
                    <span className="typing-dot w-1 h-1 bg-cyan-400 rounded-full"></span>
                    <span className="typing-dot w-1 h-1 bg-cyan-400 rounded-full"></span>
                  </span>
                )}
                {tool.status === "complete" && (
                  <svg className="w-3.5 h-3.5 text-emerald-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2.5} d="M5 13l4 4L19 7" />
                  </svg>
                )}
              </div>
            ))}
          </div>
        )}

        <div
          className={`rounded-lg px-4 py-3 ${
            isUser
              ? "bg-gradient-to-br from-cyan-600 to-cyan-700 text-white rounded-br-sm"
              : "bg-navy-800 text-text-primary border border-card-border rounded-bl-sm"
          }`}
        >
          {isUser ? (
            <p className="text-sm leading-relaxed">{message.content}</p>
          ) : (
            <>
              {message.content ? (
                <div className="prose prose-sm max-w-none prose-invert">
                  <ReactMarkdown remarkPlugins={[remarkGfm]}>{cleanMedgemmaContent(message.content)}</ReactMarkdown>
                </div>
              ) : message.isStreaming ? (
                <div className="flex items-center gap-2 py-1">
                  <span className="flex items-center gap-1.5">
                    <span className="typing-dot w-1.5 h-1.5 bg-cyan-400 rounded-full"></span>
                    <span className="typing-dot w-1.5 h-1.5 bg-cyan-400 rounded-full"></span>
                    <span className="typing-dot w-1.5 h-1.5 bg-cyan-400 rounded-full"></span>
                  </span>
                  <span className="text-xs text-text-muted italic">
                    {message.toolCalls?.some((t) => t.status === "running" && t.name === "MEDGEMMA_MEDICAL_INTERPRETER")
                      ? message.toolCalls.find((t) => t.status === "running" && t.name === "MEDGEMMA_MEDICAL_INTERPRETER")?.input?.P_MODE === "interpret_image"
                        ? "Analyzing medical image with MedGemma…"
                        : "Consulting MedGemma for clinical insight…"
                      : message.toolCalls?.some((t) => t.status === "running" && t.name === "PATIENT_ANALYST")
                      ? "Querying patient records…"
                      : message.toolCalls?.some((t) => t.status === "complete")
                      ? "Synthesizing clinical findings…"
                      : "Reviewing patient context…"}
                  </span>
                </div>
              ) : null}
            </>
          )}
        </div>

        {message.ecgInterpretation && !isUser && (
          <div className="mt-3 rounded-lg overflow-hidden border border-card-border bg-navy-800 glow-card">
            <div className="px-4 py-2.5 bg-navy-700 border-b border-card-border flex items-center justify-between">
              <div className="flex items-center gap-2">
                <svg className="w-4 h-4 text-cyan-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
                </svg>
                <span className="text-xs font-semibold text-text-primary">MedGemma Clinical Analysis</span>
              </div>
              <span className={`text-[10px] font-medium px-2 py-0.5 rounded ${
                message.ecgInterpretation.status === "success"
                  ? "bg-emerald-900/30 text-emerald-400"
                  : message.ecgInterpretation.status === "error"
                  ? "bg-red-900/30 text-red-400"
                  : "bg-amber-900/30 text-amber-400"
              }`}>
                {message.ecgInterpretation.status === "success" ? "ANALYZED" : message.ecgInterpretation.status === "error" ? "SERVICE UNAVAILABLE" : "PROCESSING"}
              </span>
            </div>

            <div className="grid grid-cols-4 gap-px bg-card-border">
              <div className="bg-navy-800 px-3 py-2">
                <div className="text-[10px] text-text-muted uppercase tracking-wider">Patient</div>
                <div className="text-xs text-text-primary font-medium mt-0.5">{message.ecgInterpretation.patientName}</div>
              </div>
              <div className="bg-navy-800 px-3 py-2">
                <div className="text-[10px] text-text-muted uppercase tracking-wider">Recorded</div>
                <div className="text-xs text-text-primary font-medium mt-0.5">{message.ecgInterpretation.recordedDate?.split(" ")[0] || "—"}</div>
              </div>
              <div className="bg-navy-800 px-3 py-2">
                <div className="text-[10px] text-text-muted uppercase tracking-wider">Category</div>
                <div className="text-xs text-cyan-400 font-medium mt-0.5">{message.ecgInterpretation.category || "—"}</div>
              </div>
              <div className="bg-navy-800 px-3 py-2">
                <div className="text-[10px] text-text-muted uppercase tracking-wider">Indication</div>
                <div className="text-xs text-text-primary font-medium mt-0.5 truncate">{message.ecgInterpretation.indication || "—"}</div>
              </div>
            </div>

            {message.ecgInterpretation.imageUrl && (
              <div className="p-3 border-t border-card-border">
                <div className="rounded-md overflow-hidden bg-white">
                  <img
                    src={message.ecgInterpretation.imageUrl}
                    alt="12-Lead ECG Recording"
                    className="w-full max-h-96 object-contain"
                    onError={(e) => {
                      (e.target as HTMLImageElement).closest("div")!.innerHTML = '<div class="px-4 py-8 text-center text-text-muted text-xs">ECG image unavailable</div>';
                    }}
                  />
                </div>
              </div>
            )}
          </div>
        )}

        {!message.ecgInterpretation && message.ecgImageUrl && !isUser && (
          <div className="mt-3 rounded-lg overflow-hidden border border-card-border bg-navy-800">
            <div className="px-4 py-2.5 bg-navy-700 border-b border-card-border flex items-center gap-2">
              <svg className="w-4 h-4 text-purple-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 16l4.586-4.586a2 2 0 012.828 0L16 16m-2-2l1.586-1.586a2 2 0 012.828 0L20 14m-6-6h.01M6 20h12a2 2 0 002-2V6a2 2 0 00-2-2H6a2 2 0 00-2 2v12a2 2 0 002 2z" />
              </svg>
              <span className="text-xs font-semibold text-text-primary">ECG Image</span>
            </div>
            <div className="p-3">
              <div className="rounded-md overflow-hidden bg-white">
                <img
                  src={message.ecgImageUrl}
                  alt="ECG Recording"
                  className="w-full max-h-80 object-contain"
                  onError={(e) => {
                    (e.target as HTMLImageElement).style.display = "none";
                  }}
                />
              </div>
            </div>
          </div>
        )}

        {/* Query Results table hidden — data is used by agent but not displayed */}

        <div className={`mt-1.5 text-[10px] ${isUser ? "text-right text-cyan-300/50" : "text-text-muted"}`}>
          {message.timestamp.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}
        </div>
      </div>
    </div>
  );
}
