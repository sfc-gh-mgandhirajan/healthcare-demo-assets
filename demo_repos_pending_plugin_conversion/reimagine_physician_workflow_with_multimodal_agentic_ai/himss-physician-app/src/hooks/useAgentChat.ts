import { useState, useCallback, useRef } from "react";
import type { ChatMessage, SqlResult, ToolCall, ImageInterpretation } from "../types";

const AGENT_DB = import.meta.env.VITE_AGENT_DATABASE as string || "SNOWFLAKE_INTELLIGENCE";
const AGENT_SCHEMA = import.meta.env.VITE_AGENT_SCHEMA as string || "AGENTS";
const AGENT_NAME = import.meta.env.VITE_AGENT_NAME as string || "HIMSS_PHYSICIAN_AGENT";
const AGENT_MODEL = import.meta.env.VITE_AGENT_MODEL as string || "claude-4-sonnet";
const AGENT_API_URL = `/api/v2/databases/${AGENT_DB}/schemas/${AGENT_SCHEMA}/agents/${AGENT_NAME}:run`;

interface UseAgentChatReturn {
  messages: ChatMessage[];
  isLoading: boolean;
  error: string | null;
  sendMessage: (text: string, patientContext?: { patientId: string; patientName: string }) => Promise<void>;
  clearMessages: () => void;
}

const TOKEN = import.meta.env.VITE_SNOWFLAKE_PAT as string | undefined;

export function useAgentChat(): UseAgentChatReturn {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const clearMessages = useCallback(() => {
    setMessages([]);
    setError(null);
  }, []);

  const sendMessage = useCallback(
    async (text: string, patientContext?: { patientId: string; patientName: string }) => {
      if (!TOKEN) {
        setError("Missing VITE_SNOWFLAKE_PAT in .env.local");
        return;
      }

      const userMsg: ChatMessage = {
        id: crypto.randomUUID(),
        role: "user",
        content: text,
        timestamp: new Date(),
      };

      const assistantId = crypto.randomUUID();
      const assistantMsg: ChatMessage = {
        id: assistantId,
        role: "assistant",
        content: "",
        isStreaming: true,
        timestamp: new Date(),
        toolCalls: [],
      };

      setMessages((prev) => [...prev, userMsg, assistantMsg]);
      setIsLoading(true);
      setError(null);

      abortRef.current = new AbortController();

      try {
        const response = await fetch(AGENT_API_URL, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${TOKEN}`,
            "X-Snowflake-Authorization-Token-Type": "PROGRAMMATIC_ACCESS_TOKEN",
            Accept: "text/event-stream",
          },
          body: JSON.stringify({
            model: AGENT_MODEL,
            messages: [
              ...messages
                .filter((m) => !m.isStreaming && m.content)
                .slice(-6)
                .map((m) => ({ role: m.role, content: [{ type: "text", text: m.content }] })),
              { role: "user", content: [{ type: "text", text: patientContext ? `[Patient Context: ${patientContext.patientName}, ID: ${patientContext.patientId}] ${text}` : text }] },
            ],
          }),
          signal: abortRef.current.signal,
        });

        if (!response.ok) {
          const errBody = await response.text();
          throw new Error(`Agent API error ${response.status}: ${errBody}`);
        }

        const reader = response.body?.getReader();
        if (!reader) throw new Error("No response body");

        const decoder = new TextDecoder();
        let buffer = "";
        let accumulatedText = "";
        let accumulatedTools: ToolCall[] = [];
        let imageUrl: string | undefined;
        let imageInterpretation: ImageInterpretation | undefined;
        let sqlResults: SqlResult | undefined;
        let currentEventType = "";

        const updateMsg = () => {
          setMessages((prev) =>
            prev.map((m) =>
              m.id === assistantId
                ? { ...m, content: accumulatedText, toolCalls: accumulatedTools, imageUrl, imageInterpretation, sqlResults }
                : m
            )
          );
        };

        const processImageResult = (json: Record<string, unknown>) => {
          const imgUrl = (json.image_url as string) || (json.ecg_image_url as string);
          if (imgUrl) {
            imageUrl = imgUrl;
          }
          if (json.image_id || json.ecg_id || imgUrl) {
            imageInterpretation = {
              imageId: (json.image_id as string) || (json.ecg_id as string) || "",
              patientName: (json.patient_name as string) || "",
              modality: (json.modality as string) || "ECG",
              bodyPart: (json.body_part as string) || "",
              imageDate: (json.image_date as string) || (json.recorded_date as string) || "",
              description: (json.description as string) || "",
              indication: (json.clinical_indication as string) || "",
              imageUrl: imgUrl || "",
              interpretation: (json.medgemma_interpretation as string) || undefined,
              status: (json.status as "success" | "error" | "pending") || "pending",
            };
          }
        };

        const processSqlResult = (json: Record<string, unknown>) => {
          const resultSet = json.result_set as Record<string, unknown> | undefined;
          if (resultSet) {
            const meta = resultSet.resultSetMetaData as Record<string, unknown> | undefined;
            const cols = (meta?.rowType as { name: string }[])?.map((c) => c.name) || [];
            const rows = (resultSet.data as (string | number | null)[][]) || [];
            sqlResults = { sql: (json.sql as string) || "", columns: cols, rows };
          }
        };

        const cleanErrorText = (msg: string): string => {
          if (msg.includes("Python Interpreter Error")) return "ECG interpretation service is temporarily unavailable.";
          if (msg.includes("SnowparkSQLException")) return "Query execution error. Retrying with alternative approach.";
          if (msg.includes("cannot be called in warehouse")) return "ECG model requires GPU compute. Service is starting up.";
          return msg.replace(/Traceback.*$/s, "").trim();
        };

        while (true) {
          const { done, value } = await reader.read();
          if (done) break;

          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split("\n");
          buffer = lines.pop() || "";

          for (const line of lines) {
            if (line.startsWith("event:")) {
              currentEventType = line.slice(6).trim();
              continue;
            }
            if (!line.startsWith("data:")) continue;

            const jsonStr = line.slice(5).trim();
            if (!jsonStr || jsonStr === "[DONE]") continue;

            try {
              const evt = JSON.parse(jsonStr);

              if (currentEventType === "response.text.delta") {
                accumulatedText += evt.text || "";
                updateMsg();
              }

              if (currentEventType === "response.tool_use") {
                const toolName = evt.name || "unknown";
                const existing = accumulatedTools.find((t) => t.name === toolName);
                if (!existing) {
                  accumulatedTools = [...accumulatedTools, { name: toolName, input: evt.input, status: "running" }];
                  updateMsg();
                }
              }

              if (currentEventType === "response.tool_result") {
                if (Array.isArray(evt.content)) {
                  for (const item of evt.content) {
                    if (item.type === "json" && item.json) {
                      const json = item.json as Record<string, unknown>;
                      processImageResult(json);
                      processSqlResult(json);
                      if (json.error) {
                        const raw = (json.error as Record<string, string>).message || JSON.stringify(json.error);
                        accumulatedText += `\n\n*${cleanErrorText(raw)}*\n`;
                      }
                    }
                  }
                }
                accumulatedTools = accumulatedTools.map((t) => ({ ...t, status: "complete" as const }));
                updateMsg();
              }

              if (currentEventType === "response.text") {
                const resultSet = evt.result_set as Record<string, unknown> | undefined;
                if (resultSet) {
                  processSqlResult(evt as Record<string, unknown>);
                  updateMsg();
                }
              }

              if (currentEventType === "response") {
                if (Array.isArray(evt.content)) {
                  for (const item of evt.content) {
                    if (item.type === "text" && item.text) {
                      accumulatedText = item.text;
                    }
                    if (item.type === "tool_result" && item.tool_result?.content) {
                      for (const sub of item.tool_result.content) {
                        if (sub.type === "json" && sub.json) {
                          processImageResult(sub.json);
                          processSqlResult(sub.json);
                        }
                      }
                    }
                  }
                  updateMsg();
                }
              }

              if (currentEventType === "error") {
                const errMsg = evt.message || "Unknown agent error";
                accumulatedText += `\n\n*${cleanErrorText(errMsg)}*\n`;
                updateMsg();
              }

            } catch {
              // skip unparseable lines
            }
          }
        }

        setMessages((prev) =>
          prev.map((m) =>
            m.id === assistantId
              ? { ...m, content: accumulatedText, isStreaming: false, imageUrl, imageInterpretation, sqlResults, toolCalls: accumulatedTools }
              : m
          )
        );
      } catch (err: unknown) {
        if (err instanceof DOMException && err.name === "AbortError") return;
        const msg = err instanceof Error ? err.message : "Unknown error";
        setError(msg);
        setMessages((prev) =>
          prev.map((m) =>
            m.id === assistantId
              ? { ...m, content: `Error: ${msg}`, isStreaming: false }
              : m
          )
        );
      } finally {
        setIsLoading(false);
      }
    },
    [messages]
  );

  return { messages, isLoading, error, sendMessage, clearMessages };
}
