import React from "react"

/**
 * Dependency-free, XSS-safe markdown renderer (React nodes only — never
 * dangerouslySetInnerHTML). Supports what the Cortex agent emits:
 * headings, paragraphs, bold/italic, inline code, bullet & numbered lists,
 * fenced code blocks, and simple pipe tables. Pass `streaming` to append a
 * blinking cursor to the last rendered block while text is still arriving.
 */

/** Inline: **bold**, *italic* / _italic_, `code` → React nodes. */
function inline(s: string, keyBase: string): React.ReactNode[] {
  const out: React.ReactNode[] = []
  const re = /\*\*([^*]+)\*\*|\*([^*]+)\*|_([^_]+)_|`([^`]+)`/g
  let last = 0, m: RegExpExecArray | null, i = 0
  while ((m = re.exec(s)) !== null) {
    if (m.index > last) out.push(s.slice(last, m.index))
    if (m[1] != null) out.push(<strong key={`${keyBase}-b${i}`}>{m[1]}</strong>)
    else if (m[2] != null || m[3] != null) out.push(<em key={`${keyBase}-e${i}`}>{m[2] ?? m[3]}</em>)
    else out.push(<code key={`${keyBase}-c${i}`}>{m[4]}</code>)
    last = m.index + m[0].length; i++
  }
  if (last < s.length) out.push(s.slice(last))
  return out
}

function isTableSep(line: string): boolean {
  return /^\s*\|?\s*:?-{2,}:?\s*(\|\s*:?-{2,}:?\s*)+\|?\s*$/.test(line)
}
function splitRow(line: string): string[] {
  return line.replace(/^\s*\|/, "").replace(/\|\s*$/, "").split("|").map((c) => c.trim())
}

export function Markdown({ text, streaming = false }: { text: string; streaming?: boolean }) {
  const t = (text ?? "").replace(/\r/g, "")
  const lines = t.split("\n")
  const blocks: React.ReactNode[] = []
  let bullets: string[] = []
  let ordered: string[] = []
  let n = 0

  const flushBullets = () => {
    if (bullets.length) {
      const items = bullets
      blocks.push(<ul key={`ul${n}`}>{items.map((b, j) => <li key={j}>{inline(b, `u${n}-${j}`)}</li>)}</ul>)
      n++; bullets = []
    }
  }
  const flushOrdered = () => {
    if (ordered.length) {
      const items = ordered
      blocks.push(<ol key={`ol${n}`}>{items.map((b, j) => <li key={j}>{inline(b, `o${n}-${j}`)}</li>)}</ol>)
      n++; ordered = []
    }
  }
  const flush = () => { flushBullets(); flushOrdered() }

  for (let i = 0; i < lines.length; i++) {
    const raw = lines[i]
    const line = raw.trim()

    // fenced code block
    if (line.startsWith("```")) {
      flush()
      const code: string[] = []
      i++
      while (i < lines.length && !lines[i].trim().startsWith("```")) { code.push(lines[i]); i++ }
      blocks.push(<pre key={`pre${n++}`}><code>{code.join("\n")}</code></pre>)
      continue
    }

    // table: header row followed by a separator row
    if (line.includes("|") && i + 1 < lines.length && isTableSep(lines[i + 1])) {
      flush()
      const header = splitRow(line)
      i += 2
      const rows: string[][] = []
      while (i < lines.length && lines[i].trim().includes("|") && lines[i].trim()) {
        rows.push(splitRow(lines[i].trim())); i++
      }
      i--
      blocks.push(
        <table key={`tb${n++}`}>
          <thead><tr>{header.map((h, j) => <th key={j}>{inline(h, `th${n}-${j}`)}</th>)}</tr></thead>
          <tbody>{rows.map((r, ri) => <tr key={ri}>{r.map((c, ci) => <td key={ci}>{inline(c, `td${n}-${ri}-${ci}`)}</td>)}</tr>)}</tbody>
        </table>
      )
      continue
    }

    if (!line) { flush(); continue }

    const h = line.match(/^(#{1,3})\s+(.*)$/)
    if (h) {
      flush()
      const level = h[1].length
      const content = inline(h[2], `h${n}`)
      blocks.push(level === 1 ? <h3 key={`h${n++}`}>{content}</h3> : level === 2 ? <h4 key={`h${n++}`}>{content}</h4> : <h5 key={`h${n++}`}>{content}</h5>)
      continue
    }

    const ob = line.match(/^\d+[.)]\s+(.*)$/)
    if (ob) { flushBullets(); ordered.push(ob[1]); continue }

    const mb = line.match(/^[-*+]\s+(.*)$/)
    if (mb) { flushOrdered(); bullets.push(mb[1]); continue }

    flush()
    blocks.push(<p key={`p${n++}`}>{inline(line, `p${n}`)}</p>)
  }
  flush()

  if (streaming) blocks.push(<span key="cursor" className="cursor" aria-hidden="true" />)
  return <div className="md">{blocks}</div>
}
