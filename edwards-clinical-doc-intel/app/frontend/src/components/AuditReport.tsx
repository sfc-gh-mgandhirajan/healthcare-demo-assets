import { useState, useEffect, useRef } from 'react'
import { FileText, Loader2, Download, RefreshCw } from 'lucide-react'
import ReactMarkdown from 'react-markdown'
import jsPDF from 'jspdf'
import html2canvas from 'html2canvas'

const TRIALS = ['ALLIANCE', 'ENCIRCLE', 'TRISCEND II', 'ALT-FLOW II', 'CLASP IID']

interface Props {
  trialName?: string
}

const mdComponents = {
  table: ({ children, ...props }: React.HTMLAttributes<HTMLTableElement>) => (
    <div className="table-wrapper"><table {...props}>{children}</table></div>
  ),
}

export default function AuditReport({ trialName }: Props) {
  const [selected, setSelected] = useState(trialName || TRIALS[0])
  const [report, setReport] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)
  const [downloading, setDownloading] = useState(false)
  const reportRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (trialName) setSelected(trialName)
  }, [trialName])

  const generateReport = async () => {
    setLoading(true)
    setReport(null)
    try {
      const res = await fetch(`/api/reports/audit/${encodeURIComponent(selected)}`)
      const data = await res.json()
      setReport(data.report)
    } catch (err) {
      setReport('Error generating report. Please try again.')
    } finally {
      setLoading(false)
    }
  }

  const downloadPDF = async () => {
    if (!reportRef.current) return
    setDownloading(true)
    try {
      const el = reportRef.current
      const canvas = await html2canvas(el, {
        scale: 2,
        useCORS: true,
        backgroundColor: '#ffffff',
        logging: false,
      })

      const imgWidth = 190
      const pageHeight = 277
      const imgHeight = (canvas.height * imgWidth) / canvas.width
      const imgData = canvas.toDataURL('image/png')

      const pdf = new jsPDF('p', 'mm', 'a4')
      let heightLeft = imgHeight
      let position = 10

      pdf.addImage(imgData, 'PNG', 10, position, imgWidth, imgHeight)
      heightLeft -= pageHeight

      while (heightLeft > 0) {
        position = heightLeft - imgHeight + 10
        pdf.addPage()
        pdf.addImage(imgData, 'PNG', 10, position, imgWidth, imgHeight)
        heightLeft -= pageHeight
      }

      pdf.save(`TMF_Audit_Report_${selected.replace(/\s+/g, '_')}_${new Date().toISOString().split('T')[0]}.pdf`)
    } catch (err) {
      console.error('PDF download error:', err)
    } finally {
      setDownloading(false)
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-bold text-edwards-dark">Audit Readiness Report</h2>
          <p className="text-sm text-edwards-gray">AI-generated TMF audit assessment with RAG scoring</p>
        </div>
      </div>

      <div className="flex items-center gap-3">
        <select
          value={selected}
          onChange={e => setSelected(e.target.value)}
          className="px-4 py-2.5 rounded-lg border border-gray-200 bg-white text-sm font-medium text-edwards-dark focus:outline-none focus:ring-2 focus:ring-edwards-accent/30"
        >
          {TRIALS.map(t => (
            <option key={t} value={t}>{t}</option>
          ))}
        </select>
        <button
          onClick={generateReport}
          disabled={loading}
          className="px-5 py-2.5 bg-edwards-red text-white text-sm font-medium rounded-lg hover:bg-red-700 transition-colors disabled:opacity-50 flex items-center gap-2"
        >
          {loading ? <Loader2 size={16} className="animate-spin" /> : <FileText size={16} />}
          {loading ? 'Generating...' : 'Generate Report'}
        </button>
        {report && !loading && (
          <>
            <button
              onClick={generateReport}
              className="px-3 py-2.5 border border-gray-200 rounded-lg hover:bg-gray-50 transition-colors"
              title="Regenerate"
            >
              <RefreshCw size={16} className="text-edwards-gray" />
            </button>
            <button
              onClick={downloadPDF}
              disabled={downloading}
              className="px-4 py-2.5 bg-edwards-dark text-white text-sm font-medium rounded-lg hover:bg-edwards-slate transition-colors disabled:opacity-50 flex items-center gap-2"
            >
              {downloading ? <Loader2 size={16} className="animate-spin" /> : <Download size={16} />}
              {downloading ? 'Exporting...' : 'Download PDF'}
            </button>
          </>
        )}
      </div>

      {loading && (
        <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-12 text-center">
          <Loader2 size={32} className="mx-auto text-edwards-accent animate-spin mb-4" />
          <p className="text-sm text-edwards-gray">Analyzing {selected} trial documents with Cortex AI...</p>
          <p className="text-xs text-edwards-gray mt-1">This may take 15-30 seconds</p>
        </div>
      )}

      {report && !loading && (
        <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
          <div className="px-6 py-4 border-b border-gray-100 flex items-center justify-between bg-gray-50">
            <div className="flex items-center gap-2">
              <FileText size={18} className="text-edwards-red" />
              <h3 className="text-sm font-semibold text-edwards-dark">
                TMF Audit Readiness Report - {selected}
              </h3>
            </div>
            <div className="flex items-center gap-2 text-xs text-edwards-gray">
              <span>Generated {new Date().toLocaleDateString()}</span>
              <span>|</span>
              <span>Powered by Cortex AI</span>
            </div>
          </div>
          <div ref={reportRef} className="px-6 py-6 markdown-content text-sm text-edwards-slate leading-relaxed">
            <ReactMarkdown components={mdComponents}>{report}</ReactMarkdown>
          </div>
        </div>
      )}

      {!report && !loading && (
        <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-16 text-center">
          <FileText size={40} className="mx-auto text-gray-300 mb-4" />
          <p className="text-sm text-edwards-gray">Select a trial and click "Generate Report" to create an audit readiness assessment</p>
        </div>
      )}
    </div>
  )
}
