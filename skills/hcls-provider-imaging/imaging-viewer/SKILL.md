---
name: imaging-viewer
description: "React-based medical imaging analytics application on Snowflake. Cohort explorer, cross-modality comparisons, equipment utilization dashboards, temporal trend visualization, quality scorecards, and patient imaging timeline drill-down. Deployed via Snowpark Container Services. Use when: React imaging app, imaging dashboard, cohort dashboard, imaging UI, DICOM viewer, radiology UI, deploy viewer, equipment dashboard, quality scorecard UI."
parent_skill: hcls-provider-imaging
---

# Medical Imaging Viewer & Dashboard (React)

## When to Load

Parent router routes here on **VIEWER** intent: React imaging app, imaging dashboard, cohort dashboard, imaging UI, DICOM viewer, equipment dashboard, quality scorecard UI.

## Prerequisites

- Node.js >= 18, React 18+ with TypeScript
- Snowflake SQL API access or SPCS for deployment
- Analytics Dynamic Tables from `dicom-analytics`: `DT_IMAGING_COHORTS`, `DT_EQUIPMENT_UTILIZATION_DEEP`, `DT_PATIENT_IMAGING_TIMELINE`, `DT_IMAGING_QUALITY_SCORECARD`

## Architecture

```
src/
├── App.tsx                         # Root with react-router
├── api/snowflake.ts                # SQL API client
├── hooks/useSnowflakeQuery.ts      # TanStack Query hook
├── types/imaging.ts                # TypeScript interfaces
├── components/
│   ├── CohortExplorer.tsx          # Step 2
│   ├── ModalityDashboard.tsx       # Step 3
│   ├── EquipmentDashboard.tsx      # Step 4
│   ├── TemporalTrends.tsx          # Step 5
│   ├── QualityScorecard.tsx        # Step 6
│   └── shared/{DataTable,FilterPanel,ChartCard}.tsx
├── Dockerfile
└── service-spec.yaml
```

## Workflow

### Step 1: Project Setup

**Ask** user which components: (1) Cohort Explorer, (2) Cross-Modality Dashboard, (3) Equipment Utilization, (4) Temporal Trends, (5) Quality Scorecard, (6) All.

**MANDATORY STOPPING POINT:** Confirm scope before scaffolding.

```bash
npx create-react-app imaging-viewer --template typescript
cd imaging-viewer
npm install recharts react-router-dom @tanstack/react-query axios date-fns tailwindcss
```

**Snowflake SQL API client (`api/snowflake.ts`):**
```typescript
import axios from 'axios';
const API = import.meta.env.VITE_SNOWFLAKE_API_URL || '/api/v2';

export async function executeQuery(sql: string) {
  const res = await axios.post(`${API}/statements`, {
    statement: sql, timeout: 60, database: '{database}',
    schema: '{schema}', warehouse: '{warehouse}'
  }, { headers: { 'Authorization': `Bearer ${import.meta.env.VITE_SNOWFLAKE_TOKEN}` } });
  const cols = res.data.resultSetMetaData.rowType.map((r: {name: string}) => r.name);
  return { data: (res.data.data || []).map((row: string[]) => {
    const obj: Record<string, unknown> = {};
    cols.forEach((c: string, i: number) => { obj[c] = row[i]; });
    return obj;
  }), columns: cols };
}
```

**Query hook (`hooks/useSnowflakeQuery.ts`):**
```typescript
import { useQuery } from '@tanstack/react-query';
import { executeQuery } from '../api/snowflake';
export function useSnowflakeQuery(key: string[], sql: string, enabled = true) {
  return useQuery({ queryKey: key, queryFn: () => executeQuery(sql), enabled, staleTime: 300000, retry: 2 });
}
```

### Step 2: Cohort Explorer

Interactive cohort builder with filters, modality profile chart, results grid.

**Key SQL:** `SELECT * FROM DT_IMAGING_COHORTS WHERE TOTAL_STUDIES >= :min ORDER BY TOTAL_STUDIES DESC LIMIT 500`

**Component pattern:** `FilterPanel` (modality multi-select, institution, date range, min studies slider) -> `BarChart` (modality profile distribution) -> `DataTable` (patient rows with PATIENT_ID, MODALITY_PROFILE, TOTAL_STUDIES, CT_COUNT, MR_COUNT, IMAGING_SPAN_DAYS).

**Summary chart SQL:**
```sql
SELECT MODALITY_PROFILE, COUNT(*) AS PATIENT_COUNT, AVG(TOTAL_STUDIES) AS AVG_STUDIES
FROM DT_IMAGING_COHORTS GROUP BY MODALITY_PROFILE ORDER BY PATIENT_COUNT DESC LIMIT 20
```

Use Recharts `BarChart` with `MODALITY_PROFILE` on X-axis and `PATIENT_COUNT` as bars.

**MANDATORY STOPPING POINT:** Verify cohort data connectivity before building more components.

### Step 3: Cross-Modality Dashboard

Modality distribution (PieChart), monthly volume trend (LineChart), body part coverage (BarChart).

**SQL patterns:**
```sql
SELECT ser.MODALITY, COUNT(DISTINCT s.STUDY_INSTANCE_UID) AS STUDIES
FROM DICOM_SERIES ser JOIN DICOM_STUDY s ON ser.STUDY_KEY = s.STUDY_KEY
WHERE ser.MODALITY IS NOT NULL GROUP BY ser.MODALITY;

SELECT DATE_TRUNC('month', s.STUDY_DATE) AS MONTH, ser.MODALITY, COUNT(DISTINCT s.STUDY_INSTANCE_UID) AS STUDIES
FROM DICOM_SERIES ser JOIN DICOM_STUDY s ON ser.STUDY_KEY = s.STUDY_KEY
WHERE s.STUDY_DATE IS NOT NULL GROUP BY MONTH, ser.MODALITY ORDER BY MONTH;

SELECT ser.BODY_PART_EXAMINED, ser.MODALITY, COUNT(DISTINCT s.STUDY_INSTANCE_UID) AS STUDIES
FROM DICOM_SERIES ser JOIN DICOM_STUDY s ON ser.STUDY_KEY = s.STUDY_KEY
WHERE ser.BODY_PART_EXAMINED IS NOT NULL GROUP BY ser.BODY_PART_EXAMINED, ser.MODALITY ORDER BY STUDIES DESC LIMIT 30;
```

Render using Recharts `PieChart` (modality), `LineChart` (trend by month), stacked `BarChart` (body parts).

### Step 4: Equipment Utilization Dashboard

Scanner throughput heatmaps, comparative performance bars, station selector.

**SQL patterns:**
```sql
SELECT STATION_NAME, MANUFACTURER, MANUFACTURER_MODEL_NAME, SUM(STUDIES_PERFORMED) AS TOTAL_STUDIES,
    COUNT(DISTINCT STUDY_DAY) AS ACTIVE_DAYS
FROM DT_EQUIPMENT_UTILIZATION_DEEP GROUP BY STATION_NAME, MANUFACTURER, MANUFACTURER_MODEL_NAME;

SELECT DAY_OF_WEEK_NUM, HOUR_OF_DAY, SUM(STUDIES_PERFORMED) AS STUDIES
FROM DT_EQUIPMENT_UTILIZATION_DEEP GROUP BY DAY_OF_WEEK_NUM, HOUR_OF_DAY;

SELECT STATION_NAME, ROUND(SUM(STUDIES_PERFORMED)/NULLIF(COUNT(DISTINCT STUDY_DAY),0),1) AS AVG_DAILY,
    MAX(STUDIES_PERFORMED) AS PEAK_HOURLY
FROM DT_EQUIPMENT_UTILIZATION_DEEP GROUP BY STATION_NAME ORDER BY AVG_DAILY DESC;
```

Render heatmap using Recharts `ScatterChart` (X=hour, Y=day, Z=studies), throughput comparison using `BarChart`. Add station selector buttons to filter heatmap.

### Step 5: Temporal Trend Visualization

Patient timeline drill-down, follow-up gap analysis, repeat study highlighting.

**Patient timeline:** Input field for PATIENT_ID. Query `DT_PATIENT_IMAGING_TIMELINE` ordered by STUDY_SEQUENCE. Render as vertical timeline with colored cards per event (red border for IS_REPEAT_STUDY, blue for normal). Show "+Nd" badge for DAYS_SINCE_PREV_STUDY.

**Gap analysis:**
```sql
SELECT MODALITY, BODY_PART_EXAMINED, AVG(DAYS_SINCE_PREV_STUDY) AS AVG_GAP, MAX(DAYS_SINCE_PREV_STUDY) AS MAX_GAP,
    SUM(CASE WHEN IS_REPEAT_STUDY THEN 1 ELSE 0 END) AS REPEAT_COUNT
FROM DT_PATIENT_IMAGING_TIMELINE WHERE DAYS_SINCE_PREV_STUDY IS NOT NULL
GROUP BY MODALITY, BODY_PART_EXAMINED ORDER BY AVG_GAP DESC LIMIT 20;
```

Render using `BarChart` with conditional coloring (red if MAX_GAP > 180 days).

### Step 6: Quality Scorecard

Summary KPI cards, completeness trend line, drill-down table for poor-quality studies.

**Summary SQL:**
```sql
SELECT ROUND(AVG(METADATA_COMPLETENESS_PCT),1) AS AVG_COMPLETENESS,
    COUNT(CASE WHEN METADATA_COMPLETENESS_PCT >= 80 THEN 1 END) AS HIGH_QUALITY,
    COUNT(CASE WHEN METADATA_COMPLETENESS_PCT < 50 THEN 1 END) AS LOW_QUALITY,
    SUM(CASE WHEN MISSING_MODALITY THEN 1 ELSE 0 END) AS MISSING_MODALITY_COUNT
FROM DT_IMAGING_QUALITY_SCORECARD;
```

**Trend SQL:**
```sql
SELECT DATE_TRUNC('month', STUDY_DATE) AS MONTH, ROUND(AVG(METADATA_COMPLETENESS_PCT),1) AS AVG_COMPLETENESS
FROM DT_IMAGING_QUALITY_SCORECARD WHERE STUDY_DATE IS NOT NULL GROUP BY MONTH ORDER BY MONTH;
```

Render: 4 KPI cards (avg completeness, high/medium/low counts), `LineChart` for trend, `DataTable` for studies below adjustable threshold (range slider).

**MANDATORY STOPPING POINT:** Review all component queries against available Dynamic Tables before deployment.

### Step 7: SPCS Deployment

**Dockerfile:**
```dockerfile
FROM node:18-alpine AS build
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
RUN npm run build
FROM nginx:alpine
COPY --from=build /app/build /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 8080
CMD ["nginx", "-g", "daemon off;"]
```

**Service spec:**
```yaml
spec:
  containers:
    - name: imaging-viewer
      image: /{database}/{schema}/IMAGE_REPO/imaging-viewer:latest
      resources:
        requests: { memory: 2Gi, cpu: 1 }
  endpoints:
    - name: viewer
      port: 8080
      public: true
```

**Deploy:**
```sql
CREATE IMAGE REPOSITORY IF NOT EXISTS {database}.{schema}.IMAGE_REPO;
CREATE COMPUTE POOL IF NOT EXISTS IMAGING_VIEWER_POOL MIN_NODES=1 MAX_NODES=2 INSTANCE_FAMILY=CPU_X64_S;
CREATE SERVICE {database}.{schema}.IMAGING_VIEWER_SVC
    IN COMPUTE POOL IMAGING_VIEWER_POOL FROM @SERVICE_SPECS SPECIFICATION_FILE='service-spec.yaml';
SHOW ENDPOINTS IN SERVICE {database}.{schema}.IMAGING_VIEWER_SVC;
```

**MANDATORY STOPPING POINT:** Compute pool and SPCS service incur costs. Confirm before creation.

## Stopping Points

- After Step 1: Confirm dashboard scope
- After Step 2: Verify cohort data connectivity
- After Step 6: Review all components before deployment
- After Step 7: SPCS cost approval

## Output

| Artifact | Type | Purpose |
|----------|------|---------|
| React project | Application | Complete dashboard with 5 views |
| `CohortExplorer` | Component | Interactive patient cohort builder |
| `ModalityDashboard` | Component | Modality distribution and volume trends |
| `EquipmentDashboard` | Component | Scanner utilization heatmaps |
| `TemporalTrends` | Component | Patient timelines and gap analysis |
| `QualityScorecard` | Component | Metadata completeness visualization |
| `IMAGING_VIEWER_SVC` | SPCS Service | Deployed React app on Snowflake |
