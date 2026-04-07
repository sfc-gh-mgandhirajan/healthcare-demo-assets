import axios from 'axios';

const API_BASE_URL = import.meta.env.VITE_API_URL || '/api';

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

export interface FilterOptions {
  modalities: { value: string; count: number }[];
  bodyParts: { value: string; count: number }[];
  manufacturers: { value: string; count: number }[];
  institutions: { value: string; count: number }[];
  studyDateRange: { min: string | null; max: string | null };
  patientSex: { value: string; count: number }[];
}

export interface CohortFilters {
  modalities?: string[];
  bodyParts?: string[];
  studyDateFrom?: string;
  studyDateTo?: string;
  manufacturers?: string[];
  institutions?: string[];
  patientSex?: string[];
  patientId?: string;
  accessionNumber?: string;
}

export interface CohortPreview {
  counts: {
    patients: number;
    studies: number;
    series: number;
    instances: number;
  };
  modalityBreakdown: { modality: string; count: number }[];
  monthlyTrend: { month: string; count: number }[];
}

export interface Study {
  studyKey: number;
  studyInstanceUID: string;
  accessionNumber: string | null;
  studyDate: string | null;
  studyDescription: string | null;
  patientKey: number;
  patientId: string;
  patientName: string | null;
  patientSex: string | null;
  patientBirthDate: string | null;
  modalities: string[];
  seriesCount: number;
  instanceCount: number;
}

export interface StudyListResponse {
  studies: Study[];
  pagination: {
    page: number;
    pageSize: number;
    totalCount: number;
    totalPages: number;
  };
}

export interface Series {
  seriesKey: number;
  seriesInstanceUID: string;
  seriesNumber: number | null;
  modality: string;
  bodyPartExamined: string | null;
  seriesDescription: string | null;
  protocolName: string | null;
  numberOfInstances: number;
  equipment: {
    manufacturer: string | null;
    modelName: string | null;
    stationName: string | null;
  };
}

export interface StudyDetails {
  studyKey: number;
  studyInstanceUID: string;
  accessionNumber: string | null;
  studyId: string | null;
  studyDatetime: string | null;
  studyDate: string | null;
  studyDescription: string | null;
  referringPhysician: string | null;
  numberOfSeries: number;
  numberOfInstances: number;
  patient: {
    patientKey: number;
    patientId: string;
    patientName: string | null;
    patientSex: string | null;
    patientBirthDate: string | null;
    patientAge: string | null;
  };
  series: Series[];
}

export interface Instance {
  instanceKey: number;
  sopInstanceUID: string;
  sopClassUID: string;
  instanceNumber: number | null;
  imageType: string[];
  numberOfFrames: number;
  filePath: string | null;
  storageUri: string | null;
  rows: number | null;
  columns: number | null;
}

export interface ViewerConfig {
  instanceKey: number;
  sopInstanceUID: string;
  sopClassUID: string;
  numberOfFrames: number;
  seriesInstanceUID: string;
  modality: string;
  studyInstanceUID: string;
  storageUri: string | null;
  storageProvider: string | null;
  viewerConfig: {
    wadoUriRoot: string;
    wadoUri: string;
    ohifViewerUrl: string;
  };
}

export interface Cohort {
  cohortKey: number;
  cohortName: string;
  cohortDescription: string | null;
  createdBy: string | null;
  filterCriteria: CohortFilters;
  isActive: boolean;
  createdAt: string;
  updatedAt: string;
}

export const getFilterOptions = async (): Promise<FilterOptions> => {
  const response = await api.get('/filters/options');
  return response.data;
};

export const previewCohort = async (filters: CohortFilters): Promise<CohortPreview> => {
  const response = await api.post('/cohorts/preview', filters);
  return response.data;
};

export const getCohortStudies = async (
  filters: CohortFilters,
  page: number = 1,
  pageSize: number = 25,
  sortBy: string = 'study_date',
  sortOrder: string = 'desc'
): Promise<StudyListResponse> => {
  const response = await api.post('/cohorts/studies', {
    filters,
    page,
    pageSize,
    sortBy,
    sortOrder,
  });
  return response.data;
};

export const getStudyDetails = async (studyInstanceUID: string): Promise<StudyDetails> => {
  const response = await api.get(`/studies/${studyInstanceUID}`);
  return response.data;
};

export const getSeriesInstances = async (
  seriesInstanceUID: string
): Promise<{ seriesInstanceUID: string; instances: Instance[]; count: number }> => {
  const response = await api.get(`/series/${seriesInstanceUID}/instances`);
  return response.data;
};

export const getViewerConfig = async (sopInstanceUID: string): Promise<ViewerConfig> => {
  const response = await api.get(`/instances/${sopInstanceUID}/view`);
  return response.data;
};

export const listCohorts = async (): Promise<{ cohorts: Cohort[] }> => {
  const response = await api.get('/cohorts');
  return response.data;
};

export const saveCohort = async (
  cohortName: string,
  cohortDescription: string,
  filterCriteria: CohortFilters,
  createdBy?: string
): Promise<{ message: string; cohortKey: number }> => {
  const response = await api.post('/cohorts', {
    cohortName,
    cohortDescription,
    filterCriteria,
    createdBy,
  });
  return response.data;
};

export default api;
