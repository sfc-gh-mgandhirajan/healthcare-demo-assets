import React, { useState } from 'react';
import {
  ThemeProvider,
  createTheme,
  CssBaseline,
  AppBar,
  Toolbar,
  Typography,
  Container,
  Box,
  Tabs,
  Tab,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  TextField,
  Button,
  Snackbar,
  Alert,
} from '@mui/material';
import LocalHospitalIcon from '@mui/icons-material/LocalHospital';
import CohortBuilder from './components/CohortBuilder';
import StudyExplorer from './components/StudyExplorer';
import StudyDetailView from './components/StudyDetailView';
import DicomViewer from './components/DicomViewer';
import { saveCohort } from './api/imagingApi';
import type { CohortFilters, Study } from './api/imagingApi';

const theme = createTheme({
  palette: {
    primary: {
      main: '#1976d2',
    },
    secondary: {
      main: '#7b1fa2',
    },
    background: {
      default: '#f5f5f5',
    },
  },
  typography: {
    fontFamily: '"Inter", "Roboto", "Helvetica", "Arial", sans-serif',
  },
  components: {
    MuiPaper: {
      styleOverrides: {
        root: {
          borderRadius: 8,
        },
      },
    },
  },
});

type ViewMode = 'cohort' | 'study-detail' | 'viewer';

interface TabPanelProps {
  children?: React.ReactNode;
  index: number;
  value: number;
}

function TabPanel(props: TabPanelProps) {
  const { children, value, index, ...other } = props;
  return (
    <div role="tabpanel" hidden={value !== index} {...other}>
      {value === index && <Box py={3}>{children}</Box>}
    </div>
  );
}

function App() {
  const [tabValue, setTabValue] = useState(0);
  const [activeFilters, setActiveFilters] = useState<CohortFilters>({});
  const [viewMode, setViewMode] = useState<ViewMode>('cohort');
  const [selectedStudy, setSelectedStudy] = useState<Study | null>(null);
  const [viewerStudyUID, setViewerStudyUID] = useState<string | null>(null);
  const [viewerSeriesUID, setViewerSeriesUID] = useState<string | undefined>(undefined);

  const [saveCohortOpen, setSaveCohortOpen] = useState(false);
  const [cohortName, setCohortName] = useState('');
  const [cohortDescription, setCohortDescription] = useState('');
  const [pendingFilters, setPendingFilters] = useState<CohortFilters>({});
  const [snackbar, setSnackbar] = useState<{ open: boolean; message: string; severity: 'success' | 'error' }>({
    open: false,
    message: '',
    severity: 'success',
  });

  const handleSearch = (filters: CohortFilters) => {
    setActiveFilters(filters);
    setTabValue(1);
    setViewMode('cohort');
  };

  const handleViewStudy = (study: Study) => {
    setSelectedStudy(study);
    setViewMode('study-detail');
  };

  const handleOpenViewer = (studyInstanceUID: string, seriesInstanceUID?: string) => {
    setViewerStudyUID(studyInstanceUID);
    setViewerSeriesUID(seriesInstanceUID);
    setViewMode('viewer');
    setTabValue(2);
  };

  const handleBackFromDetail = () => {
    setViewMode('cohort');
    setSelectedStudy(null);
  };

  const handleSaveCohortClick = (filters: CohortFilters) => {
    setPendingFilters(filters);
    setCohortName('');
    setCohortDescription('');
    setSaveCohortOpen(true);
  };

  const handleSaveCohort = async () => {
    if (!cohortName.trim()) return;

    try {
      await saveCohort(cohortName, cohortDescription, pendingFilters);
      setSaveCohortOpen(false);
      setSnackbar({
        open: true,
        message: `Cohort "${cohortName}" saved successfully`,
        severity: 'success',
      });
    } catch {
      setSnackbar({
        open: true,
        message: 'Failed to save cohort',
        severity: 'error',
      });
    }
  };

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <Box sx={{ flexGrow: 1 }}>
        <AppBar position="static" elevation={1}>
          <Toolbar>
            <LocalHospitalIcon sx={{ mr: 2 }} />
            <Typography variant="h6" component="div" sx={{ flexGrow: 1 }}>
              Medical Imaging Studies Portal
            </Typography>
            <Typography variant="body2" sx={{ opacity: 0.8 }}>
              Clinical Research Platform
            </Typography>
          </Toolbar>
        </AppBar>

        <Container maxWidth="xl" sx={{ mt: 2 }}>
          <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
            <Tabs value={tabValue} onChange={(_, v) => setTabValue(v)}>
              <Tab label="Cohort Builder" />
              <Tab label="Study Explorer" disabled={Object.keys(activeFilters).length === 0} />
              <Tab label="DICOM Viewer" disabled={!viewerStudyUID} />
            </Tabs>
          </Box>

          <TabPanel value={tabValue} index={0}>
            <CohortBuilder onSearch={handleSearch} onSaveCohort={handleSaveCohortClick} />
          </TabPanel>

          <TabPanel value={tabValue} index={1}>
            {viewMode === 'study-detail' && selectedStudy ? (
              <StudyDetailView
                studyInstanceUID={selectedStudy.studyInstanceUID}
                onBack={handleBackFromDetail}
                onOpenViewer={handleOpenViewer}
              />
            ) : (
              <StudyExplorer
                filters={activeFilters}
                onViewStudy={handleViewStudy}
                onOpenViewer={handleOpenViewer}
              />
            )}
          </TabPanel>

          <TabPanel value={tabValue} index={2}>
            {viewerStudyUID && (
              <DicomViewer
                studyInstanceUID={viewerStudyUID}
                seriesInstanceUID={viewerSeriesUID}
                ohifBaseUrl={import.meta.env.VITE_OHIF_URL || 'http://localhost:3000'}
              />
            )}
          </TabPanel>
        </Container>
      </Box>

      <Dialog open={saveCohortOpen} onClose={() => setSaveCohortOpen(false)} maxWidth="sm" fullWidth>
        <DialogTitle>Save Cohort</DialogTitle>
        <DialogContent>
          <TextField
            autoFocus
            margin="dense"
            label="Cohort Name"
            fullWidth
            value={cohortName}
            onChange={(e) => setCohortName(e.target.value)}
            sx={{ mb: 2 }}
          />
          <TextField
            margin="dense"
            label="Description (optional)"
            fullWidth
            multiline
            rows={3}
            value={cohortDescription}
            onChange={(e) => setCohortDescription(e.target.value)}
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setSaveCohortOpen(false)}>Cancel</Button>
          <Button onClick={handleSaveCohort} variant="contained" disabled={!cohortName.trim()}>
            Save
          </Button>
        </DialogActions>
      </Dialog>

      <Snackbar
        open={snackbar.open}
        autoHideDuration={5000}
        onClose={() => setSnackbar((s) => ({ ...s, open: false }))}
      >
        <Alert severity={snackbar.severity} onClose={() => setSnackbar((s) => ({ ...s, open: false }))}>
          {snackbar.message}
        </Alert>
      </Snackbar>
    </ThemeProvider>
  );
}

export default App;
