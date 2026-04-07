import React, { useState, useEffect } from 'react';
import {
  Box,
  Paper,
  Typography,
  Chip,
  List,
  ListItemButton,
  ListItemText,
  ListItemIcon,
  Divider,
  CircularProgress,
  Alert,
  Button,
  IconButton,
  Collapse,
  Stack,
} from '@mui/material';
import PersonIcon from '@mui/icons-material/Person';
import CalendarTodayIcon from '@mui/icons-material/CalendarToday';
import LocalHospitalIcon from '@mui/icons-material/LocalHospital';
import ImageIcon from '@mui/icons-material/Image';
import ExpandLessIcon from '@mui/icons-material/ExpandLess';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import OpenInNewIcon from '@mui/icons-material/OpenInNew';
import ArrowBackIcon from '@mui/icons-material/ArrowBack';
import { format } from 'date-fns';
import {
  getStudyDetails,
  getSeriesInstances,
} from '../api/imagingApi';
import type {
  StudyDetails,
  Series,
  Instance,
} from '../api/imagingApi';

interface StudyDetailViewProps {
  studyInstanceUID: string;
  onBack: () => void;
  onOpenViewer: (studyInstanceUID: string, seriesInstanceUID?: string) => void;
}

const StudyDetailView: React.FC<StudyDetailViewProps> = ({
  studyInstanceUID,
  onBack,
  onOpenViewer,
}) => {
  const [study, setStudy] = useState<StudyDetails | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedSeries, setExpandedSeries] = useState<string | null>(null);
  const [seriesInstances, setSeriesInstances] = useState<Record<string, Instance[]>>({});
  const [loadingInstances, setLoadingInstances] = useState<string | null>(null);

  useEffect(() => {
    loadStudyDetails();
  }, [studyInstanceUID]);

  const loadStudyDetails = async () => {
    try {
      setLoading(true);
      setError(null);
      const details = await getStudyDetails(studyInstanceUID);
      setStudy(details);
    } catch (err) {
      setError('Failed to load study details');
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  const handleToggleSeries = async (seriesInstanceUID: string) => {
    if (expandedSeries === seriesInstanceUID) {
      setExpandedSeries(null);
      return;
    }

    setExpandedSeries(seriesInstanceUID);

    if (!seriesInstances[seriesInstanceUID]) {
      try {
        setLoadingInstances(seriesInstanceUID);
        const response = await getSeriesInstances(seriesInstanceUID);
        setSeriesInstances((prev) => ({
          ...prev,
          [seriesInstanceUID]: response.instances,
        }));
      } catch (err) {
        console.error('Failed to load instances:', err);
      } finally {
        setLoadingInstances(null);
      }
    }
  };

  const formatDate = (dateString: string | null): string => {
    if (!dateString) return '-';
    try {
      return format(new Date(dateString), 'MMMM dd, yyyy');
    } catch {
      return dateString;
    }
  };

  const getModalityColor = (modality: string): 'default' | 'primary' | 'secondary' | 'success' | 'warning' | 'info' => {
    const colors: Record<string, 'default' | 'primary' | 'secondary' | 'success' | 'warning' | 'info'> = {
      CT: 'primary',
      MR: 'secondary',
      CR: 'default',
      DX: 'default',
      US: 'info',
      PT: 'warning',
      NM: 'warning',
      MG: 'success',
    };
    return colors[modality] || 'default';
  };

  if (loading) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight={300}>
        <CircularProgress />
      </Box>
    );
  }

  if (error || !study) {
    return <Alert severity="error">{error || 'Study not found'}</Alert>;
  }

  return (
    <Box>
      <Box mb={2} display="flex" alignItems="center" gap={1}>
        <IconButton onClick={onBack}>
          <ArrowBackIcon />
        </IconButton>
        <Typography variant="h6">Study Details</Typography>
      </Box>

      <Stack spacing={3}>
        <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 3 }}>
          <Paper elevation={2} sx={{ p: 2, flex: '1 1 300px', minWidth: 280 }}>
            <Typography variant="subtitle2" color="text.secondary" gutterBottom>
              Patient Information
            </Typography>
            <Box display="flex" alignItems="center" gap={1} mb={1}>
              <PersonIcon color="action" />
              <Typography variant="h6">{study.patient.patientName || 'Unknown'}</Typography>
            </Box>
            <Typography variant="body2" color="text.secondary">
              Patient ID: <strong>{study.patient.patientId}</strong>
            </Typography>
            <Typography variant="body2" color="text.secondary">
              Sex: <strong>{study.patient.patientSex || '-'}</strong>
            </Typography>
            <Typography variant="body2" color="text.secondary">
              Birth Date: <strong>{formatDate(study.patient.patientBirthDate)}</strong>
            </Typography>
            {study.patient.patientAge && (
              <Typography variant="body2" color="text.secondary">
                Age: <strong>{study.patient.patientAge}</strong>
              </Typography>
            )}
          </Paper>

          <Paper elevation={2} sx={{ p: 2, flex: '2 1 500px', minWidth: 400 }}>
            <Typography variant="subtitle2" color="text.secondary" gutterBottom>
              Study Information
            </Typography>
            <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 3 }}>
              <Box sx={{ flex: '1 1 200px' }}>
                <Box display="flex" alignItems="center" gap={1} mb={1}>
                  <CalendarTodayIcon color="action" fontSize="small" />
                  <Typography variant="body2">
                    Study Date: <strong>{formatDate(study.studyDate)}</strong>
                  </Typography>
                </Box>
                <Typography variant="body2" color="text.secondary">
                  Accession: <strong>{study.accessionNumber || '-'}</strong>
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Study ID: <strong>{study.studyId || '-'}</strong>
                </Typography>
              </Box>
              <Box sx={{ flex: '1 1 200px' }}>
                <Box display="flex" alignItems="center" gap={1} mb={1}>
                  <LocalHospitalIcon color="action" fontSize="small" />
                  <Typography variant="body2">
                    Referring: <strong>{study.referringPhysician || '-'}</strong>
                  </Typography>
                </Box>
                <Typography variant="body2" color="text.secondary">
                  Series: <strong>{study.numberOfSeries}</strong>
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Instances: <strong>{study.numberOfInstances}</strong>
                </Typography>
              </Box>
            </Box>
            <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
              Description: <strong>{study.studyDescription || '-'}</strong>
            </Typography>
            <Box mt={2}>
              <Button
                variant="contained"
                startIcon={<OpenInNewIcon />}
                onClick={() => onOpenViewer(study.studyInstanceUID)}
              >
                Open in DICOM Viewer
              </Button>
            </Box>
          </Paper>
        </Box>

        <Paper elevation={2}>
          <Box p={2}>
            <Typography variant="subtitle2" color="text.secondary">
              Series ({study.series.length})
            </Typography>
          </Box>
          <Divider />
          <List disablePadding>
            {study.series.map((series: Series, index: number) => (
              <React.Fragment key={series.seriesKey}>
                {index > 0 && <Divider />}
                <ListItemButton onClick={() => handleToggleSeries(series.seriesInstanceUID)}>
                  <ListItemIcon>
                    <Chip
                      label={series.modality}
                      size="small"
                      color={getModalityColor(series.modality)}
                    />
                  </ListItemIcon>
                  <ListItemText
                    primary={
                      <Box display="flex" alignItems="center" gap={1}>
                        <Typography variant="body1">
                          Series {series.seriesNumber || '-'}
                        </Typography>
                        {series.bodyPartExamined && (
                          <Chip label={series.bodyPartExamined} size="small" variant="outlined" />
                        )}
                      </Box>
                    }
                    secondary={
                      <>
                        {series.seriesDescription || 'No description'}
                        {series.protocolName && ` • Protocol: ${series.protocolName}`}
                        {series.equipment.manufacturer && (
                          <> • {series.equipment.manufacturer} {series.equipment.modelName || ''}</>
                        )}
                      </>
                    }
                  />
                  <Typography variant="body2" color="text.secondary" sx={{ mr: 2 }}>
                    {series.numberOfInstances} images
                  </Typography>
                  <IconButton
                    size="small"
                    onClick={(e) => {
                      e.stopPropagation();
                      onOpenViewer(study.studyInstanceUID, series.seriesInstanceUID);
                    }}
                  >
                    <ImageIcon />
                  </IconButton>
                  {expandedSeries === series.seriesInstanceUID ? (
                    <ExpandLessIcon />
                  ) : (
                    <ExpandMoreIcon />
                  )}
                </ListItemButton>
                <Collapse
                  in={expandedSeries === series.seriesInstanceUID}
                  timeout="auto"
                  unmountOnExit
                >
                  <Box sx={{ pl: 8, pr: 2, pb: 2, bgcolor: 'grey.50' }}>
                    {loadingInstances === series.seriesInstanceUID ? (
                      <Box py={2} display="flex" justifyContent="center">
                        <CircularProgress size={24} />
                      </Box>
                    ) : (
                      <List dense>
                        {seriesInstances[series.seriesInstanceUID]?.slice(0, 20).map((instance) => (
                          <Box key={instance.instanceKey} sx={{ py: 0.5 }}>
                            <Typography variant="body2">
                              Instance {instance.instanceNumber || '-'}
                            </Typography>
                            <Typography variant="caption" color="text.secondary">
                              SOP: {instance.sopInstanceUID.substring(0, 30)}...
                              {instance.rows && instance.columns && (
                                <> • {instance.rows}x{instance.columns}</>
                              )}
                              {instance.numberOfFrames > 1 && (
                                <> • {instance.numberOfFrames} frames</>
                              )}
                            </Typography>
                          </Box>
                        ))}
                        {(seriesInstances[series.seriesInstanceUID]?.length || 0) > 20 && (
                          <Typography variant="caption" color="text.secondary" sx={{ pl: 2 }}>
                            ... and {(seriesInstances[series.seriesInstanceUID]?.length || 0) - 20} more instances
                          </Typography>
                        )}
                      </List>
                    )}
                  </Box>
                </Collapse>
              </React.Fragment>
            ))}
          </List>
        </Paper>
      </Stack>
    </Box>
  );
};

export default StudyDetailView;
