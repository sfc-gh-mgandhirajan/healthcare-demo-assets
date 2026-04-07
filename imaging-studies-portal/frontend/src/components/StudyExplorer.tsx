import React, { useState } from 'react';
import {
  Box,
  Paper,
  Typography,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TablePagination,
  TableSortLabel,
  Chip,
  IconButton,
  CircularProgress,
  Alert,
  Tooltip,
} from '@mui/material';
import VisibilityIcon from '@mui/icons-material/Visibility';
import ImageIcon from '@mui/icons-material/Image';
import PersonIcon from '@mui/icons-material/Person';
import { format } from 'date-fns';
import { getCohortStudies } from '../api/imagingApi';
import type { Study, StudyListResponse, CohortFilters } from '../api/imagingApi';

interface StudyExplorerProps {
  filters: CohortFilters;
  onViewStudy: (study: Study) => void;
  onOpenViewer: (studyInstanceUID: string) => void;
}

type SortField = 'study_date' | 'patient_name' | 'modality' | 'study_description' | 'accession_number';
type SortOrder = 'asc' | 'desc';

const StudyExplorer: React.FC<StudyExplorerProps> = ({ filters, onViewStudy, onOpenViewer }) => {
  const [studies, setStudies] = useState<Study[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(25);
  const [totalCount, setTotalCount] = useState(0);
  const [sortBy, setSortBy] = useState<SortField>('study_date');
  const [sortOrder, setSortOrder] = useState<SortOrder>('desc');
  const [initialized, setInitialized] = useState(false);

  React.useEffect(() => {
    if (Object.keys(filters).some((key) => {
      const value = filters[key as keyof CohortFilters];
      return Array.isArray(value) ? value.length > 0 : !!value;
    })) {
      loadStudies();
      setInitialized(true);
    }
  }, [filters, page, rowsPerPage, sortBy, sortOrder]);

  const loadStudies = async () => {
    try {
      setLoading(true);
      setError(null);
      const response: StudyListResponse = await getCohortStudies(
        filters,
        page + 1,
        rowsPerPage,
        sortBy,
        sortOrder
      );
      setStudies(response.studies);
      setTotalCount(response.pagination.totalCount);
    } catch (err) {
      setError('Failed to load studies');
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  const handleChangePage = (_: unknown, newPage: number) => {
    setPage(newPage);
  };

  const handleChangeRowsPerPage = (event: React.ChangeEvent<HTMLInputElement>) => {
    setRowsPerPage(parseInt(event.target.value, 10));
    setPage(0);
  };

  const handleSort = (field: SortField) => {
    if (sortBy === field) {
      setSortOrder(sortOrder === 'asc' ? 'desc' : 'asc');
    } else {
      setSortBy(field);
      setSortOrder('desc');
    }
    setPage(0);
  };

  const formatDate = (dateString: string | null): string => {
    if (!dateString) return '-';
    try {
      return format(new Date(dateString), 'MMM dd, yyyy');
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

  if (!initialized) {
    return (
      <Paper elevation={2} sx={{ p: 3, textAlign: 'center' }}>
        <Typography color="text.secondary">
          Use the Cohort Builder above to search for imaging studies
        </Typography>
      </Paper>
    );
  }

  if (error) {
    return <Alert severity="error">{error}</Alert>;
  }

  return (
    <Paper elevation={2}>
      <Box p={2} display="flex" justifyContent="space-between" alignItems="center">
        <Typography variant="h6">
          Study Explorer
          {totalCount > 0 && (
            <Typography component="span" variant="body2" color="text.secondary" sx={{ ml: 1 }}>
              ({totalCount.toLocaleString()} studies)
            </Typography>
          )}
        </Typography>
        {loading && <CircularProgress size={24} />}
      </Box>

      <TableContainer>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>
                <TableSortLabel
                  active={sortBy === 'study_date'}
                  direction={sortBy === 'study_date' ? sortOrder : 'desc'}
                  onClick={() => handleSort('study_date')}
                >
                  Study Date
                </TableSortLabel>
              </TableCell>
              <TableCell>
                <TableSortLabel
                  active={sortBy === 'patient_name'}
                  direction={sortBy === 'patient_name' ? sortOrder : 'desc'}
                  onClick={() => handleSort('patient_name')}
                >
                  Patient
                </TableSortLabel>
              </TableCell>
              <TableCell>Patient ID</TableCell>
              <TableCell>
                <TableSortLabel
                  active={sortBy === 'accession_number'}
                  direction={sortBy === 'accession_number' ? sortOrder : 'desc'}
                  onClick={() => handleSort('accession_number')}
                >
                  Accession #
                </TableSortLabel>
              </TableCell>
              <TableCell>
                <TableSortLabel
                  active={sortBy === 'modality'}
                  direction={sortBy === 'modality' ? sortOrder : 'desc'}
                  onClick={() => handleSort('modality')}
                >
                  Modalities
                </TableSortLabel>
              </TableCell>
              <TableCell>
                <TableSortLabel
                  active={sortBy === 'study_description'}
                  direction={sortBy === 'study_description' ? sortOrder : 'desc'}
                  onClick={() => handleSort('study_description')}
                >
                  Description
                </TableSortLabel>
              </TableCell>
              <TableCell align="center">Series</TableCell>
              <TableCell align="center">Images</TableCell>
              <TableCell align="center">Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {studies.length === 0 && !loading ? (
              <TableRow>
                <TableCell colSpan={9} align="center">
                  <Typography color="text.secondary" py={3}>
                    No studies found matching your criteria
                  </Typography>
                </TableCell>
              </TableRow>
            ) : (
              studies.map((study) => (
                <TableRow
                  key={study.studyKey}
                  hover
                  sx={{ cursor: 'pointer' }}
                  onClick={() => onViewStudy(study)}
                >
                  <TableCell>{formatDate(study.studyDate)}</TableCell>
                  <TableCell>
                    <Box display="flex" alignItems="center" gap={1}>
                      <PersonIcon fontSize="small" color="action" />
                      {study.patientName || '-'}
                    </Box>
                  </TableCell>
                  <TableCell>
                    <Typography variant="body2" fontFamily="monospace">
                      {study.patientId}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Typography variant="body2" fontFamily="monospace">
                      {study.accessionNumber || '-'}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Box display="flex" gap={0.5} flexWrap="wrap">
                      {study.modalities.map((mod) => (
                        <Chip
                          key={mod}
                          label={mod}
                          size="small"
                          color={getModalityColor(mod)}
                          variant="outlined"
                        />
                      ))}
                    </Box>
                  </TableCell>
                  <TableCell>
                    <Typography
                      variant="body2"
                      sx={{
                        maxWidth: 200,
                        overflow: 'hidden',
                        textOverflow: 'ellipsis',
                        whiteSpace: 'nowrap',
                      }}
                    >
                      {study.studyDescription || '-'}
                    </Typography>
                  </TableCell>
                  <TableCell align="center">{study.seriesCount}</TableCell>
                  <TableCell align="center">{study.instanceCount}</TableCell>
                  <TableCell align="center">
                    <Box display="flex" gap={0.5} justifyContent="center">
                      <Tooltip title="View Details">
                        <IconButton
                          size="small"
                          onClick={(e) => {
                            e.stopPropagation();
                            onViewStudy(study);
                          }}
                        >
                          <VisibilityIcon fontSize="small" />
                        </IconButton>
                      </Tooltip>
                      <Tooltip title="Open in Viewer">
                        <IconButton
                          size="small"
                          color="primary"
                          onClick={(e) => {
                            e.stopPropagation();
                            onOpenViewer(study.studyInstanceUID);
                          }}
                        >
                          <ImageIcon fontSize="small" />
                        </IconButton>
                      </Tooltip>
                    </Box>
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </TableContainer>

      <TablePagination
        component="div"
        count={totalCount}
        page={page}
        onPageChange={handleChangePage}
        rowsPerPage={rowsPerPage}
        onRowsPerPageChange={handleChangeRowsPerPage}
        rowsPerPageOptions={[10, 25, 50, 100]}
      />
    </Paper>
  );
};

export default StudyExplorer;
