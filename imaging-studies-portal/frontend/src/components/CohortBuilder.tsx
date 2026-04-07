import React, { useState, useEffect } from 'react';
import {
  Box,
  Paper,
  Typography,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Chip,
  TextField,
  Button,
  CircularProgress,
  Alert,
  Divider,
  OutlinedInput,
  Stack,
} from '@mui/material';
import type { SelectChangeEvent } from '@mui/material';
import FilterListIcon from '@mui/icons-material/FilterList';
import ClearIcon from '@mui/icons-material/Clear';
import SearchIcon from '@mui/icons-material/Search';
import SaveIcon from '@mui/icons-material/Save';
import {
  getFilterOptions,
  previewCohort,
} from '../api/imagingApi';
import type {
  FilterOptions,
  CohortFilters,
  CohortPreview,
} from '../api/imagingApi';

interface CohortBuilderProps {
  onSearch: (filters: CohortFilters) => void;
  onSaveCohort?: (filters: CohortFilters) => void;
}

const CohortBuilder: React.FC<CohortBuilderProps> = ({ onSearch, onSaveCohort }) => {
  const [filterOptions, setFilterOptions] = useState<FilterOptions | null>(null);
  const [loading, setLoading] = useState(true);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [preview, setPreview] = useState<CohortPreview | null>(null);

  const [filters, setFilters] = useState<CohortFilters>({
    modalities: [],
    bodyParts: [],
    manufacturers: [],
    institutions: [],
    patientSex: [],
    studyDateFrom: undefined,
    studyDateTo: undefined,
    patientId: '',
    accessionNumber: '',
  });

  useEffect(() => {
    loadFilterOptions();
  }, []);

  useEffect(() => {
    const debounceTimer = setTimeout(() => {
      if (hasActiveFilters()) {
        loadPreview();
      } else {
        setPreview(null);
      }
    }, 500);

    return () => clearTimeout(debounceTimer);
  }, [filters]);

  const loadFilterOptions = async () => {
    try {
      setLoading(true);
      const options = await getFilterOptions();
      setFilterOptions(options);
    } catch (err) {
      setError('Failed to load filter options');
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  const loadPreview = async () => {
    try {
      setPreviewLoading(true);
      const previewData = await previewCohort(filters);
      setPreview(previewData);
    } catch (err) {
      console.error('Failed to load preview:', err);
    } finally {
      setPreviewLoading(false);
    }
  };

  const hasActiveFilters = (): boolean => {
    return (
      (filters.modalities?.length ?? 0) > 0 ||
      (filters.bodyParts?.length ?? 0) > 0 ||
      (filters.manufacturers?.length ?? 0) > 0 ||
      (filters.institutions?.length ?? 0) > 0 ||
      (filters.patientSex?.length ?? 0) > 0 ||
      !!filters.studyDateFrom ||
      !!filters.studyDateTo ||
      !!filters.patientId ||
      !!filters.accessionNumber
    );
  };

  const handleMultiSelectChange = (field: keyof CohortFilters) => (
    event: SelectChangeEvent<string[]>
  ) => {
    const value = event.target.value;
    setFilters((prev) => ({
      ...prev,
      [field]: typeof value === 'string' ? value.split(',') : value,
    }));
  };

  const handleTextChange = (field: keyof CohortFilters) => (
    event: React.ChangeEvent<HTMLInputElement>
  ) => {
    setFilters((prev) => ({
      ...prev,
      [field]: event.target.value,
    }));
  };

  const handleDateChange = (field: 'studyDateFrom' | 'studyDateTo') => (
    event: React.ChangeEvent<HTMLInputElement>
  ) => {
    setFilters((prev) => ({
      ...prev,
      [field]: event.target.value || undefined,
    }));
  };

  const handleClearFilters = () => {
    setFilters({
      modalities: [],
      bodyParts: [],
      manufacturers: [],
      institutions: [],
      patientSex: [],
      studyDateFrom: undefined,
      studyDateTo: undefined,
      patientId: '',
      accessionNumber: '',
    });
    setPreview(null);
  };

  const handleSearch = () => {
    onSearch(filters);
  };

  const handleSave = () => {
    if (onSaveCohort) {
      onSaveCohort(filters);
    }
  };

  if (loading) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight={200}>
        <CircularProgress />
      </Box>
    );
  }

  if (error) {
    return <Alert severity="error">{error}</Alert>;
  }

  return (
    <Paper elevation={2} sx={{ p: 3 }}>
      <Box display="flex" alignItems="center" mb={2}>
        <FilterListIcon sx={{ mr: 1 }} />
        <Typography variant="h6">Cohort Builder</Typography>
      </Box>

      <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 2, mb: 2 }}>
        <Box sx={{ flex: '1 1 200px', minWidth: 200 }}>
          <FormControl fullWidth size="small">
            <InputLabel>Modality</InputLabel>
            <Select
              multiple
              value={filters.modalities || []}
              onChange={handleMultiSelectChange('modalities')}
              input={<OutlinedInput label="Modality" />}
              renderValue={(selected) => (
                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                  {selected.map((value) => (
                    <Chip key={value} label={value} size="small" />
                  ))}
                </Box>
              )}
            >
              {filterOptions?.modalities.map((opt) => (
                <MenuItem key={opt.value} value={opt.value}>
                  {opt.value} ({opt.count})
                </MenuItem>
              ))}
            </Select>
          </FormControl>
        </Box>

        <Box sx={{ flex: '1 1 200px', minWidth: 200 }}>
          <FormControl fullWidth size="small">
            <InputLabel>Body Part</InputLabel>
            <Select
              multiple
              value={filters.bodyParts || []}
              onChange={handleMultiSelectChange('bodyParts')}
              input={<OutlinedInput label="Body Part" />}
              renderValue={(selected) => (
                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                  {selected.map((value) => (
                    <Chip key={value} label={value} size="small" />
                  ))}
                </Box>
              )}
            >
              {filterOptions?.bodyParts.map((opt) => (
                <MenuItem key={opt.value} value={opt.value}>
                  {opt.value} ({opt.count})
                </MenuItem>
              ))}
            </Select>
          </FormControl>
        </Box>

        <Box sx={{ flex: '1 1 200px', minWidth: 200 }}>
          <FormControl fullWidth size="small">
            <InputLabel>Manufacturer</InputLabel>
            <Select
              multiple
              value={filters.manufacturers || []}
              onChange={handleMultiSelectChange('manufacturers')}
              input={<OutlinedInput label="Manufacturer" />}
              renderValue={(selected) => (
                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                  {selected.map((value) => (
                    <Chip key={value} label={value} size="small" />
                  ))}
                </Box>
              )}
            >
              {filterOptions?.manufacturers.map((opt) => (
                <MenuItem key={opt.value} value={opt.value}>
                  {opt.value} ({opt.count})
                </MenuItem>
              ))}
            </Select>
          </FormControl>
        </Box>

        <Box sx={{ flex: '1 1 200px', minWidth: 200 }}>
          <FormControl fullWidth size="small">
            <InputLabel>Institution</InputLabel>
            <Select
              multiple
              value={filters.institutions || []}
              onChange={handleMultiSelectChange('institutions')}
              input={<OutlinedInput label="Institution" />}
              renderValue={(selected) => (
                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                  {selected.map((value) => (
                    <Chip key={value} label={value} size="small" />
                  ))}
                </Box>
              )}
            >
              {filterOptions?.institutions.map((opt) => (
                <MenuItem key={opt.value} value={opt.value}>
                  {opt.value} ({opt.count})
                </MenuItem>
              ))}
            </Select>
          </FormControl>
        </Box>

        <Box sx={{ flex: '1 1 200px', minWidth: 200 }}>
          <TextField
            fullWidth
            size="small"
            type="date"
            label="Study Date From"
            InputLabelProps={{ shrink: true }}
            value={filters.studyDateFrom || ''}
            onChange={handleDateChange('studyDateFrom')}
          />
        </Box>

        <Box sx={{ flex: '1 1 200px', minWidth: 200 }}>
          <TextField
            fullWidth
            size="small"
            type="date"
            label="Study Date To"
            InputLabelProps={{ shrink: true }}
            value={filters.studyDateTo || ''}
            onChange={handleDateChange('studyDateTo')}
          />
        </Box>

        <Box sx={{ flex: '1 1 200px', minWidth: 200 }}>
          <FormControl fullWidth size="small">
            <InputLabel>Patient Sex</InputLabel>
            <Select
              multiple
              value={filters.patientSex || []}
              onChange={handleMultiSelectChange('patientSex')}
              input={<OutlinedInput label="Patient Sex" />}
              renderValue={(selected) => (
                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                  {selected.map((value) => (
                    <Chip key={value} label={value} size="small" />
                  ))}
                </Box>
              )}
            >
              {filterOptions?.patientSex.map((opt) => (
                <MenuItem key={opt.value} value={opt.value}>
                  {opt.value} ({opt.count})
                </MenuItem>
              ))}
            </Select>
          </FormControl>
        </Box>

        <Box sx={{ flex: '1 1 200px', minWidth: 200 }}>
          <TextField
            fullWidth
            size="small"
            label="Patient ID"
            value={filters.patientId || ''}
            onChange={handleTextChange('patientId')}
          />
        </Box>

        <Box sx={{ flex: '1 1 200px', minWidth: 200 }}>
          <TextField
            fullWidth
            size="small"
            label="Accession Number"
            value={filters.accessionNumber || ''}
            onChange={handleTextChange('accessionNumber')}
          />
        </Box>
      </Box>

      <Divider sx={{ my: 2 }} />

      {preview && (
        <Box mb={2}>
          <Typography variant="subtitle2" color="text.secondary" gutterBottom>
            Cohort Preview {previewLoading && <CircularProgress size={14} sx={{ ml: 1 }} />}
          </Typography>
          <Stack direction="row" spacing={2} flexWrap="wrap" useFlexGap>
            <Paper variant="outlined" sx={{ p: 1.5, textAlign: 'center', minWidth: 100 }}>
              <Typography variant="h5" color="primary">
                {preview.counts.patients.toLocaleString()}
              </Typography>
              <Typography variant="caption" color="text.secondary">
                Patients
              </Typography>
            </Paper>
            <Paper variant="outlined" sx={{ p: 1.5, textAlign: 'center', minWidth: 100 }}>
              <Typography variant="h5" color="primary">
                {preview.counts.studies.toLocaleString()}
              </Typography>
              <Typography variant="caption" color="text.secondary">
                Studies
              </Typography>
            </Paper>
            <Paper variant="outlined" sx={{ p: 1.5, textAlign: 'center', minWidth: 100 }}>
              <Typography variant="h5" color="primary">
                {preview.counts.series.toLocaleString()}
              </Typography>
              <Typography variant="caption" color="text.secondary">
                Series
              </Typography>
            </Paper>
            <Paper variant="outlined" sx={{ p: 1.5, textAlign: 'center', minWidth: 100 }}>
              <Typography variant="h5" color="primary">
                {preview.counts.instances.toLocaleString()}
              </Typography>
              <Typography variant="caption" color="text.secondary">
                Instances
              </Typography>
            </Paper>
          </Stack>
        </Box>
      )}

      <Box display="flex" gap={1} justifyContent="flex-end">
        <Button
          variant="outlined"
          startIcon={<ClearIcon />}
          onClick={handleClearFilters}
          disabled={!hasActiveFilters()}
        >
          Clear
        </Button>
        {onSaveCohort && (
          <Button
            variant="outlined"
            startIcon={<SaveIcon />}
            onClick={handleSave}
            disabled={!hasActiveFilters()}
          >
            Save Cohort
          </Button>
        )}
        <Button
          variant="contained"
          startIcon={<SearchIcon />}
          onClick={handleSearch}
          disabled={!hasActiveFilters()}
        >
          Search Studies
        </Button>
      </Box>
    </Paper>
  );
};

export default CohortBuilder;
