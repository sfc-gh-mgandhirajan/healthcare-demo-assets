import React from 'react';
import { Box, Paper, Typography, Button, CircularProgress } from '@mui/material';
import OpenInNewIcon from '@mui/icons-material/OpenInNew';

interface DicomViewerProps {
  studyInstanceUID: string;
  seriesInstanceUID?: string;
  ohifBaseUrl?: string;
}

const DicomViewer: React.FC<DicomViewerProps> = ({
  studyInstanceUID,
  seriesInstanceUID,
  ohifBaseUrl = 'http://localhost:3000',
}) => {
  const [loading, setLoading] = React.useState(true);

  const viewerUrl = React.useMemo(() => {
    let url = `${ohifBaseUrl}/viewer?StudyInstanceUIDs=${studyInstanceUID}`;
    if (seriesInstanceUID) {
      url += `&SeriesInstanceUID=${seriesInstanceUID}`;
    }
    return url;
  }, [studyInstanceUID, seriesInstanceUID, ohifBaseUrl]);

  const handleOpenInNewTab = () => {
    window.open(viewerUrl, '_blank');
  };

  return (
    <Paper elevation={2} sx={{ height: '100%', minHeight: 600 }}>
      <Box
        p={2}
        display="flex"
        justifyContent="space-between"
        alignItems="center"
        borderBottom="1px solid"
        borderColor="divider"
      >
        <Typography variant="h6">DICOM Viewer</Typography>
        <Button
          variant="outlined"
          size="small"
          startIcon={<OpenInNewIcon />}
          onClick={handleOpenInNewTab}
        >
          Open in New Tab
        </Button>
      </Box>
      <Box sx={{ position: 'relative', height: 'calc(100% - 64px)' }}>
        {loading && (
          <Box
            sx={{
              position: 'absolute',
              top: '50%',
              left: '50%',
              transform: 'translate(-50%, -50%)',
              zIndex: 1,
            }}
          >
            <CircularProgress />
          </Box>
        )}
        <iframe
          src={viewerUrl}
          style={{
            width: '100%',
            height: '100%',
            border: 'none',
          }}
          onLoad={() => setLoading(false)}
          title="OHIF DICOM Viewer"
        />
      </Box>
    </Paper>
  );
};

export default DicomViewer;
