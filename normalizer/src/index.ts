import express, { Request, Response } from 'express';
import cors from 'cors';
import multer from 'multer';
import dotenv from 'dotenv';
import { handleIngestUpserts } from './handlers/ingestUpserts';
import { handleIngestDeltas } from './handlers/ingestDeltas';
import { handleEmail } from './handlers/email';

// Load environment variables
dotenv.config();

// Create Express app
const app = express();
const port = process.env.PORT || 8080;

// Configure middleware
app.use(cors());
app.use(express.json({ limit: '10mb' })); // 10MB limit for JSON payloads
app.use(express.urlencoded({ extended: true }));

// Configure multer for file uploads
const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 50 * 1024 * 1024, // 50MB max file size
  },
  fileFilter: (_req, file, cb) => {
    // Accept CSV, Excel, JSON files
    const allowedMimes = [
      'text/csv',
      'application/vnd.ms-excel',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      'application/json',
      'application/octet-stream', // For files with unknown mimetype
    ];
    
    const allowedExts = ['.csv', '.xls', '.xlsx', '.json', '.ndjson'];
    const hasValidExt = allowedExts.some(ext => file.originalname.toLowerCase().endsWith(ext));
    
    if (allowedMimes.includes(file.mimetype) || hasValidExt) {
      cb(null, true);
    } else {
      cb(new Error('Invalid file type. Only CSV, Excel, and JSON files are allowed.'));
    }
  },
});

// Health check endpoint
app.get('/health', (_req: Request, res: Response) => {
  res.json({ 
    status: 'ok',
    service: 'normalizer',
    version: '1.0.0',
    timestamp: new Date().toISOString()
  });
});

// Routes
app.post('/ingest/kafka/upserts', handleIngestUpserts);
app.post('/ingest/kafka/deltas', handleIngestDeltas);
app.post('/ingest/email', upload.single('file'), handleEmail);

// Error handling middleware
app.use((err: Error, _req: Request, res: Response, _next: any) => {
  console.error('Error:', err);
  res.status(500).json({ 
    error: 'Internal server error',
    message: err.message
  });
});

// 404 handler
app.use((_req: Request, res: Response) => {
  res.status(404).json({ error: 'Not found' });
});

// Start server
app.listen(port, () => {
  console.log(`🚀 Normalizer service running on port ${port}`);
  console.log(`   Health check: http://localhost:${port}/health`);
  console.log(`   Kafka upserts: POST http://localhost:${port}/ingest/kafka/upserts`);
  console.log(`   Kafka deltas: POST http://localhost:${port}/ingest/kafka/deltas`);
  console.log(`   Email upload: POST http://localhost:${port}/ingest/email`);
});

// Handle graceful shutdown
process.on('SIGTERM', () => {
  console.log('SIGTERM signal received: closing HTTP server');
  process.exit(0);
});

process.on('SIGINT', () => {
  console.log('SIGINT signal received: closing HTTP server');
  process.exit(0);
});
