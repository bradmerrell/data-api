const express = require('express');
const multer = require('multer');
const cors = require('cors');
const XLSX = require('xlsx');
const swaggerUi = require('swagger-ui-express');
const swaggerJSDoc = require('swagger-jsdoc');

require('dotenv').config();
const { BlobServiceClient, ContainerClient } = require('@azure/storage-blob');

const app = express();
const upload = multer(); // for parsing multipart/form-data
const port = process.env.PORT || 3000;
const connectionString = process.env.AZURE_STORAGE_CONNECTION_STRING;

// Enable CORS for client-side
app.use(cors());

// Swagger definition
const swaggerDefinition = {
  openapi: '3.0.0',
  info: {
    title: 'Data API',
    version: '1.0.0',
    description: 'API that allows for uploading of spreadsheet to azure blob storage and allows for querying and return in json format',
  },
  servers: [
    {
      url: 'http://localhost:' + port,
      description: 'Local server',
    },
    {
      url: 'http://houbuildspreadsheetapi.azurewebsites.net',
      description: 'Azure',
    },
  ],
};

// Options for the swagger docs
const options = {
  swaggerDefinition,  
  apis: ['./index.js'], // Paths to files containing OpenAPI definitions
};

// Initialize swagger-jsdoc
const swaggerSpec = swaggerJSDoc(options);

// Serve Swagger
app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(swaggerSpec));

// Middleware to check the API key
function apiKeyCheck(req, res, next) {
  const apiKey = process.env.MY_API_KEY;
  const requestApiKey = req.headers['x-api-key'];
  console.log ("API Key:", apiKey);
  console.log ("Request API Key:", requestApiKey);

  if (!apiKey || requestApiKey !== apiKey) {
    res.status(401).send('Unauthorized: Invalid API key');
    return;
  }

  next();
}

// Apply the middleware
app.use(apiKeyCheck);

/**
 * @swagger
 * /upload:
 *   post:
 *     summary: Upload a spreadsheet to Azure Blob Storage
 *     requestBody:
 *       required: true
 *       content:
 *         multipart/form-data:
 *           schema:
 *             type: object
 *             properties:
 *               file:
 *                 type: string
 *                 format: binary
 *     responses:
 *       200:
 *         description: Successfully uploaded
 */
app.post('/upload', upload.single('file'), async (req, res) => {
    if (!req.file) {
    return res.status(400).send('No file uploaded.');
    }

    const containerName = 'data';
    //const blobName = req.file.originalname;
    const blobName = "spreadsheet.xlsx"
    const blobServiceClient = BlobServiceClient.fromConnectionString(connectionString);
    const containerClient = blobServiceClient.getContainerClient(containerName);

    const blockBlobClient = containerClient.getBlockBlobClient(blobName);
    const uploadBlobResponse = await blockBlobClient.upload(req.file.buffer, req.file.size);

    res.status(200).send(`File uploaded successfully. Upload block blob response: ${uploadBlobResponse.requestId}`);
});

/**
 * @swagger
 * /data:
 *   get:
 *     summary: Query data and return in JSON format
 *     responses:
 *       200:
 *         description: Data retrieved successfully
 */
app.get('/data', async (req, res) => {
  const sheetName = req.query.tab;
  const blobName = "spreadsheet.xlsx"
  const connectionString = process.env.AZURE_STORAGE_CONNECTION_STRING;
  const containerName = 'data';
  const blobServiceClient = BlobServiceClient.fromConnectionString(connectionString);
  const containerClient = blobServiceClient.getContainerClient(containerName);
  const blobClient = containerClient.getBlobClient(blobName);

  try {
      const downloadBlockBlobResponse = await blobClient.download(0);
      const buffer = await streamToBuffer(downloadBlockBlobResponse.readableStreamBody);
      const workbook = XLSX.read(buffer, { type: 'buffer' });
      const worksheet = workbook.Sheets[sheetName];        

      if (!worksheet) {
          throw new Error(`Tab "${sheetName}" not found in the spreadsheet.`);
      }

      if (sheetName) {
          // If the 'tab' query parameter is provided, use it

          let worksheetData = XLSX.utils.sheet_to_json(worksheet);
          // Transform data
          const transformedData = transformData(worksheetData);

          // Filter data
          delete req.query.tab;
          const filteredData = filterData(transformedData, req.query);

          // Return data
          res.status(200).json(filteredData);
        } else {
          // Handle the case where 'tab' query parameter is not provided
          res.status(400).send('Tab query parameter is required');
        }
  } catch (error) {
    res.status(500).send(error.message);
  }
});

// Function to filter data based on query parameters
function filterData(data, queryParams) {
    return data.filter(row => {
        return Object.keys(queryParams).every(field => {
            if (field != "tab")
            {
                const fieldValues = queryParams[field].split(',');
                return fieldValues.some(value => row[field] === value);
            }                 
        });
    });
}

function transformData(spreadsheetData) {
    return spreadsheetData.map(row => {
        // Remove "Ext. at " from "Primary Opp" and split it   
        var client = '';             
        var project = '';
        if (row['Primary Opp'])
        {
            const primaryOpp = (row['Primary Opp'] || '').replace(/^Ext\. at /, '');
            const segments = primaryOpp.split(' - ');
            if (segments.length >= 2)
            {
                client = segments[0];
                project = segments[1];
            }
        }

        // Convert "Name" format
        if (row['Name'])
        {
            const [lastName, firstName] = (row['Name'] || '').split(', ');                
            row['Name'] = firstName ? `${firstName} ${lastName}` : '';
        }

        // Convert "Primary Owner" format
        if (row['Primary Owner'])
        {                
            const [ownerLastName, ownerFirstName] = (row['Primary Owner'] || '').split(', ');                
            row['Primary Owner'] = ownerFirstName ? `${ownerFirstName} ${ownerLastName}` : '';
        }

        
        return {
            ...row,
            'Client': client,
            'Project': project
        };
    });
}

async function streamToBuffer(readableStream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    readableStream.on('data', (data) => {
      chunks.push(data instanceof Buffer ? data : Buffer.from(data));
    });
    readableStream.on('end', () => {
      resolve(Buffer.concat(chunks));
    });
    readableStream.on('error', reject);
  });
}

  
app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});