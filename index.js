const express = require('express');
const multer = require('multer');
const cors = require('cors');
const XLSX = require('xlsx');
const swaggerUi = require('swagger-ui-express');
const swaggerJSDoc = require('swagger-jsdoc');
const { BlobServiceClient, ContainerClient } = require('@azure/storage-blob');
const { DefaultAzureCredential } = require("@azure/identity");
require('dotenv').config();

const app = express();
const upload = multer(); // for parsing multipart/form-data
const port = process.env.PORT || 3000;

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
    
    // Get BlobClient based on local or Azure
    let blobServiceClient;

    if (process.env.AZURE_STORAGE_CONNECTION_STRING) {
        // Local development - use connection string
        blobServiceClient = BlobServiceClient.fromConnectionString(process.env.AZURE_STORAGE_CONNECTION_STRING);
    } else {
        // Azure environment - use Managed Identity
        const credentials = new DefaultAzureCredential();
        const accountName = process.env.AZURE_STORAGE_ACCOUNT;
        const url = `https://${accountName}.blob.core.windows.net`;
        blobServiceClient = new BlobServiceClient(url, credentials);
    }    

    const containerName = 'data';
    //const blobName = req.file.originalname;
    const blobName = "spreadsheet.xlsx"
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
  let blobServiceClient;

  if (process.env.AZURE_STORAGE_CONNECTION_STRING) {
      // Local development - use connection string
      blobServiceClient = BlobServiceClient.fromConnectionString(process.env.AZURE_STORAGE_CONNECTION_STRING);
  } else {
      // Azure environment - use Managed Identity
      const credentials = new DefaultAzureCredential();
      const accountName = process.env.AZURE_STORAGE_ACCOUNT;
      const url = `https://${accountName}.blob.core.windows.net`;
      blobServiceClient = new BlobServiceClient(url, credentials);
  }

  const sheetName = req.query.tab;
  const blobName = "spreadsheet.xlsx"
  const containerName = 'data';
  
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

/**
 * @swagger
 * /lastmodified:
 *   get:
 *     summary: Get the "Last Modified" date of the file "spreadsheet.xlsx"
 *     responses:
 *       200:
 *         description: Last Modified date returned successfully
 *       404:
 *         description: File not found
 */
app.get('/lastmodified', async (req, res) => {
  let blobServiceClient;

  if (process.env.AZURE_STORAGE_CONNECTION_STRING) {
      // Local development - use connection string
      blobServiceClient = BlobServiceClient.fromConnectionString(process.env.AZURE_STORAGE_CONNECTION_STRING);
  } else {
      // Azure environment - use Managed Identity
      const credentials = new DefaultAzureCredential();
      const accountName = process.env.AZURE_STORAGE_ACCOUNT;
      const url = `https://${accountName}.blob.core.windows.net`;
      blobServiceClient = new BlobServiceClient(url, credentials);
  }

  const containerName = 'data';
  const blobName = "spreadsheet.xlsx";
  const containerClient = blobServiceClient.getContainerClient(containerName);
  const blobClient = containerClient.getBlobClient(blobName);

  try {
      const properties = await blobClient.getProperties();
      const lastModified = properties.lastModified;
      res.status(200).send({ lastModified: lastModified.toISOString() });
  } catch (error) {
      if (error.statusCode === 404) {
          res.status(404).send('File not found');
      } else {
          res.status(500).send(error.message);
      }
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
        var next_client_project = '';
        var next_client = '';             
        var next_project = '';
        if (row['Primary Opp'])
        {
            next_client_project = (row['Primary Opp'] || '').replace(/^Ext\. at /, '');
            const segments = next_client_project.split(' - ');
            if (segments.length >= 2)
            {
              next_client = segments[0];
              next_project = segments[1];
            }
        }

        var current_client = '';             
        var current_project = '';
        var current_client_project = '';
        if (row['Current Eng.'])
        {
            current_client_project = (row['Current Eng.'] || '').replace(/^Ext\. at /, '');
            const segments = current_client_project.split(' - ');
            if (segments.length >= 2)
            {
              current_client = segments[0];
              current_project = segments[1];
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

        // Convert "Current Owner" format
        if (row['Current Owner'])
        {                
            const [ownerLastName, ownerFirstName] = (row['Current Owner'] || '').split(', ');                
            row['Current Owner'] = ownerFirstName ? `${ownerFirstName} ${ownerLastName}` : '';
        }

        // Convert "Manager" format
        if (row['Manager'])
        {                
            const [ownerLastName, ownerFirstName] = (row['Manager'] || '').split(', ');                
            row['Manager'] = ownerFirstName ? `${ownerFirstName} ${ownerLastName}` : '';
        }
        
        return {
            ...row,
            'ClientProject': current_client_project,
            'Client': current_client,
            'Project': current_project,
            'NextClientOpp': next_client_project,
            'NextClient': next_client,
            'NextProject': next_project  
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

/**
 * @swagger
 * /healthcheck:
 *   get:
 *     summary: returns a success, if working properly
 *     responses:
 *       200:
 *         description: Everything is working properyly
 */
app.get('/healthcheck', async (req, res) => {
  try {     
    res.status(200).send("Everthing is working okay");
  } catch (error) {
    res.status(500).send(error.message);
  }
});

  
app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});