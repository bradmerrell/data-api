const express = require('express');
const multer = require('multer');
const cors = require('cors');
const XLSX = require('xlsx');


require('dotenv').config();
const { BlobServiceClient, ContainerClient } = require('@azure/storage-blob');

const app = express();
const upload = multer(); // for parsing multipart/form-data
const port = process.env.PORT || 3000;
const connectionString = process.env.AZURE_STORAGE_CONNECTION_STRING;

    // Enable CORS for client-side
    app.use(cors());

    app.post('/api/data', upload.single('file'), async (req, res) => {
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

  app.get('/api/data', async (req, res) => {
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