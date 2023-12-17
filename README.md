# data-api
API that allows for uploading of spreadsheet to azure blob storage and allows for querying and return in json format

# Security
users must provide a API Key in the header called 'x-api-key', which should match the key in the environment, otherwise they will be denied access.
