const fs = require("fs").promises;
const http = require("http");
const fetch = require('node-fetch');

async function getCollections() {
  
  // Staging API 
  fetch("https://marketplace-staging.api.yesports.gg/collection", {
    "method": "GET"
  }).then(res => {return res.json()}).then(async response => {
      await fs.writeFile('./utils/test_collections.json', JSON.stringify(response, null, 3));
  });
  
  // Prod API
  fetch("https://marketplace.api.yesports.gg/collection", {
    "method": "GET"
  }).then(res => {return res.json()}).then(async response => {
      await fs.writeFile('./utils/collections.json', JSON.stringify(response, null, 3));
  });
}

// Main entrypoint when called directly
if (require.main === module) {
  getCollections();
}

module.exports.getCollections = getCollections;