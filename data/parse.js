const csv = require('csvtojson');
const fs = require('fs');
const _ = require('lodash');
const moment = require('moment');

const csvFilePath = './data.csv'; 
const outputFilePath = './output_file.json'; // Path for the output JSON file

csv()
  .fromFile(csvFilePath)
  .then((jsonObj) => {
    // Writing JSON array to a file

    _.forEach(jsonObj, obj => {
        obj.Tags = obj.Tags.split(',');
        obj.DateConnectedUnix = moment(obj.DateConnected).unix();
        obj.DateConnectedStr = moment(obj.DateConnected).format('MMM DD, YYYY');
    })

    fs.writeFile(outputFilePath, JSON.stringify(jsonObj, null, 2), (err) => {
      if (err) {
        console.error('Error writing JSON to file:', err);
      } else {
        console.log('JSON file has been saved.');
      }
    });
  })
  .catch((error) => {
    console.error('Error processing CSV file:', error);
  });
