require('dotenv').config();

const _ = require('lodash');
const algoliasearch = require('algoliasearch');
const async = require('async');
const fs = require('fs');
const csv = require('csvtojson');
const moment = require('moment');
const { storeImage } = require('./utils');

// Initialize the Algolia client.
const client = algoliasearch(process.env.ALGOLIA_APP_ID, process.env.ALGOLIA_ADMIN_API_KEY);
const AlgoliaIndex = client.initIndex(process.env.ALGOLIA_INDEX_NAME);

// ------------------------------------------------------------------------------
const csvFilePath = './data-05-15-24.csv'; 

function getLinkedInId (data) {
  try {
    return data.Linkedin.split('/in/')[1].replace('/', '').trim();
  } catch(err) {
    console.log('Skipping', err.message, data.Linkedin || 'No URL');
    return null;
  }
}

// create a queue object with concurrency 2
var ghQueue = async.queue(function(task, callback) {
  storeImage(task.url, 'network-book/logos', task.name).then(callback);
}, 1);

// assign a callback
ghQueue.drain(function() {
  console.log("\nAll Records have been processed.");
});

// const converter = csv({
//   noheader: false,
//   trim: true
// });

// // Function to replace spaces in header keys
// converter.on("header", (header) => {
//   console.log(header, header.map(field => field.replace(/\s+/g, '')))
//   return header.map(field => field.replace(/\s+/g, ''));
// });

csv()
  .fromFile(csvFilePath)
  .then(async (records) => {

    const dataToIndex = [];
    _.forEach(records, record => {
      const id = getLinkedInId(record);
      if (id) {
        record.objectID = getLinkedInId(record);
        record.Tags = record.Tags.split(',');
        record.DateConnectedUnix = moment(record.DateConnected).unix();
        record.DateConnectedStr = moment(record.DateConnected).format('MMM DD, YYYY');
        record.calendly_LastMeetingAtUnix = 0;
        
        // record.Locations = record.Location.split(', ').join(' > ');
        var regions = record.Location.split(', ');
        record.Locations = {
          lvl1: regions[0],
          lvl2: [ regions[0], regions[1]].join(' > '),
          lvl3: [ regions[0], regions[1], regions[2] ].join(' > '),
        }
        
        dataToIndex.push(record);
        // ghQueue.push({ url: record.ProfilePicture, name: `${record.objectID}.jpeg`});
      } else {
        console.log('missing', record.Linkedin)
      }
    });

    // Force the index to run.
    await AlgoliaIndex.saveObjects(dataToIndex).then(({ objectIDs }) => {
      console.log('Data pushed to Algolia:', objectIDs.length);
    }).catch(err => {
      console.error('Error pushing data to Algolia:', err);
    });

    // // Dump it to a file.
    // const outputFilePath = './output_file.json'; 
    // fs.writeFile(outputFilePath, JSON.stringify(records, null, 2), (err) => {
    //   if (err) {
    //     console.error('Error writing JSON to file:', err);
    //   } else {
    //     console.log('JSON file has been saved.');
    //   }
    // });

  })
  .catch((error) => {
    console.error('Error processing CSV file:', error);
  });


