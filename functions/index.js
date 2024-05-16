const functions = require("firebase-functions");
const express = require("express");
const algoliasearch = require("algoliasearch");

// Initialize Express app
const app = express();

const appid = functions.config().algolia.appid;
const key = functions.config().algolia.searchkey;
const algoliaClient = algoliasearch(appid, key);

// Define a route
app.get("/", (req, res) => {
  res.send("Hello, World!");
});

app.post("/sffv", async ({body}, res) => {
  console.log("Call to the facet search");

  const {requests} = body;
  const results = await algoliaClient.searchForFacetValues(requests);
  res.status(200).send(results);
});

// Define another route if needed
app.post("/search", async ({body}, res) => {
  console.log("Call to the POST search");

  const {requests} = body;
  const results = await algoliaClient.search(requests);
  res.status(200).send(results);
});

// Export the Express app as a Firebase Function
exports.api = functions.region("us-central1").runWith({
  memory: "512MB",
  cpu: 1}).https.onRequest(app);
