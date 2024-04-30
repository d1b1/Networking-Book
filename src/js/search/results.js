var instantsearch = require('instantsearch.js');
var createWidgets = require('../util/create_widgets.js');

module.exports = function (opts) {

  var search = instantsearch({
    appId: opts.appId,
    apiKey: opts.apiKey,
    indexName: opts.indexName,
    urlSync: {
      useHash: true
    }
  });

  var widgets = createWidgets(opts.indexName);

  widgets.forEach(function (widget) { search.addWidget(widget); });
  search.start();
};
