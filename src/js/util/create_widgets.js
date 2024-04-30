var instantsearch = require('instantsearch.js');
var infiniteScrollWidget = require('../search/widgets/infinite_scroll.js');

module.exports = function (indexName) {
  console.log('asdf', instantsearch.widgets)
  return [
    instantsearch.widgets.searchBox({
      container: '#search-input'
    }),
    instantsearch.widgets.refinementList({
      container: '#tags',
      attributeName: 'Tags',
      limit: 10,
      operator: 'or'
    }),
    infiniteScrollWidget({
      container: '#hits',
      templates: {
        items: document.querySelector('#hits-template').innerHTML,
        empty: document.querySelector('#no-results-template').innerHTML
      },
      offset: 850
    }),
  ];
};
