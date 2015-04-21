var request = require('request'),
  fs = require('fs'),
  util = require('util'),
  _ = require('underscore');

var BASE_URL = 'https://phlrollout-phl.cs17.force.com/services/apexrest/inventory/v1/';

var datajson = {
  '@type': 'dcat:Catalog',
  conformsTo: 'https://project-open-data.cio.gov/v1.1/schema',
  dataset: []
};

// Map raw data to data.json spec
var mapFeed = function(rawData) {
  return rawData.map(function(item, index, list) {
    var dataset = {
      '@type': 'dcat:Dataset',
      title: item.Name,
      description: item.Contents__c,
      identifier: item.Id
    };

    if(item.endpoints !== undefined && item.endpoints.length) {
      dataset.distribution = [];
      for(var i = 0; i < item.endpoints.length; i++) {
        var distribution = {
          '@type': 'dcat:Distribution',
          title: item.endpoints[i].Transformation__r.Name,
          downloadURL: item.endpoints[i].Endpoint_URL__c, // should be accessURL for APIs/applications
          mediaType: item.endpoints[i].Format__c,
          description: item.endpoints[i].Transformation__r.Contents__c
        };
        dataset.distribution.push(distribution);
      }
    }

    return dataset;
  });
}

// Fetch datasets and representations
request({
  url: BASE_URL + 'datasets.json',
  json: true
}, function(err, response, datasets) {
  // Fetch endpoints
  request({
    url: BASE_URL + 'loads.json',
    json: true
  }, function(err, response, endpoints) {
    // Group endpoints by their dataset id
    var groupedEndpoints = _.groupBy(endpoints, function(endpoint) {
      return endpoint.Transformation__r.Dataset__c;
    });

    // Put endpoints into their dataset
    datasets.forEach(function(dataset) {
      dataset.endpoints = []; // Property should be set even if empty
      if(groupedEndpoints[dataset.Id] !== undefined) {
        dataset.endpoints = groupedEndpoints[dataset.Id];
      }
    });

    datajson.dataset = mapFeed(datasets);
    //console.log(util.inspect(mappedFeed, false, null));
    fs.writeFile('data.json', JSON.stringify(datajson, null, 4), function(err) {
  		if(err) {
  			console.log(err);
  		} else {
  			console.log('File written to data.json');
  		}
  	})
  });
});
