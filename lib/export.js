// Copyright (c) 2013 Azuqua, Inc.

// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

"use strict"
var azure = require("azure");
var _ = require("underscore");
var fs = require("fs");
var async = require("async");
var path = require("path");
var util = require("util");

module.exports = function(service, config, storageTypes) {
  console.log("Starting export of %j", storageTypes);
  var root = path.resolve(config.options.root);

  /**
   * Create the directory if 
   * it does not exist (Sync)
   */
  if(!fs.existsSync(root)) {
    fs.mkdirSync(root);
    for (var i = 0; i < storageTypes.length; i++) {
      if(!fs.existsSync(root + "/" + storageTypes[i])) {
        fs.mkdirSync(root + "/" + storageTypes[i]);
      }
    }
  }

  /**
   * List of containers to be exported
   * @param  {Function} callback 
   * @return {array}    list of containers
   */
  function listContainers(callback) {
    if (config.export.containers.length == 0) {
      service.blobs.listContainers(function(error, list) {
        if (!error) {
          var list = _.pluck(list, "name");
          list = _.difference(list, config.ignore.containers);
          callback(null, list);
        } else{
          console.log("Error in getting list of Containers");
          console.log(error);
          callback(error);
        }
      });
    } else {
      var list = config.export.containers;
      list = _.difference(list, config.ignore.containers);
      callback(null, list);
    }
  }

  /**
   * List of tables to be exported
   * @param  {Function} callback 
   * @return {array}    list of tables
   */
  function listTables(callback) {
    if (config.export.tables.length == 0) {
      service.tables.queryTables(function(error, list) {
        if (!error) {
          var list = _.pluck(list, "TableName");
          list = _.difference(list, config.ignore.tables);
          callback(null, list);
        } else{
          console.log("Error in getting list of Tables");
          console.log(error);
          callback(error);
        }
      });
    } else {
      var list = config.export.tables;
      list = _.difference(list, config.ignore.tables);
      callback(null, list);
    }
  }

  /**
   * Write each table to file
   */
  function getTable(table, callback) {
    console.log("Starting export of " + table + " @ " + new Date());
    var tq = azure.TableQuery;
    var query = tq.select().from(table);
    service.tables.queryEntities(query, function (err, entities) {
      var temp = {};
      var list = [];
      if (!err) {
        while(temp = entities.pop()) {
          list.push(_.omit(temp, config.ignore.columns));
        }
        fs.writeFileSync(root + "/" + "tables/" + table, JSON.stringify(list));
        console.log("Completed export of " + table + " @ " + new Date());
        callback(null);
      } else {
        callback(err);
      }
    });
  }

  /**
   * Write each blob to file
   */
  function getContainer(container, callback) {
    console.log("Starting %s @ %s", container, new Date().getTime());
    bs.listBlobs(container, function(err, entities) {
      if (!err) {
        entities = _.pluck(entities, "name");
        async.eachSeries(entities, function(entity, callback) {
          var dir = directory + "/" + "blobs/" + container;
          var file = dir + "/" + entity.split("/").join("_");
          if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir);
          }
          bs.getBlobToFile(container, entity, file, function(err){
            if (!err) {callback(null);} else{callback(err);}
          });
        }, function(error) {
          if (!error) {console.log("Yay"); callback(null);} else{console.log("Boo");callback(err);};
        });
      } else{
        callback(err);
      }
    });
  }


  /**
   * Export tables function
   */
  function exportTables(callback) {
    listTables(function(err, list) {
      if(!err) {
        async.eachSeries(list, getTable, function (error) {
          if(!error){
            callback(null, "All Tables Exported");
          } else {
            callback("Somewhere - an error occured");
          }
        });
      } else {
        callback(err);
      }
    });
  }

  function exportContainers(callback) {
    listContainers(function(err, list) {
      if(!err) {
        callback(null, list);
      } else {
        callback(err);
      }
    });
  }

  /**
   * Execution Block
   */
  async.parallel([
    function(callback){
      if(_.contains(storageTypes, "tables")) {
        exportTables(callback);
      }
    },
    function(callback){
      if(_.contains(storageTypes, "blobs")) {
        exportContainers(callback);
      }
    }
  ],
  function(err, results){
    if(!err) {
      console.log(results);
    }
  });
}