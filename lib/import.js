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
  console.log("Starting import of %j", storageTypes);
  var root = path.resolve(config.options.root);

  /**
   * Check the directory exists
   */
  if(!fs.existsSync(root)) {
    console.log("Root folder in config does not exist");
    process.exit();
  } else {
    for (var i = 0; i < storageTypes.length; i++) {
      if(!fs.existsSync(root + "/" + storageTypes[i])) {
        console.log("Nothing to Import for %s", storageTypes[i]);
        process.exit();
      }
    }
  }

  /**
   * List of tables to be imported
   * Look in the tables folder and get a list
   * if not specified in config file
   */
  function listTables(callback) {
    if (config.import.tables.length == 0) {
      var list = fs.readdirSync(config.options.root + "/tables");
      list = _.difference(list, config.ignore.directories);
      list = _.difference(list, config.ignore.tables);
      callback(null, list);
    } else {
      var list = config.import.tables;
      list = _.difference(list, config.ignore.tables);
      console.log(list);
      callback(null, list);
    }
  }

  /**
   * List of containers to be imported
   * Look in the containers folder and get a list
   * if not specified in config file
   */
  function listContainers(callback) {
    if (config.import.containers.length == 0) {
      var list = fs.readdirSync(config.options.root + "/blobs");
      list = _.difference(list, config.ignore.directories);
      callback(null, list);
    } else {
      var list = config.import.containers;
      callback(null, list);
    }
  }

  function putTable(table, callback) {
    console.log("Starting import of " + table + " @ " + new Date());
    var data = fs.readFileSync(path.join(root + "/tables", table), "utf8");
    var list = [];
    var rows = [];
    try {
      list = JSON.parse(data);
    } catch (ex) {
      list = data;
    }
    createTable(table, function(err) {
      if(!err) {
        var temp = {};
        while(temp = list.pop()) {
          var record = _.omit(temp, config.ignore.columns);
          for (var prop in record) {
            if (config.isJSON[table] && config.isJSON[table][prop]) {
              record[prop] = JSON.stringify(record[prop]);
            }
          }
          rows.push(record);
        }
        async.eachLimit(rows, config.options.workers, function createRow(row, callback) {
          if(config.import.overwriteRecords) {
            service.tables.insertOrReplaceEntity(table, row, function(error, data) {
              if (!error) {
                callback()
              } else {
                console.log(error);
                callback("Error while inserting row into table " + table + ": " + error);
              }
            }); 
          } else {
            service.tables.insertEntity(table, row, function(error, data) {
              if (!error) {
                callback()
              } else if (error && error.code === 'EntityAlreadyExists') {
                console.log('Warning: Row %s:%s already exists.',row.PartitionKey, row.RowKey);
                callback();
              } else {
                console.log(error);
                callback("Error while inserting row into table " + table + ": " + error);
              }
            }); 
          }
        }, function(error) {
          if (!error) {
            console.log("Completed import of " + table + " @ " + new Date());
            callback();
          } else {
            console.log(error);
            callback(error);
          }
        });
      } else {
        callback(err);
      }
    });
  }

  function putContainer(container, callback) {
    console.log("Starting import of " + container + " @ " + new Date());
    createContainer(container, function(err) {
      if(!err) { 
        var containerPath = path.join(root, "blobs", container);
        fs.readdir(containerPath, function (error, fileList) {
          if (!error) {
            fileList = _.difference(fileList, config.ignore.directories).sort();
            console.log("Number of Files to be imported into %s: %s.", container, fileList.length);

            var filePaths = _.map(fileList, function(filePath) {
               return { filePath: path.join(containerPath, filePath), container: container }
             });

            async.eachSeries(filePaths, loadBlobWithRetry, function(error) {
              if (!error) {
                console.log("Completed import of " + container + " @ " + new Date());
                callback();
              } else {
                callback(error);
              }
            });
          } else {
            callback(error);
          }
        });
      } else {
        callback(err);
      }
    });
  }

  function loadBlobWithRetry(file, callback) {
    loadBlob(file, function(error) {
      if (!error) {
        callback();
      } else {
        // Retry 1 time after a delay
        console.log("Retrying file %s.", file.filePath);
        setTimeout(function() {
          loadBlob(file, callback);
        }, 1000 * 5);
      } 
    });
  }

  function loadBlob(file, callback) {
    var fileName = path.basename(file.filePath).split('_').join('/');
    console.log("Importing file %s into container %s.", fileName, file.container);
    var options = {
      metadata: { fileName: fileName }
    };
    service.blobs.createBlockBlobFromFile(
      file.container,
      fileName,
      file.filePath,
      options,
      function(error) {
        if (!error) {
          console.log("Imported %s", file.filePath);
          callback();
        } else {
          console.log("Error importing %s into %s", file.filePath, file.container);
          callback(error);
        }
    });
  }

  function createTable(table, callback) {
    service.tables.createTableIfNotExists(table, function(error) {
      if (!error) {
        callback();
      } else {
        callback("Error while creating table " + table + ": " + error);
      }
    });
  }

  function createContainer(container, callback) {
    service.blobs.createContainerIfNotExists(container, {"publicAccessLevel": null}, function(error) {
      if (!error) {
        console.log("Container created:", container);
        callback();
      } else {
        callback("Error while creating container " + container + ": " + error);
      }
    });
  }

  function importTables(callback) {
    listTables(function(err, list) {
      if(!err) {
        async.eachLimit(list, config.options.workers, putTable, function (error) {
          if(!error){
            callback(null, "All Tables Imported");
          } else {
            console.log(error);
            callback("Error: In Table Import");
          }
        });
      } else {
        console.log(err);
        callback(err);
      }
    });
  }

  function importContainers(callback) {
    console.log("In Import Containers");
    listContainers(function(err, list) {
      if(!err) {
        async.eachLimit(list, config.options.workers, putContainer, function (error) {
          if(!error){
            callback(null, "All Containers Imported");
          } else {
            console.log(error);
            callback("Error: In Container Import");
          }
        });
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
        importTables(callback);
      }
    },
    function(callback){
      if(_.contains(storageTypes, "blobs")) {
        importContainers(callback);
      }
    }
    ],
    function(err, results){
      if(!err) {
        console.log(results);
      } else {
        console.log(err);
      }
  });
}