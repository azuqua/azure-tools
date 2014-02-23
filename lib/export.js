// Copyright (c) 2014 Azuqua, Inc.

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
  if (!fs.existsSync(root)) {
    fs.mkdirSync(root);
  }

  for (var i = 0; i < storageTypes.length; i++) {
    if (!fs.existsSync(root + "/" + storageTypes[i])) {
      fs.mkdirSync(root + "/" + storageTypes[i]);
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
          console.log("Exporting: %s", list);
          callback(null, list);
        } else {
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
          console.log("Exporting: %s", list);
          callback(null, list);
        } else {
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
   * List of queues to be exported
   * @param  {Function} callback
   * @return {array}    list of queues
   */

  function listQueues(callback) {
    if (config.export.queues.length == 0) {
      service.queues.listQueues(function(error, list) {
        if (!error) {
          var list = _.pluck(list, "name");
          list = _.difference(list, config.ignore.tables);
          console.log("Exporting: %s", list);
          callback(null, list);
        } else {
          console.log("Error in getting list of Tables");
          console.log(error);
          callback(error);
        }
      });
    } else {
      var list = config.export.queues;
      list = _.difference(list, config.ignore.queues);
      callback(null, list);
    }
  }

  /**
   * Get each message from queue
   */

  function getQueue(queue, callback) {
    console.log("Starting export of " + queue + " @ " + new Date());
    var list = [];
    var done = false;
    async.until(
      function() {
        return done;
      },
      function(check) {
        service.queues.getMessages(queue, function(err, qData) {
          if (!err) {
            if (!qData[0]) {
              done = true;
              check(null);
            } else {
              var message = qData[0];
              try {
                message.messagetext = JSON.parse(message.messagetext);
              } catch(e){
                //do nothing
              }
              list.push(message);
              // service.queues.deleteMessage(queue, message.messageid, message.popreceipt, function(error) {
              //   callback(error, qData);
              // });
              check(null);
            }
          } else {
            console.log(err);
            check(err);
          }
        })
      },
      function(error) {
        if (!error) {
          fs.writeFileSync(root + "/" + "queues/" + queue, JSON.stringify(list));
          console.log("Completed export of " + queue + " @ " + new Date());
          callback(null);
        } else {
          callback(err);
        }
      }
    );
  }

  /**
   * Write each table to file
   */

  function getTable(table, callback) {
    console.log("Starting export of " + table + " @ " + new Date());
    var tq = azure.TableQuery;
    var filter = config.export.PartitionKey; //this only works for one table
    var query = tq.select().from(table).where("PartitionKey eq " + "'" + filter + "'");
    service.tables.queryEntities(query, function(err, entities) {
      var temp = {};
      var list = [];
      if (!err) {
        while (temp = entities.pop()) {
          var record = _.omit(temp, config.ignore.columns);
          console.log("Exporting: %s %s", record.PartitionKey, record.RowKey)
          for (var prop in record) {
            if (config.isJSON[table] && config.isJSON[table][prop]) {
              record[prop] = JSON.parse(record[prop]);
            }
          }
          list.push(record);
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
    console.log("Starting export of " + container + " @ " + new Date());
    service.blobs.listBlobs(container, function(err, entities) {
      if (!err) {
        entities = _.pluck(entities, "name");
        async.eachLimit(entities, config.options.workers, function(entity, callback) {
          var dir = root + "/" + "blobs/" + container;
          var file = dir + "/" + entity.split("/").join("_");
          if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir);
          }
          service.blobs.getBlobToFile(container, entity, file, function(err) {
            if (!err) {
              callback(null);
            } else {
              callback(err);
            }
          });
        }, function(error) {
          if (!error) {
            console.log("Completed export of " + container + " @ " + new Date());
            callback(null);
          } else {
            callback(err);
          }
        });
      } else {
        callback(err);
      }
    });
  }


  /**
   * Export tables function
   */

  function exportTables(callback) {
    listTables(function(err, list) {
      if (!err) {
        async.eachSeries(list, getTable, function(error) {
          if (!error) {
            callback(null, "All Tables Exported");
          } else {
            callback("Error: In Table Export");
          }
        });
      } else {
        callback(err);
      }
    });
  }

  function exportContainers(callback) {
    listContainers(function(err, list) {
      if (!err) {
        async.eachSeries(list, getContainer, function(error) {
          if (!error) {
            callback(null, "All Containers Exported");
          } else {
            callback("Error: In Container Export");
          }
        });
      } else {
        callback(err);
      }
    });
  }

  function exportQueues(callback) {
    listQueues(function(err, list) {
      if (!err) {
        async.eachSeries(list, getQueue, function(error) {
          if (!error) {
            callback(null, "All Queues Exported");
          } else {
            callback("Error: In Queue Export");
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
      function(callback) {
        if (_.contains(storageTypes, "tables")) {
          exportTables(callback);
        }
      },
      function(callback) {
        if (_.contains(storageTypes, "blobs")) {
          exportContainers(callback);
        }
      },
      function(callback) {
        if (_.contains(storageTypes, "queues")) {
          exportQueues(callback);
        }
      }
    ],
    function(err, results) {
      if (!err) {
        console.log(results);
      }
    });
}
