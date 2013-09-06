#!/usr/bin/env node
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

var program = require("commander");
var _ = require("underscore");
var yaml = require("js-yaml");
var azure = require("azure");
var package = require('./package.json');

var config = {};
var env = {};

/**
 * Validate that everything exists
 */
function validateOptions() {
  try {
    config = require("./config/" + program.config + ".json");
  } catch(ex) {
    console.log(ex);
    process.exit();
  }

  var environments = require('./environments.yaml');
  env = environments[program.environment];

  if(!env) {
    console.log("Environment is not configured in environments.yaml");
    process.exit();
  }
}

/**
 * Create Azure Connection
 * @param  {string}     [the type of connection to azure tables/blobs/all]
 * @return {object}     [The service connection object]
 */
function createService(params) {
  var _service = {};
  if(params == "tables") {
    _service.tables = azure.createTableService(env.keys.account, env.keys.secret);
  }
  if(params == "blobs") {
    _service.blobs = azure.createBlobService(env.keys.account, env.keys.secret);
  }
  if(params == "all") {
    _service.tables = azure.createTableService(env.keys.account, env.keys.secret);
    _service.blobs = azure.createBlobService(env.keys.account, env.keys.secret);
  }
  return _service;
}

function runProcess (action, params) {
  var storageTypes = [];
  if (params == "tables") {
    storageTypes.push("tables");
  } else if (params == "blobs") {
    storageTypes.push("blobs");
  } else {
    storageTypes.push("tables");
    storageTypes.push("blobs");
  }
  console.log("Entering %s of %j", action, storageTypes);
  var service = createService(params);
  require("./lib/" + action + ".js")(service, config, storageTypes);
}

program
  .version(package.version)
  .option("-e, --environment [environment]", "Environment [development]", "development")
  .option("-c, --config [config]", "Name of config file [myconfig]", "myconfig");

program
  .command('export [tables/blobs/all]')
  .description('Export from Azure Storage')
  .action(function(options){
    if (!options) {
      console.log("There is nothing to do");
      program.help();
    } else if (!_.contains(["tables", "blobs", "all"], options)) {
      console.log("Invalid object: %s", options);
      program.help();
    } else {
      validateOptions();
      runProcess("export", options);
    }
  });

program
  .command('import [tables/blobs/all]')
  .description('Import into Azure Storage')
  .action(function(options){
    if (!options) {
      console.log("There is nothing to do");
      program.help();
    } else if (!_.contains(["tables", "blobs", "all"], options)) {
      console.log("Invalid object: %s", options);
      program.help();
    } else {
      validateOptions();
      runProcess("import", options);
    }
  });

program.parse(process.argv);
