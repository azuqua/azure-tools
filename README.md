azure-tools
===========

Tools to work with data in Azure storage services (tables, blobs)

Setup:

Clone the git repository

run npm install

Usage:

To import tables -- 
node index.js import --config myconfig --envrionment development tables

To export blobs --
node index.js export --config myconfig --envrionment development blobs

To import everything --
node index.js import --config myconfig --envrionment development all


Notes:
Run an export to see how the directory structure should be for importing.
Under the root folder you must have sub-folders called tables and blobs

To load selective tables update config/myconfig and add the tables, blobs you want.

Add tables, containers into ingore to ignore them.
