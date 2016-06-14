var fs = require('fs'),
    mongodb = require('mongodb'),
    JSONStream = require('JSONStream');

/**
* readFile(fileName) - Read the file into memory
*
* @param string fileName: The file to read
* @return object: A reduced data set
*/
function readFile(fileName) {
    var result = {};
    var readableStream = fs.createReadStream('')


    return result;
}


// 1. Read in the file

// 2. Store data in DB
// 3. Do necessary filtering
// 4. Organize by date (and start time)
// 5. Output report

