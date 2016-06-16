/* PACKAGES */
var fs = require('fs'),
    mongodb = require('mongodb'),
    JSONStream = require('JSONStream');

/* GLOBALS - CONSTANTS */
var C = {}
C.fileName = '\\data\\gCycle-data\\Takeout\\Location History\\test.json';


/**
 * initDB() - Initialize the database
 *
 * @return mongodb - The database pointer
 */
function initDB() {
    return null;
}

/**
 * readFile(fileName) - Read the file into memory
 *
 * @param string fileName: The file to read
 * @param mongodb db: The database in which to store the results
 * @return object: A reduced data set
 */
function readFile(fileName, db) {
    var result = {};

    fs.createReadStream(fileName)
        .pipe(JSONStream.parse('locations'))
        .on('data', function (data) {
            console.dir(data);
        });

    return result;
}


// 1. Read in the file and store into DB
var db = initDB();
res = readFile(C.fileName, db);

// 2. Do necessary filtering
// 3. Organize by date (and start time)
// 4. Output report
