/* PACKAGES */
var fs = require('fs'),
    MongoClient = require('mongodb').MongoClient,
    JSONStream = require('JSONStream'),
    test = require('assert');

/* GLOBALS - CONSTANTS */
var C = {}
C.fileName = '\\data\\gCycle-data\\Takeout\\Location History\\test.json';
C.mongoURL = 'mongodb://localhost:27017/gCycle';


/**
 * readFile(fileName) - Read the file into memory
 *
 * @param string fileName: The file to read
 * @param MongoDB.Collection col: The database in which to store the results
 * @return object: A reduced data set
 */
function readFile(fileName, col) {
    var result = {};

    fs.createReadStream(fileName)
        .pipe(JSONStream.parse('locations'))
        .on('data', function (data) {
            col.insertMany(data, function(err, r){
                test.equal(null, err);
                test.equal(data.length, r.insertedCount);
            });
        });

    return result;
}


// 1. Read in the file and store into DB
MongoClient.connect(C.mongoURL, function (err, db) {
    test.equal(null, err);

    // Add a collection to hold the raw data
    db.createCollection('rawData', function(err, col){
        test.equal(null, err);
        // Read the data
        readFile(C.fileName, col);
    });

});

// 2. Do necessary filtering
// 3. Organize by date (and start time)
// 4. Output report
