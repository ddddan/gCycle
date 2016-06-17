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
 * @param MongoDB.Db db: The database in which to store the results
 * @param MongoDB.Collection rawCol: The collection in which to store the results
 *
 * @return object: A reduced data set
 */
function readFile(fileName, db, rawCol) {
    console.log("Reading file...");
    fs.createReadStream(fileName)
        .pipe(JSONStream.parse('locations'))
        .on('data', function (data) {
            console.log("Adding " + data.length + " records...");
            rawCol.insertMany(data, function (err, r) {
                test.equal(null, err);
                test.equal(data.length, r.insertedCount);
            });
        })
        .once('end', function () {
            console.log("Done.");
            filterData(db, rawCol);
        });
}

/**
 * filterData(col)
 *
 *
 * Filter results - We only want records with onBicycle 'activitys'
 *
 * @param MongoDB.Db db: The database in which to store the results
 * @param MongoDB.Collection rawCol - The collection we are operating on
 *
 */
function filterData(db, rawCol) {
    console.log("Filtering data...");

    // Access the filtered collection
    var filteredCol = db.collection('filteredData');

    // Delete all records from the filtered collection
    filteredCol.deleteMany({}, function (err) {
        test.equal(err, null);

        // Insert into the filtered collection any records with onBicycle
        var filtered = rawCol.aggregate([
            {
                $match: {
                    activitys: {
                        $elemMatch: {
                            activities: {
                                $elemMatch: {
                                    type: 'onBicycle'
                                }
                            }
                        }
                    }
                }
            }
        ]);

        filtered.toArray(function(err, filteredData){
            test.equal(null, err);
            // test(filteredData.length > 0);
            // console.dir(filteredData);

            filteredCol.insertMany(filteredData, function (err, r){
                test.equal(null, err);
                test.equal(filteredData.length, r.insertedCount);
                console.log(r.insertedCount + ' records inserted.');
                process.exit(); /////////////////////////////////
            });
        });

    });
}


/* Main entry point */

MongoClient.connect(C.mongoURL, function (err, db) {
    test.equal(null, err);

    // Access the desired collection
    var rawCol = db.collection('rawData');

    /* TODO: SKIPPING FOR NOW! */
    // Delete all records, then if all is well add the new records
    console.log("Deleting records...");
    rawCol.deleteMany({}, function (err) {
        test.equal(null, err);

        readFile(C.fileName, db, rawCol);

    });
    // filterData(db, rawCol); // Remove this line when reading from the file


});


// 3. Organize by date (and start time)
// 4. Output report
