/* PACKAGES */
var util = require('util'),
    fs = require('fs'),
    MongoClient = require('mongodb').MongoClient,
    JSONStream = require('JSONStream'),
    test = require('assert');

/* GLOBALS - CONSTANTS */
var C = {};
C.fileName = '\\data\\gCycle-data\\Takeout\\Location History\\test.json';
C.mongoURL = 'mongodb://localhost:27017/gCycle';
C.minIntervalForNew = 3600000; // One hour
C.minConfidence = 1;


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
    console.log('Reading file...');
    fs.createReadStream(fileName)
        .pipe(JSONStream.parse('locations'))
        .on('data', function (data) {
            console.log('Adding ' + data.length + ' records...');
            rawCol.insertMany(data, function (err, r) {
                test.equal(null, err);
                test.equal(data.length, r.insertedCount);
            });
        })
        .once('end', function () {
            console.log('Done.');
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
    console.log('Filtering data...');

    // Access the filtered collection
    var filteredCol = db.collection('filteredData');

    // Delete all records from the filtered collection
    filteredCol.deleteMany({}, function (err) {
        test.equal(err, null);

        // Create an aggregation pipeline to isolate only the useful fields,
        // including only records with onBicycle, above the given minimum confidence
        var filtered = rawCol.aggregate([
            {
                $project: {
                    _id: 0,
                    'activitys.timestampMs': 1,
                    'activitys.activities.type': 1,
                    'activitys.activities.confidence': 1
                }
            },
            {
                $unwind: '$activitys'
            },
            {
                $unwind: '$activitys.activities'
            },
            {
                $match: {
                    'activitys.activities.type': 'onBicycle'
                }
            },
            {
                '$match': {
                    'activitys.activities.confidence': {
                        $gte: C.minConfidence
                    }
                }
            },
            {
                $project: {
                    'timestampMs': '$activitys.timestampMs',
                    'confidence': '$activitys.activities.confidence'
                }
            }
        ]);

        // Insert into the filtered collection any records with onBicycle
        // TODO: Chunking
        filtered.toArray(function (err, filteredData) {
            test.equal(null, err);
            // test(filteredData.length > 0);
/*            console.log(util.inspect(filteredData, {
                showHidden: false,
                depth: null,
                colors: true
            }));
*/
            filteredCol.insertMany(filteredData, function (err, r) {
                test.equal(null, err);
                test.equal(filteredData.length, r.insertedCount);

                console.log(r.insertedCount + ' records inserted.');
                appendDateTime(db, filteredCol);
            });
        });

    });
}

/**
 *
 * getDate(timestampMs)
 *
 * Helper function to get the date from a timestampMS (ms since the epoch as string)
 *
 * @param string timestampMs: The timestampMs to process
 */
function getDate(timestampMs) {
    var t = parseInt(timestampMs, 10);
    var d = new Date(t);
    return d.toDateString();
}

/**
 *
 * getDate(timestampMs)
 *
 * Helper function to get the date from a timestampMS (ms since the epoch as string)
 *
 * @param string timestampMs: The timestampMs to process
 */
function getTime(timestampMs) {
    var t = parseInt(timestampMs, 10);
    var d = new Date(t);
    return d.toTimeString();
}


/**
 * appendDateTime(db, filteredCol)
 *
 * Append the date and time to each record in filteredCol
 *
 * @param MongoDB.Db db: The database in which to store the results
 * @param MongoDB.Collection filteredCol - The collection we are operating on
 */
function appendDateTime(db, filteredCol) {
    filteredCol.count(function (err, count) {
        var updated = 0;
        filteredCol.find().snapshot().forEach(function (doc) {
            filteredCol.updateOne({
                _id: doc._id
            }, {
                $set: {
                    'Date': getDate(doc.timestampMs),
                    'Time': getTime(doc.timestampMs)
                }
            }, function (err, r) {
                test.equal(null, err);

                updated++;
                if(updated == count) {
                    process.exit(); // Next jump off point
                }
            });

        });

    });

}


/**
 * groupByDate(db, filteredCol)
 *
 * Create a collection grouped by date
 *
 * @param MongoDB.Db db: The database in which to store the results
 * @param MongoDB.Collection filteredCol - The collection we are operating on
 */
function groupByDate(db, filteredCol) {
    // Using JS approach
    filteredCol.find().toArray(function (err, docs) {
        var lastTime = 0;
        test.equal(null, err);
        for (var i = 0; i < docs.length; i++) {
            console.log(docs[i]);
            var act1 = docs[i].activitys; // Not a typo!!

        }
    });
}

/* Main entry point */

MongoClient.connect(C.mongoURL, function (err, db) {
    test.equal(null, err);

    // Access the desired collection
    var rawCol = db.collection('rawData');

    /* TODO: SKIPPING FOR NOW!
    // Delete all records, then if all is well add the new records
    console.log("Deleting records...");
    rawCol.deleteMany({}, function (err) {
        test.equal(null, err);

        readFile(C.fileName, db, rawCol);

    });
    //*/
    // TODO: Remove this section when reading from the file
    filterData(db, rawCol);
    // var filteredCol = db.collection('filteredData');

    // appendDateTime(db, filteredCol);
    //*/

});


// 3. Organize by date (and start time)
// 4. Output report
