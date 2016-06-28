console.time('overall');
/* PACKAGES */
var util = require('util'),
    fs = require('fs'),
    MongoClient = require('mongodb').MongoClient,
    JSONStream = require('JSONStream'),
    test = require('assert');

/* GLOBALS - CONSTANTS */
var C = {};
C.fileName = '..\\..\\gCycleData\\Takeout\\Location History\\test.json';
C.mongoURL = 'mongodb://localhost:27017/gCycle';
C.minIntervalForNew = 3600000; // One hour
C.minute = 60000; // One minute
C.minConfidence = 33;


/**
 * readFile(fileName) - Read the file into memory
 *
 * @param {string} fileName: The file to read
 * @param {MongoDB.Db} db: The database in which to store the results
 * @param {MongoDB.Collection} rawCol: The collection in which to store the results
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
                console.log(r.insertedCount + ' records added.');
                filterData(db, rawCol);
            });
        })
        .once('end', function () {
            console.log('[Reading file] Done.');

        });
}

/**
 * filterData(col)
 *
 *
 * Filter results - We only want records with onBicycle 'activitys'
 *
 * @param {MongoDB.Db} db: The database in which to store the results
 * @param {MongoDB.Collection} rawCol - The collection we are operating on
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
        // TODO: Handle empty set
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
            //*/
        ]);

        // Insert into the filtered collection any records with onBicycle
        // TODO: Chunking
        filtered.toArray(function (err, filteredData) {
            test.equal(null, err);
            test(filteredData.length > 0);
            /*
            console.log(util.inspect(filteredData, {
                showHidden: false,
                depth: null,
                colors: true
            }));
*/

            filteredCol.insertMany(filteredData, function (err, r) {
                test.equal(null, err);
                test.equal(filteredData.length, r.insertedCount);

                console.log(r.insertedCount + ' records inserted.');
                console.log('Indexing...');

                // Create an index on timestampMs - for sorting later
                filteredCol.createIndex('timestampMs', {
                    w: 1
                }, function (err, indexName) {
                    test.equal(null, err);
                    test.equal('timestampMs_1', indexName);

                    appendDateTime(db, filteredCol);

                });


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
 * @param {int} timestampMs: The timestampMs to process
 */
function getDate(timestampMs) {
    var d = new Date(timestampMs);
    return d.toDateString();
}

/**
 *
 * getDate(timestampMs)
 *
 * Helper function to get the date from a timestampMS (ms since the epoch as string)
 *
 * @param {int} timestampMs: The timestampMs to process
 */
function getTime(timestampMs) {
    var d = new Date(timestampMs);
    return d.toTimeString();
}


/**
 * appendDateTime(db, filteredCol)
 *
 * Append the date and time to each record in filteredCol
 *
 * @param {MongoDB.Db} db: The database in which to store the results
 * @param {MongoDB.Collection} filteredCol - The collection we are operating on
 */
function appendDateTime(db, filteredCol) {
    console.log('Adding date and time...');
    filteredCol.count(function (err, count) {
        var updated = 0;
        filteredCol.find().snapshot().forEach(function (doc) {
            // Convert to numeric
            var timestampMs = parseInt(doc.timestampMs, 10);
            filteredCol.updateOne({
                _id: doc._id
            }, {
                $set: {
                    'timestampMs': timestampMs,
                    'Date': getDate(timestampMs),
                    'Time': getTime(timestampMs)
                }
            }, function (err, r) {
                test.equal(null, err);

                updated++;
                if (updated === count) {
                    groupByTrip(db, filteredCol);
                }
            });

        });

    });

}


/**
 * completeRide(ride, lastTime)
 *
 * Helper function to complete a ride definition to be pushed to the results array
 *
 * @param {object} completeRide: The ride object to be completed
 * @param {object} lastTime: the previous timestamp (now the end time of the ride)
 * @returns {object} The modified ride object
 */
function completeRide(ride, lastTime) {
    var d = new Date(lastTime);

    // Set end time of ride to the last recorded time
    ride.endTimeMs = lastTime;
    ride.endTime = d.toTimeString();

    // Calculate duration in ms and minutes
    ride.duration = lastTime - ride.startTimeMs;
    ride.minutes = ride.duration / C.minute;
    return ride;
}


/**
 * groupByTrip(db, filteredCol)
 *
 * Create a collection grouped by trip by using the minIntervalForNew as delimiter
 *
 * @param {MongoDB.Db} db: The database in which to store the results
 * @param {MongoDB.Collection} filteredCol: The collection we are operating on
 */
function groupByTrip(db, filteredCol) {
    // Using JS approach
    console.log('Grouping by ride...');
    filteredCol.find().sort({
        timestampMs: 1
    }).toArray(function (err, docs) {
        var lastTime = 0,
            results = [],
            ride = {},
            d;
        test.equal(null, err);
        for (var i = 0; i < docs.length; i++) {
            var newTime = docs[i].timestampMs;

            if (newTime - lastTime > C.minIntervalForNew) {
                // Close off the previous ride, if any and push to array
                if (ride && ride.hasOwnProperty('startTime')) {
                    ride = completeRide(ride, lastTime);
                    if (ride.duration > 0) {
                        results.push(ride);
                    }
                    ride = {};
                }
                // Initiate the new ride
                ride.date = docs[i].Date;
                ride.startTimeMs = newTime;
                d = new Date(ride.startTimeMs);
                ride.startTime = d.toTimeString();
                ride.points = [];
            }
            lastTime = newTime;
            var point = {
                timestampMs: newTime,
                confidence: docs[i].confidence
            };
            ride.points.push(point);
        }
        if (ride && ride.hasOwnProperty('startTime')) {
            ride = completeRide(ride, lastTime);
            if (ride.duration > 0) {
                results.push(ride);
            }
        }
        /*
                console.dir(results, {
                    showHidden: false,
                    depth: null,
                    // colors: true
                });
        */
        console.log(results.length + ' rides identified.');
        console.timeEnd('overall');
        process.exit(); /////////////////////
    });
}

/* Main entry point */

MongoClient.connect(C.mongoURL, function (err, db) {
    test.equal(null, err);

    // Access the desired collection
    var rawCol = db.collection('rawData');

    /* TODO: SKIPPING FOR NOW!
    // Delete all records, then if all is well add the new records
    console.log('Deleting records...');
    rawCol.deleteMany({}, function (err) {
        test.equal(null, err);

        readFile(C.fileName, db, rawCol);

    });
    //*/
    // TODO: Remove this section when reading from the file
    filterData(db, rawCol);
    // var filteredCol = db.collection('filteredData');

    // appendDateTime(db, filteredCol);

    // groupByTrip(db, filteredCol);

    //*/

});


// 3. Organize by date (and start time)
// 4. Output report
