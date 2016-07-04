/* PACKAGES */
var util = require('util'),
    fs = require('fs'),
    MongoClient = require('mongodb').MongoClient,
    JSONStream = require('JSONStream'),
    test = require('assert');

/**
 * tabulate: The main middleware function
 * @param {object} req - http request (standard)
 * @param {object} res - http response (standard)
 * @param {callback function} next - next step in express chain
 */
var tabulate = function (req, res, next) {

    /* GLOBALS - CONSTANTS */
    var C = {};
    C.fileName = '..\\gCycleData\\Takeout\\Location History\\LocationHistory.json';
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
                    // Only locations with activity 'onBicycle'
                    $match: {
                        'activitys.activities.type': 'onBicycle'
                    }
                },
                // Project only required fields
                {
                    $project: {
                        _id: 0,
                        'activitys.timestampMs': 1,
                        'activitys.activities.type': 1,
                        'activitys.activities.confidence': 1
                    }
                },
                // Flatten
                {
                    $unwind: '$activitys'
                            },
                {
                    $unwind: '$activitys.activities'
                            },
                {
                    $project: {
                        timestampMs: '$activitys.timestampMs',
                        type: '$activitys.activities.type',
                        confidence: '$activitys.activities.confidence'
                    }
                },
                // Sort by decreasing confidence level
                {
                    $sort: {
                        timestampMs: 1,
                        confidence: -1
                    }
                },
                // Take the first record from each timestamp (max confidence)
                {
                    $group: {
                        _id: '$timestampMs',
                        type: {
                            $first: '$type'
                        },
                        confidence: {
                            $first: '$confidence'
                        }
                    }
                },
                // Remove any records that are not 'onBicycle'
                {
                    $match: {
                        'type': 'onBicycle'
                    }
                },
                // Project to rename '_id' to 'timestampMs'
                {
                    $project: {
                        timestampMs: '$_id',
                        type: 1,
                        confidence: 1
                    }
                }

        ], {
                allowDiskUse: true
            });

            // Insert into the filtered collection any records with onBicycle
            filtered.toArray(function (err, filteredData) {
                test.equal(null, err);
                test(filteredData.length > 0);

                /* TESTING ONLY!!!! */
                // res.json(filteredData.slice(0, 20));
                //*/

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
            res.json(results);
        });
    }

    /* Main entry point */
    var opts = {};
    if (req.body.hasOwnProperty('options')) {
        opts = req.body.options;
    } else if (req.query.hasOwnProperty('noReload')) {
        // Allow this parameter in the query string
        opts.noReload = req.query.noReload;
    }

    /* REMOVE */
    //opts.fileName = '..\\gCycleData\\Takeout\\Location History\\LocationHistory.json';
    //opts.noReload = 0;
    /* END REMOVE */

    MongoClient.connect(C.mongoURL, function (err, db) {

        console.time('overall');

        test.equal(null, err);

        // Access the desired collection
        var rawCol = db.collection('rawData');

        // Load the records - skip this if noReload set
        if (!opts.hasOwnProperty('noReload') || !opts.noReload) {

            // Delete all records, then if all is well add the new records
            console.log('Deleting records...');
            rawCol.deleteMany({}, function (err) {
                test.equal(null, err);

                var fileName = (opts.hasOwnProperty('fileName') ? opts.fileName : C.fileName);

                readFile(fileName, db, rawCol);

            });
        } else {
            // Skip diretly to filterData
            filterData(db, rawCol);
        }

    });

};

module.exports = tabulate;

// For testing purposes ONLY!!!
if (require.main === module) {
    var options = {
        noReload: 1,
        minConfidence: 30,
        fileName: '..\\..\\gCycleData\\Takeout\\Location History\\test.json'
    };

    var args = process.argv.slice(2);
    for (var i = 0; i < args.length; i++) {
        if (args[i] === '--reload' || args[i] === '-r') {
            options.noReload = 0;
        }
    }
    tabulate({
        body: {
            options: options
        }
    }, {
        json: function (data) {
            console.dir(data, {
                showHidden: false,
                depth: null,
                colors: true
            });
            process.exit();
        },
    }, function next() {
        process.exit();
    });
}
