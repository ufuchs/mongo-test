/*jslint node: true*/
/*jslint nomen: true*/

/*!
 * index
 * Copyright(c) 2014 Vincent Schöttke
 * MIT Licensed
 */

'use strict';

var mongodb = require('mongodb'),
    MongoClient = mongodb.MongoClient,
    q = require('q'),
    _ = require('lodash'),
    sampledata = require('./sampledata.json'),
    startDate = new Date("1970-01-01"),
    endDate = new Date("2015-01-01"),
    currDate,
    i;

function addJson(collection, json, date) {
    var saveData = _.cloneDeep(json);
    saveData.current_observation.observation_epoch = date.getTime() / 1000;
    saveData.current_observation.observation_time_rfc822 = date.toString();
    saveData.current_observation.temp_c = Math.random() * 100;
    return function () {
        console.log("Storing", new Date().toISOString());
        return q.ninvoke(collection, "insert", saveData).then(function (docs) {
        });
    };
}

if (process.argv[2] === 'prepare') {
    q.nfcall(MongoClient.connect, 'mongodb://127.0.0.1:27017/weather').then(function (db) {
        var collection = db.collection('test_no_separate'),
            promiseChain = q();

        console.log("Connected");

        for (currDate = startDate; currDate < endDate; currDate.setDate(currDate.getDate() + 1)) {
            promiseChain = promiseChain.then(addJson(collection, sampledata, currDate));
        }

        return promiseChain.then(function () {
            console.log("Finished adding");
        }).finally(function (err) {
            console.log("closing db", err);
            db.close();
        });
    }).fail(function (err) {
        console.log(err);
    }).done(function () {
        console.log("ALL COMPLETE");
    });
} else if (process.argv[2] === "check") {
    q.nfcall(MongoClient.connect, 'mongodb://127.0.0.1:27017/weather').then(function (db) {
        console.log("DB opened");
        var collection = db.collection('test_no_separate');
        return q.ninvoke(collection.find(), "toArray").then(function (array) {
            console.log("Array length:", array.length);
        }).finally(function () {
            console.log("closing db");
            db.close();
        });
    }).fail(function (err) {
        console.log(err);
    }).done(function () {
        console.log("finished");
    });
} else if (process.argv[2] === "count") {
    q.nfcall(MongoClient.connect, 'mongodb://127.0.0.1:27017/weather').then(function (db) {
        console.log("DB opened");
        var collection = db.collection('test_no_separate');
        return q.ninvoke(collection, "count").then(function (count) {
            console.log("COUNT:", count);
        }).finally(function () {
            console.log("closing db");
            db.close();
        });
    }).fail(function (err) {
        console.log(err);
    }).done(function () {
        console.log("finished");
    });
} else if (process.argv[2] === 'query') {
    var startEpoch = +(new Date(process.argv[3]) / 1000) || 1262304000,
        endEpoch = +(new Date(process.argv[4]) / 1000) || startEpoch;

    q.nfcall(MongoClient.connect, 'mongodb://127.0.0.1:27017/weather').then(function (db) {
        console.log("DB opened");
        var collection = db.collection('test_no_separate');
        return q.ninvoke(collection.find({
            "current_observation.observation_epoch": {
                $gte: startEpoch,
                $lte: endEpoch
            }
        }, {
            "current_observation.observation_epoch": 1,
            "current_observation.temp_c": 1
        }), "toArray").then(function (array) {
            console.log("Result:", array);
        }).finally(function () {
            console.log("closing db");
            db.close();
        });
    }).fail(function (err) {
        console.log(err);
    }).done(function () {
        console.log("finished");
    });
} else if (process.argv[2] === 'mapreduce') {
    var startEpoch = +(new Date(process.argv[3]) / 1000) || 1,
        endEpoch = +(new Date(process.argv[4]) / 1000) || Number.MAX_VALUE;

    q.nfcall(MongoClient.connect, 'mongodb://127.0.0.1:27017/weather').then(function (db) {
        console.log("DB opened");
        var collection = db.collection('test_no_separate');

        var mapFunc = function () {
            emit("test", this.current_observation.temp_c);
        };

        var reduceFunc = function (key, temp) {
            return Array.avg(temp);
        };

        return q.ninvoke(collection, "mapReduce", mapFunc, reduceFunc, {
            out: {
                inline: 1
            },
            verbose: true,
            query: {
                "current_observation.observation_epoch": {
                    $gte: startEpoch,
                    $lte: endEpoch
                }
            }
        }).then(function (results, stats) {
            console.log("Result:", results);
            console.log("Stats:", stats);
        }).finally(function () {
            console.log("closing db");
            db.close();
        });
    }).fail(function (err) {
        console.log(err);
    }).done(function () {
        console.log("finished");
    });
}
