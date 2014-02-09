#!/%PROGRAMFILES%/nodejs/node
/* THIS IS IT ... Run this JS file which has it all */
console.log("Hello, Code Camper MongoDb Generator");
console.log("Remember to first start cc-angular server so the generator can hit the Web API");
console.log("Generator version: 0.1.0" );

/* Breeze config */
var breeze = require('./breeze.debug');
require('./najaxAdapter'); // adapts breeze to use the najax adapter for XHR calls
breeze.config.initializeAdapterInstance('ajax', 'najax', true);
breeze.NamingConvention.camelCase.setAsDefault();

// Entity source data comes from CC-Angular Web API/SQL server
var webApiServiceName = "http://localhost:58576/breeze/breeze";
var em = new breeze.EntityManager(webApiServiceName);

/* Mongo config */
var mongo = require('mongodb');
var server = new mongo.Server('localhost', 27017, {auto_reconnect: true});
var db = new mongo.Db('ngCodeCamper', server, {fsync:true}); //{safe: false});
var ObjectID = mongo.ObjectID; // Don't need this?

// nsend is Q.js helper to convert node async operation into a Q promise
var Q = require('q');
var nsend = Q.nsend;

// Stats from this run
var stats = {
    start: null,
    end: null
}

run();

//*** Private functions
function run(){

    stats.start = Date.now();
    return nsend(db,'open')
        .then(function(){
            nsend(db,'dropDatabase');
        })
        .then(function(){return Q.all([
            lookups(),
            transfer('Persons', personMapper),
            transfer('Sessions', sessionMapper)
        ]);})
        .then(getSpeakers) // to demonstrate how to do this
        .fail(reportError)
        .fin(function(){ db.close(); console.log('Closing database');})
        .fin(displayStats);
}

function lookups(){

    return new breeze.EntityQuery('Lookups')
        .using(em).execute()
        .then(function(data) {
            var lookups = data.results[0];
            console.log("Got lookups")
            return Q.all([
                writeCollection('Rooms', roomMapper, lookups.rooms),
                writeCollection('TimeSlots', timeSlotMapper, lookups.timeslots),
                writeCollection('Tracks', trackMapper, lookups.tracks)
            ]);
        }).catch(function(error){
            console.log("Lookups fetch failed: "+(error.message || error));
            console.log("Is the CC Web Api server running");
            throw error;
        });
}

function getSpeakers(){
    return getCollection("Sessions")
        .then(function(collection){
            return nsend(collection, 'distinct', 'speakerId')
        })
        .then(function(speakerIds){
            if (!speakerIds){
                throw new Error("mongoDb did not return a result when getting distinct sessions.speakerIds");
            }
            // get a projection of these speakers (persons among the speakerIds)
            return getPersonsWithIds(speakerIds, {firstName:1, lastName:1});
        })
        .catch(function(error){
            console.log("Get speakers failed: "+(error.message || error));
            throw error;
        });
}

function getPersonsWithIds(ids, projection){
    return getCollection("Persons")
        .then(function(collection){
            return nsend(collection, 'find', {_id: {$in: ids}}, projection);
        })
        .then(function(cursor){
            return nsend(cursor, 'toArray');
        })
        .then(function(items){
            reportArray("Speakers", items);
        });
}

function roomMapper(item){
    return {
        _id: item.id,
        name: item.name
    }
}

function timeSlotMapper(item){
    return {
        _id: item.id,
        start: item.start,
        isSessionSlot: item.isSessionSlot,
        duration: item.duration
    }
}

function trackMapper(item){
    return {
        _id: item.id,
        name: item.name
    }
}

function personMapper(item){
    return {
        _id: item.id,
        firstName: item.firstName,
        lastName: item.lastName,
        email: item.email,
        blog: item.blog,
        twitter: item.twitter,
        gender: item.gender,
        imageSource: item.imageSource,
        bio: item.bio
    }
}

function sessionMapper(item){
    return {
        _id: item.id,
        title: item.title,
        code: item.code,
        speakerId: item.speakerId,
        trackId: item.trackId,
        timeSlotId: item.timeSlotId,
        roomId: item.roomId,
        level: item.level,
        tags: item.tags,
        description: item.description
    }
}

/*** Utility functions ***/

// Less verbose alternative to 'verifyCollection'
function confirmSave(collection){
    var cname = collection.collectionName;
    return nsend(collection, "find")
        .then(function(cursor){
            return nsend(cursor, "toArray");
        })
        .then(function(items){
            if (!items || items.length === 0) {
                throw new Error("Expected some '"+cname+"'; didn't get them");
            }
            console.log("Got "+items.length+" '"+cname+"'");
            stats[cname] = items.length;
            return(collection);
        });
}
function displayStats(){
    stats.end = Date.now();
    console.log("\n=== STATS ===");
    console.log(JSON.stringify(stats, null, 2));
}
function getCollection(collectionName){
    return nsend(db, 'collection', collectionName);
}
function getCleanCollection(collectionName){
    var getCollection = function(){return nsend(db, 'collection', collectionName);};

    return getCollection()
        .then(function(collection) {
            // 'drop' is faster than 'remove' and kills indexes
            return nsend(collection, 'drop');
        })
        // whether 'drop' succeeds or fails, create the new collection
        .then(getCollection, getCollection)
        // Paranoia testing
        .then(function(collection){
            return nsend(collection, 'count')
                .then(function(count){
                    if (count !=0) {
                        throw new Error("test collection not empty after remove call");
                    }
                    return collection;
                });
        })
}
function insertCollection(collection, data){
    console.log("Writing "+collection.collectionName);
    return nsend(collection, 'insert', data)
        .then(function(){
            return collection});
}
function reportArray(name, items){
    if (!items || items.length === 0) {
        throw new Error("Expected some '" + name + "' items; didn't get them");
    }
    var count = items.length;
    console.log("Got "+count+" '"+name+"' item(s)");
    if (count>3){
        console.log("First three from '"+name+"':");
        console.dir(items.slice(0,3),'items');
        console.log("...");
    } else {
        console.dir(items,'items') ;
    }
    stats[name] = items.length;
}
function reportError(err){
    console.log('!!! run error:');
    console.dir(err);
}
function transfer(resourceName, mapper){
    return new breeze.EntityQuery(resourceName)
        .using(em).execute()
        .then(function(data) {
            console.log("Got "+resourceName)
            return writeCollection(resourceName, mapper, data.results);
        }).catch(function(error){
            console.log(resourceName + " fetch failed: "+(error.message || error));
            throw error;
        });
}
// verify that the Mongo collection was written and display first 3 items
function verifyCollection(collection){
    return nsend(collection, 'find')
        .then(function(cursor){
            return nsend(cursor, 'toArray');
        })
        .then(function(items){
            var collectionName = collection.collectionName;
            reportArray(collectionName, items);
            return collection;
        });
}
function writeCollection(collectionName, mapper, items){
    return getCleanCollection(collectionName)
        .then(function(collection){
            var mapped = items.map(mapper);
            return insertCollection(collection, mapped);
        })
        .then(verifyCollection);
}
