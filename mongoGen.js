#!/%PROGRAMFILES%/nodejs/node
/* THIS IS IT ... Run this JS file which has it all */
console.log("Hello, Code Camper MongoDb Generator");
console.log("Remember to first start cc-angular server so the generator can hit the Web API");
console.log("Generator version: 0.1.0" );

require('./najaxAdapter');
var breeze = require('./breeze.debug');
var mongo = require('mongodb');
var ObjectID = mongo.ObjectID;

var Q = require('q');
var nsend = Q.nsend;
var db, em;
var webApiServiceName = "http://localhost:58576/breeze/breeze";

var stats = {
    start: Date.now(),
    end: Date.now()
}
function run(){
    var server = new mongo.Server('localhost', 27017, {auto_reconnect: true});
    db = new mongo.Db('ngCodeCamper', server, {fsync:true}); //{safe: false});

    return nsend(db,'open')
        .then(lookups)
        .fail(reportError)
        .fin(function(){ db.close(); console.log('Closing database')})
        .fin(displayStats);
}
run();

//*** Private functions
function lookups(){

    breeze.config.initializeAdapterInstance('ajax', 'najax', true);
    breeze.NamingConvention.camelCase.setAsDefault();

    em = new breeze.EntityManager(webApiServiceName);
    return new breeze.EntityQuery('Lookups')
        .using(em).execute()
        .then(function(data) {
            var lookups = data.results[0];
            console.log("Got lookups")
            return Q.all([
                writeRooms(lookups.rooms),
                writeTimeSlots(lookups.timeslots),
                writeTracks(lookups.tracks)
            ]);
        }).catch(function(error){
            console.log("Lookups fetch failed: "+(error.message || error));
            console.log("Is the CC Web Api server running");
            throw error;
        });

    function writeRooms( items){
        return getCleanCollection('Rooms')
            .then(function(collection){
                var mStuff = [];
                items.forEach(function(item){
                    mStuff.push({
                        _id: item.id,
                        name: item.name
                    });
                });
                return insertCollection(collection, mStuff);
            })
            .then(confirmSave);
    }
    function writeTimeSlots( items){
        return getCleanCollection('TimeSlots')
            .then(function(collection){
                var mStuff = [];
                items.forEach(function(item){
                    mStuff.push({
                        _id: item.id,
                        start: item.start,
                        isSessionSlot: item.isSessionSlot,
                        duration: item.duration
                    });
                });
                return insertCollection(collection, mStuff);
            })
            .then(confirmSave);
    }
    function writeTracks( items){
        return getCleanCollection('Tracks')
            .then(function(collection){
                var mStuff = [];
                items.forEach(function(item){
                    mStuff.push({
                        _id: item.id,
                        name: item.name
                    });
                });
                return insertCollection(collection, mStuff);
            })
            .then(confirmSave);
    }
}

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



//*** Utility functions ***
function getCleanCollection(collectionName){
    var getCollection = function(){return nsend(db, 'collection', collectionName);};

    return getCollection()
        .then(function(collection) {
            // 'drop' is faster than 'remove' and kills indexes
            return nsend(collection, "drop");
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
function verifyCollection(collection){
    return nsend(collection, 'find')
        .then(function(cursor){
            return nsend(cursor, 'toArray');
        })
        .then(function(items){
            var collectionName = collection.collectionName;
            if (!items || items.length === 0) {
                throw new Error("Expected some '" + collectionName + "' items; didn't get them");
            }
            var count = items.length;
            console.log("Got "+count+" '"+collectionName+"' item(s)");
            if (count>3){
                console.log("First three from '"+collectionName+"':");
                console.dir(items.slice(0,3),'items');
                console.log("...");
            } else {
                console.dir(items,'items') ;
            }
            app.stats[collectionName] = count;
            return collection;
        });
}

function reportError(err){
    console.log('!!! run error:');
    console.dir(err);
}

function displayStats(){
    console.log("\n=== STATS ===");
    console.log(JSON.stringify(stats, null, 2));
}

