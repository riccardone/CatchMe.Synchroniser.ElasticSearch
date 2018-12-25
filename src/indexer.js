var elasticsearch = require('elasticsearch');
var busModule = require('./bus');
var logger = require('log4js').getLogger('indexer');

var eventBus = busModule();
var _docsReady = [];
var _friendSessionDocsReady = [];
var _client;
var _interval;
var _flushInterval;
var _link;

function Indexer(link, flushInterval) {
    _link = link;
    _flushInterval = flushInterval;
    init();
    subscribeMe();
}

function subscribeMe() {
    eventBus.subscribe("friendSessionEventReceived", (doc) => {
        if (!_friendSessionDocsReady) {
            _friendSessionDocsReady = [];
        }
        _friendSessionDocsReady.push(doc);
    });
    eventBus.subscribe("geoTrackingEventReceived", (doc) => {
        if (!_docsReady) {
            _docsReady = [];
        }
        _docsReady.push(doc);
    });
}

function init() {
    _client = new elasticsearch.Client({
        host: _link
    });
    initIndex("friendSessions", putMappingsForFriendSessionEvents)
        .then((response) => {
            return initIndex("geoTrakings", putMappings);
        }).then((response) => {
            _interval = setInterval(function () {
                _docsReady = flusher(_docsReady, "geoTrakings", "geoTracking");
                _friendSessionDocsReady = flusher(_friendSessionDocsReady, "friendSessions", "friendSession");
            }, _flushInterval);
        }).catch((error) => {
            logger.error(error);
        })
}

function initIndex(indexName, mappingFunc) {
    return _client.indices.exists({ index: indexName })
        .then(function (indexExists) {
            if (!indexExists) {
                return _client.indices.create({ index: indexName })
                    .then(function (response) {
                        logger.info("Index '" + indexName + "' created");
                        return mappingFunc();
                    });
            }
        });
}

function flusher(docs, indexName, docType) {
    if (docs && docs.length > 0) {
        indexDocuments(docs, indexName, docType).then(function (resp) {
            if (resp.errors) {
                logger.error("bulk operation on '" + indexName + "' index completed with errors");
                return docs;
            } else {
                logger.info("stored or refreshed " + docs.length + " documents in '" + indexName + "' index");
                return [];
            }
        });
    } else {
        return docs;
    }
}

var stopIndexer = function () {
    clearInterval(_interval);
}

function putMappingsForFriendSessionEvents() {
    return _client.indices.putMapping({
        "index": "friendSessions",
        "type": "friendSession",
        "body": {
            "friendSessionEvent": {
                "properties": {
                    "Id": {
                        "type": "text"
                    },
                    "OwnerId": {
                        "type": "text"
                    },
                    "FriendId": {
                        "type": "text"
                    },
                    "Applies": {
                        "type": "date"
                    },
                    "Source": {
                        "type": "text"
                    }
                }
            }
        }
    });
}

function putMappings() {
    return _client.indices.putMapping({
        "index": "geoTrakings",
        "type": "geoTracking",
        "body": {
            "geoTrackingEvent": {
                "properties": {
                    "Id": {
                        "type": "text"
                    },
                    "Lon": {
                        "type": "double"
                    },
                    "Lat": {
                        "type": "double"
                    },
                    "Speed": {
                        "type": "double"
                    },
                    "Heading": {
                        "type": "double"
                    },
                    "Altitude": {
                        "type": "double"
                    },
                    "Message": {
                        "type": "text"
                    },
                    "Applies": {
                        "type": "date"
                    },
                    "Source": {
                        "type": "text"
                    },
                    "CorrelationId": {
                        "type": "text"
                    }
                }
            }
        }
    });
}

function indexDocuments(documents, indexName, docType) {
    var br = [];
    function create_bulk(bulk_request) {
        var obj

        for (i = 0; i < documents.length; i++) {
            obj = documents[i]
            bulk_request.push({ index: { _index: indexName, _type: docType, _id: obj.Id } });
            bulk_request.push(obj);
        }
        return bulk_request;
    };
    create_bulk(br);
    // Standard function of ElasticSearch to use bulk command
    return _client.bulk(
        {
            body: br
        });
}

module.exports = Indexer;