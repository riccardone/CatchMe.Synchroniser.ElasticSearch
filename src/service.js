var logger = require('log4js').getLogger('service');
var indexerModule = require('./indexer');

var busModule = require('./bus');
var eventBus = busModule();

var lastPosition = null;
var resolveLinkTos = false;
var mapperInstance;
var connection;
var subscription;
var _indexer;

function Service(mapper) {
    mapperInstance = mapper;
    _indexer = new indexerModule('http://elasticsearch:9200', 5000);
}

function start(conn, credentials) {
    connection = conn;
    subscription = connection.subscribeToAllFrom(lastPosition, resolveLinkTos, eventAppeard, liveProcessingStarted, subscriptionDropped, credentials);
}

var handlers = {
    'ConnectionAcceptedV1': handleFriendSessionEvent,
    'ConnectionEstablishedV1': handleFriendSessionEvent,
    'FriendDisconnectedV1': handleFriendSessionEvent,
    'GeoInfoUpdatedV1': handleGeoTrackingEvent,
    'TrackingPositionStartedV1': handleGeoTrackingEvent
};

function handleFriendSessionEvent(data, metadata) {
    var doc = mapperInstance.toFriendSessionModel(data, metadata);
    eventBus.publish("diaryLogReceived", doc);
}

function handleGeoTrackingEvent(data, metadata) {
    var doc = mapperInstance.toGeoTrackingModel(data, metadata);
    eventBus.publish("geoTrackingEventReceived", doc);
}

var handle = function (type, data, metadata) {
    handlers[type](data, metadata);
}

//TODO find a way to make this not public but accessible from tests
function processEvent(eventType, data, metadata) {
    handle(eventType, data, metadata);
}

const eventAppeard = (sub, event) => {
    if (isSystemEvent(event.event.eventType)) return;
    if (!handlerFound(event.event.eventType)) return;
    var data = deserialiseEventToString(event.event.data);
    var metadata = deserialiseEventToString(event.event.metadata);

    processEvent(event.event.eventType, data, metadata);
}

function deserialiseEventToString(buffer) {
    return buffer.toString('utf-8');
}

const liveProcessingStarted = () => {
    logger.info("CatchMe Indexing Service LiveProcessingStarted! Listening for new events.")
}

const subscriptionDropped = (subscription, reason, error) =>
    logger.info(error ? error : "Subscription dropped.")

function isSystemEvent(eventType) {
    return eventType[0] == "$"
}

function handlerFound(eventType) {
    return handlers[eventType] !== undefined
}

Service.prototype.start = start;
Service.prototype.processEvent = processEvent;
module.exports = Service;