var geoTrackingModel = require('./readmodel/geoTrackingModel');
var friendSessionModel = require('./readmodel/friendSessionModel');
const uuidv5 = require('uuid/v5');

module.exports = Mapper;

const my_namespace = 'f11c5317-06bb-47ad-b589-2b8e8332eecd';

function Mapper() { }

Mapper.prototype.toGeoTrackingModel = function (dataAsString, metadataAsString, logType) {
    var data = deserialiseEventToObject(dataAsString);
    var metadata = deserialiseEventToObject(metadataAsString);
    metadata.LogType = logType;
    // Deterministic id for the elasticsearch doc
    var id = uuidv5(metadata['$correlationId'] + metadata.Applies + dataAsString, my_namespace);
    return new geoTrackingModel(data, metadata, id);
}

Mapper.prototype.toFriendSessionModel = function (dataAsString, metadataAsString) {
    var data = deserialiseEventToObject(dataAsString);
    var metadata = deserialiseEventToObject(metadataAsString);
    return new friendSessionModel(data, metadata);
}

Mapper.prototype.getDocumentKey = function (metadataAsString) {
    var metadata = deserialiseEventToObject(metadataAsString);
    return metadata.$correlationId;
}

function deserialiseEventToObject(str) {
    return JSON.parse(str);
}