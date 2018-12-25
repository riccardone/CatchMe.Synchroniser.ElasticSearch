var moment = require('moment');

module.exports = GeoTrackingModel;

function GeoTrackingModel(obj, metadata, id) {

    var applies = moment(metadata.Applies);

    var defaultState = {
        Id: id,
        Lon: '',
        Lat: '',
        Speed: '',
        Heading: '',
        Altitude: '',
        Applies: applies.toISOString(),
        Source: '',
        CorrelationId: metadata['$correlationId']
    };

    if (obj.hasOwnProperty('Source'))
        defaultState.Source = obj.Source;

    if (obj.hasOwnProperty('Message'))
        defaultState.Message = obj.Message;

    return defaultState;
}