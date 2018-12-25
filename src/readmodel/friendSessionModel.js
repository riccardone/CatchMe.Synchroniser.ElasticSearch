var moment = require('moment');

module.exports = FriendSessionModel;

function FriendSessionModel(obj, metadata) {

    var applies = moment(metadata.Applies);

    var model = {
        Id: metadata['$correlationId'],
        OwnerId: obj.OwnerId,
        FriendId: obj.FriendId,
        Applies: applies.toISOString(),
        Source: metadata['Source']
    };
    return model;
}