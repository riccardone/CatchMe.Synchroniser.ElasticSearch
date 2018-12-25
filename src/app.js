var client = require('node-eventstore-client');
var mapperModule = require('./mapper');
var serviceModule = require('./service');

var log4js = require('log4js');
var loggerConfig = require('./logging/');

loggerConfig.run();
var logger = log4js.getLogger('app');

const credentials = new client.UserCredentials('admin', 'changeit')
var connection = client.createConnection("{'admin':'changeit'}", 'tcp://eventstore:1113', 'catchme-synchroniser-elasticsearch');
connection.on("error", err =>
    logger.info(`Error occurred on connection: ${err}`)
);

connection.on("closed", reason =>
    logger.info(`Connection closed, reason: ${reason}`)
);

connection.connect().catch(err => console.log(err));
connection.once("connected", tcpEndPoint => {

    logger.info(`Connected to eventstore cat ${tcpEndPoint.host}:${tcpEndPoint.port}`);

    var mapperInstance = new mapperModule();
    var serviceInstance = new serviceModule(mapperInstance);

    serviceInstance.start(connection, credentials);
});