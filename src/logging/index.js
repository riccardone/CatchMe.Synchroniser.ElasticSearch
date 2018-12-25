const log4js = require('log4js');

module.exports = {
    run : function(){
        log4js.configure({
            appenders:{
                debug : { type: 'console', category: 'debug' }                
            },
            categories: 
            { 
                default: { 
                    appenders: 
                        ['debug'], 
                        level: 'debug'
                } 
            }
        });
    }
};