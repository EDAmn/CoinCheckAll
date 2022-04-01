require('dotenv').config();
const tools = require('./tools.js');
const stream = require('./stream.js');
const server = require('./server.js');
const python = require('./analyze/python.js');
const cron = require('node-cron');

/*  
 * Is Websocket Stream alive?
 * {object} exchange: boolean or string
 * For first connection use false
 * 'off' = Connection will not be opened
 * true = Connection is opened
 * false = Connection is stopped and will be reconnected
 * 'error' = Connection was opened but server returned error, reconnection is stopped
*/ 
global.isStreamAlive = {
    binance: false,
    coinbase: false,
    kraken: false,
    huobi: false
}

// @desc    Active websockets objects
let binance,
    coinbase,
    kraken,
    huobi;

// @desc    Start and check if websocket is alive
function handleStream() {
    try {
        Object.keys(global.isStreamAlive).forEach(function(key) {
            //console.log(key, global.isStreamAlive[key]);
            if (global.isStreamAlive[key] == false) {
                if (key == 'binance') {
                    binance = null; 
                    tools.logger(process.env.binance_log, 'Connection initialized (index.js)');
                    binance = stream.openBinance();
                }
                if (key == 'coinbase') {
                    coinbase = null;
                    tools.logger(process.env.coinbase_log, 'Connection initialized (index.js)');
                    coinbase = stream.openCoinbase();
                }
                if (key == 'kraken') {
                    kraken = null;
                    tools.logger(process.env.kraken_log, 'Connection initialized (index.js)');
                    kraken = stream.openKraken();
                }
                if (key == 'huobi') {
                    huobi = null;
                    tools.logger(process.env.huobi_log, 'Connection initialized (index.js)');
                    huobi = stream.openHuobi();
                }
            }
        });        
    } catch (e) {
        tools.logger(process.env.log, 'handleStream() failed (index.js): ' + e);
    }
}

// @desc    CRON Schedules for functions
var cronStream = cron.schedule(String(process.env.Websockets), function() {
    handleStream();
    }, {
    scheduled: false
});

var cronAnalyzeData = cron.schedule(String(process.env.AnalyzeData), function() {
    python.analyzeTas();
    }, {
    scheduled: false
});

// @desc    Global error handlers
process.on('uncaughtException', function(err) {
    tools.logger(process.env.log, `Global uncaughtException message (index.js): ${err.message}`);
    tools.logger(process.env.log, `Global uncaughtException stack (index.js): ${err.stack}`);
    process.exit(1);
});

process.on('unhandledRejection', function(err, promise) {
    tools.logger(process.env.log, `Global unhandledRejection message (index.js): ${err.message}`);
    tools.logger(process.env.log, `Global unhandledRejection stack (index.js): ${err.stack}`);
    process.exit(1);
})


// @desc    Start server, websockets and python scripts
var init = async function() {
    await server();
    handleStream();
    cronStream.start();
    cronAnalyzeData.start();
}

init();