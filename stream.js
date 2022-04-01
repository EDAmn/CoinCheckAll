module.exports = {
    // @desc    Function for open and handle Binance websocket
    // @returns {Object} binance websocket object
    openBinance: async function() {
        try {
            var binance = new WebSocket(process.env.binance_wss);
            binance.on('open', function(connection) {
                //binance.send(JSON.stringify({method: "SUBSCRIBE", params:["btcusdt@trade","btceur@trade"], id: 1}));
                global.isStreamAlive.binance = true;
                /* heartbeat*/
                clearTimeout(binance.pingTimeout);
                binance.pingTimeout = setTimeout(function() {
                    binance.terminate();
                    global.isStreamAlive.binance = false;
                    tools.logger(process.env.binance_log, 'Heartbeat timeout (stream.js)');
                }, Number(process.env.binance_heartbeat));
                /* heartbeat*/
                tools.logger(process.env.binance_log, 'Connection opened (stream.js)');
            });
            binance.on('error', function(error) {
                clearTimeout(binance.pingTimeout);
                global.isStreamAlive.binance = 'error';
                tools.logger(process.env.binance_log, 'Error from server (stream.js): ' + error);
            });
            binance.on('ping', function() {
                /* heartbeat*/
                clearTimeout(binance.pingTimeout);
                binance.pingTimeout = setTimeout(function() {
                    binance.terminate();
                    global.isStreamAlive.binance = false;
                    tools.logger(process.env.binance_log, 'Heartbeat timeout (stream.js)');
                }, Number(process.env.binance_heartbeat));
                /* heartbeat*/
            });
            binance.on('message', function(data) {
                try {
                    let trade = tools.prepareBinance(data);
                } catch (error) {
                    tools.logger(process.env.binance_log, `Data save failed (stream.js): ${error}`);
                    tools.logger(process.env.binance_log, `Payload (stream.js): ${data}`);
                }
            });
            binance.on('close', function() {
                clearTimeout(binance.pingTimeout);
                binance.terminate();
                global.isStreamAlive.binance = false;
                tools.logger(process.env.binance_log, 'Connection closed by exchange server (stream.js)');
            });
           
            return binance

        } catch (e) {
            global.isStreamAlive.binance = 'error';
            tools.logger(process.env.binance_log, 'Fatal error (stream.js)' + e);
        }
    },
    // @desc    Function for open and handle Coinbase Pro websocket
    // @returns {Object} coinbase websocket object
    openCoinbase: async function() {
        try {
            //var coinbase = new WebSocket('wss://ws-feed-public.sandbox.pro.coinbase.com');
            var coinbase = new WebSocket(process.env.coinbase_wss);
            coinbase.on('open', function(connection) {
                coinbase.send(
                    // "BTC-USD","ETH-USD"
                    JSON.stringify({
                        type: "subscribe",
                        channels: [
                            {name: "heartbeat", product_ids: ["BTC-USD"] },
                            {name: "ticker", product_ids: ["BTC-USD"] }
                        ]})
                );
                global.isStreamAlive.coinbase = true;
                /* heartbeat*/
                clearTimeout(coinbase.pingTimeout);
                coinbase.pingTimeout = setTimeout(function() {
                    coinbase.terminate();
                    global.isStreamAlive.coinbase = false;
                    tools.logger(process.env.coinbase_log, 'Heartbeat timeout (stream.js)');
                }, Number(process.env.coinbase_heartbeat));
                /* heartbeat*/
                tools.logger(process.env.coinbase_log, 'Connection opened (stream.js)');
            });
            coinbase.on('message', function(data) {
                try {
                    let payload = JSON.parse(data);
                    if (payload.type == 'heartbeat' && payload.product_id == 'BTC-USD') {
                        /* heartbeat*/
                        clearTimeout(coinbase.pingTimeout);
                        coinbase.pingTimeout = setTimeout(function() {
                            coinbase.terminate();
                            global.isStreamAlive.coinbase = false;
                            tools.logger(process.env.coinbase_log, 'Heartbeat timeout (stream.js)');
                        }, Number(process.env.coinbase_heartbeat));
                        /* heartbeat*/
                    }
                    if (payload.type == 'ticker') {
                        let trade = tools.prepareCoinbase(data);
                    }
                } catch (error) {
                    tools.logger(process.env.coinbase_log, `Data save failed (stream.js): ${error}`);
                    tools.logger(process.env.coinbase_log, `Payload (stream.js): ${data}`);
                }
            });
            coinbase.on('error', function(error) {
                clearTimeout(coinbase.pingTimeout);
                global.isStreamAlive.coinbase = 'error';
                tools.logger(process.env.coinbase_log, 'Error from server (stream.js): ' + error);
            });
            coinbase.on('close', function() {
                clearTimeout(coinbase.pingTimeout);
                coinbase.terminate();
                global.isStreamAlive.coinbase = false;
                tools.logger(process.env.coinbase_log, 'Connection closed by exchange server (stream.js)');
            });
           
            return coinbase

        } catch (e) {
            global.isStreamAlive.coinbase = 'error';
            tools.logger(process.env.coinbase_log, 'Fatal error (stream.js)' + e);
        }
    },
    // @desc    Function for open and handle Kraken websocket
    // @returns {Object} kraken websocket object
    openKraken: async function() {
        try {
            var kraken = new WebSocket(process.env.kraken_wss);
            kraken.on('open', function(connection) {
                kraken.send(JSON.stringify({
                    event: "subscribe",
                    pair:["XBT/USD"],
                    subscription: {
                        name: "trade"
                    }
                }));
                global.isStreamAlive.kraken = true;
                /* heartbeat*/
                kraken.pingTimeout = setTimeout(function() {
                    kraken.send(JSON.stringify({event: "ping"}));
                    kraken.pongTimeout = setTimeout(function() {
                        kraken.terminate();
                        global.isStreamAlive.kraken = false;
                        tools.logger(process.env.kraken_log, 'Heartbeat timeout (stream.js)');
                    }, Number(process.env.kraken_heartbeat));
                }, Number(process.env.kraken_heartbeat));
                /* heartbeat*/
                tools.logger(process.env.kraken_log, 'Connection opened (stream.js)');
            });
            kraken.on('error', function(error) {
                clearTimeout(kraken.pingTimeout);
                clearTimeout(kraken.pongTimeout);
                global.isStreamAlive.kraken = 'error';
                tools.logger(process.env.kraken_log, 'Error from server (stream.js): ' + error);
            });
            kraken.on('message', function(data) {
                try {
                    var payload = JSON.parse(data);
                    /* heartbeat */
                    if (payload.event == 'pong') {
                        clearTimeout(kraken.pongTimeout);
                        kraken.pingTimeout = setTimeout(function() {
                            kraken.send(JSON.stringify({event: "ping"}));
                            kraken.pongTimeout = setTimeout(function() {
                                kraken.terminate();
                                global.isStreamAlive.kraken = false;
                                tools.logger(process.env.kraken_log, 'Heartbeat timeout (stream.js)');
                            }, Number(process.env.kraken_heartbeat));
                        }, Number(process.env.kraken_heartbeat));
                    }
                    /* heartbeat */
                    if (payload[2] == 'trade') {
                        let trade = tools.prepareKraken(data);
                    }
                } catch (error) {
                    tools.logger(process.env.kraken_log, `Data save failed (stream.js): ${error}`);
                    tools.logger(process.env.kraken_log, `Payload (stream.js): ${data}`);
                }
            });
            kraken.on('close', function() {
                clearTimeout(kraken.pingTimeout);
                clearTimeout(kraken.pongTimeout);
                kraken.terminate();
                global.isStreamAlive.kraken = false;
                tools.logger(process.env.kraken_log, 'Connection closed by exchange server (stream.js)');
            });

            return kraken

        } catch (e) {
            global.isStreamAlive.kraken = 'error';
            tools.logger(process.env.kraken_log, 'Fatal error (stream.js)' + e);
        }
    },
    // @desc    Function for open and handle Huobi websocket
    // @returns {Object} Huobi websocket object
    openHuobi: async function() {
        try {
            var rnd = Math.floor(1000 + Math.random() * 9000);
            var huobi = new WebSocket(process.env.huobi_wss);
            huobi.on('open', function(connection) {
                huobi.send(JSON.stringify({
                    sub: "market.btcusdt.trade.detail",
                    id: rnd
                }));
                global.isStreamAlive.huobi = true;
                /* heartbeat*/
                clearTimeout(huobi.pingTimeout);
                huobi.pingTimeout = setTimeout(function() {
                    huobi.terminate();
                    global.isStreamAlive.huobi = false;
                    tools.logger(process.env.huobi_log, 'Heartbeat timeout (stream.js)');
                }, Number(process.env.huobi_heartbeat));
                /* heartbeat*/
                tools.logger(process.env.huobi_log, 'Connection opened (stream.js)');
            });
            huobi.on('error', function(error) {
                clearTimeout(huobi.pingTimeout);
                global.isStreamAlive.huobi = 'error';
                tools.logger(process.env.huobi_log, 'Error from server (stream.js): ' + error);
            });
            huobi.on('message', function(buffer) {
                zlib.unzip(buffer, function(err, resp) {
                    if (err) {
                        tools.logger(process.env.huobi_log, `Unzip payload error: ${err}`);
                        return;
                    }
                    var payload = JSON.parse(resp);

                    if (payload.ping) {
                        huobi.send(JSON.stringify({
                            pong: payload.ping
                        }));
                        /* heartbeat*/
                        clearTimeout(huobi.pingTimeout);
                        huobi.pingTimeout = setTimeout(function() {
                            huobi.terminate();
                            global.isStreamAlive.huobi = false;
                            tools.logger(process.env.huobi_log, 'Heartbeat timeout (stream.js)');
                        }, Number(process.env.huobi_heartbeat));
                        /* heartbeat*/
                    }

                    if (payload.ch) {
                        try {
                            let trade = tools.prepareHuobi(resp);
                        } catch (error) {
                            tools.logger(process.env.huobi_log, `Data save failed (stream.js): ${error}`);
                            tools.logger(process.env.huobi_log, `Payload (stream.js): ${resp}`);
                        }
                    }
                });
            });
            huobi.on('close', function() {
                clearTimeout(huobi.pingTimeout);
                huobi.terminate();
                global.isStreamAlive.huobi = false;
                tools.logger(process.env.huobi_log, 'Connection closed by exchange server (stream.js)');
            });
           
            return huobi

        } catch (e) {
            global.isStreamAlive.huobi = 'error';
            tools.logger(process.env.huobi_log, 'Fatal error (stream.js)' + e);
        }
    }
}

const WebSocket = require('ws');
const tools = require('./tools.js');
const zlib = require('zlib');