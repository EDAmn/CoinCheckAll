module.exports = {
    logger: function(_path, record) {
        fs.appendFileSync( _path, new Date().toLocaleString() + ': ' + record + '\n');
    },
    saveToFile: function(_path, data) {
        fs.appendFileSync(_path + '.txt', data);
    },
    prepareBinance(payload) {
        var trades = '[' + payload.replace(/}{/g, '},{') + ']';
        var tradesObject = JSON.parse(trades);
        for (let i = 0; i < tradesObject.length; i++) {
            var trade = {
                e: "binance",
                t: "",
                T: "",
                p: "",
                v: "",
                o: "",
                bigPayload: ""
            };
            if (tradesObject[i].stream == 'btcusdt@trade') { trade.t = 'btcusd'; } else {   }
            trade.T = tradesObject[i].data.T
            trade.p = tradesObject[i].data.p;
            trade.v = tradesObject[i].data.q;
            if (tradesObject[i].data.m == true) {
                trade.o = 's';
            } else {
                trade.o = 'b';
            }
            
            let save = db.insert(trade);
        }
    },
    prepareCoinbase(payload) {
        var trade = {
            e: "coinbase",
            t: "",
            T: "",
            p: "",
            v: "",
            o: "",
            bigPayload: ""
        };
        var trades = JSON.parse(payload);
        if (trades.product_id = 'BTC-USD') { trade.t = 'btcusd' } else {   }
        trade.T = Date.now(); //trades.time
        trade.p = trades.price;
        trade.v = trades.last_size;
        if (trades.side = 'buy') {
            trade.o = 'b';
        } else {
            trade.o = 's';
        }
        
        let save = db.insert(trade);
    },
    prepareKraken(payload) {
        var trades =  JSON.parse(payload);
        for (let i = 0; i < trades[1].length; i++) {
            var trade = {
                e: "kraken",
                t: "",
                T: "",
                p: "",
                v: "",
                o: "",
                bigPayload: ""
            };
            if (trades[3] == 'XBT/USD') {trade.t = 'btcusd';}
            trade.p = trades[1][i][0];
            trade.v = trades[1][i][1];
            trade.T = Date.now(); //trades[1][i][2]
            if (trades[1][i][3] == 'b') {
                trade.o = 'b';
            } else {
                trade.o = 's';
            }
            
            let save = db.insert(trade);
        }
    },
    prepareHuobi(payload) {
        var trades = JSON.parse(payload);
        for (let i = 0; i < trades.tick.data.length; i++) {
            var trade = {
                e:"huobi",
                t:"",
                T:"",
                p:"",
                v:"",
                o:"",
                bigPayload: ""
            };
            if (trades.ch == 'market.btcusdt.trade.detail') {
                trade.t = 'btcusd';
            }
            trade.T = trades.tick.data[i].ts;
            trade.p = trades.tick.data[i].price;
            trade.v = trades.tick.data[i].amount;
            if (trades.tick.data[i].direction == 'buy') {
                trade.o = 'b';
            } else {
                trade.o = 's';
            }
            
            let save = db.insert(trade);
        }

    }
}

const fs = require('fs');
const db = require('./db.js');