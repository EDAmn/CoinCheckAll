module.exports = {
    connectDB: async function() {
        try {
            const connection = await mongoose.connect(process.env.Mongo_URI, {
                useNewUrlParser: true,
                useCreateIndex: true,
                useFindAndModify: false,
                useUnifiedTopology: true
            });
            tools.logger(process.env.log, `MongoDB Connected: ${connection.connection.host} (db.js)`);
        } catch (e) {
            tools.logger(process.env.log, `MongoDB Connection failed (db.js): ${e}`);
            process.exit(1);
        }
    },
    insert: async function(data) {
        try {
            var write = await tas.create(data);    
        } catch (e) {
            tools.logger(process.env.log, `Writing to database failed (db.js): ${e}`);
        }
    }
}

const mongoose = require('mongoose');
const tools = require('./tools.js');

// T&S schema
const tasSchema = new mongoose.Schema({
    e: String,
    t: String,
    T: {},
    p: Number,
    v: Number,
    o: String,
    bigPayload: Number
    },{
    collection: 'tas'
});

// Models
const tas = mongoose.model('tas', tasSchema);