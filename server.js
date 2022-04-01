module.exports = async function server() {
    try {
        // Start MongoDB server
        var startDB = await db.connectDB();

        const app = express();
        
        // Helmet
        app.use(
            helmet({
                contentSecurityPolicy: false,
            })
        );

        // CORS
        app.use(function(req, res, next) {
            res.header("Access-Control-Allow-Origin", "http://localhost:5000");
            res.header("Access-Control-Allow-Methods", "GET,POST");
            res.header("Access-Control-Allow-Headers", "Content-Type");
            next();
        });
        
        // Express Basic Auth credentials
        app.use(basicAuth({
            challenge: true,
            users: {'admin': 'xQv48bWvy' }
        }));

        // Body parser
        app.use(express.json({
            type: 'application/json',
            limit: '100kb'
        }));

        app.use(express.urlencoded({
            extended: true
        }));

        // Route files
        const routes = require('./routes/routes.js');

        // Mount routes
        app.use('/', routes);

        // Static files
        app.use('/public', express.static(path.join(__dirname, 'public')))



        const PORT = Number(process.env.PORT);

        app.listen(
            PORT,
            tools.logger(process.env.log, `Server running on port: ${PORT} (server.js)`)
        );
    } catch (e) {
        tools.logger(process.env.log, `Server start failed (server.js): ${e}`);
        process.exit(1);
    }
}

const express = require('express');
const db = require('./db.js');
const tools = require('./tools.js');
const basicAuth = require('express-basic-auth');
const path = require('path');
const helmet = require('helmet');