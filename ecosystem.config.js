module.exports = {
    apps : [{
        name: "Coin Monitor",
        script: "./index.js",
        error_file : "./log/pm2_err.log",
        out_file : "./log/pm2_out.log",
    }]
}