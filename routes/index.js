var express = require('express');
var router = express.Router();
var tabulate = require('./tabulate');

/* GET home page. */
router.get('/', function (req, res, next) {
    res.render('index', {
        title: 'gCycle'
    });
});

/* GET run page. -- TODO: MAYBE wrap into index */
router.get('/get-data', function (req, res, next) {
    // Set the noReload option to 1 to avoid re-importing the data into the db
    req.body.options = {noReload: 1};
    tabulate(req, res, next);
});

module.exports = router;
