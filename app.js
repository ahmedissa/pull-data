
require('dotenv').load();

var mongo = require('mongoskin');
var CronJob = require('cron').CronJob;
var moment = require('moment');
var request = require('request');
var async = require("async");

var Server = mongo.Server;

var db = mongo.db("mongodb://localhost:27017/riot_data", {native_parser:true});

db.bind('matchId');

db.bind('matchs');


var jobPullMatchIds = new CronJob({
  cronTime: '*/5 * * * *',
  onTick: function() {
		var minValue, date, url; 
		date = moment().subtract(30, 'minutes');
		minValue = date.get('minute');
		minValue = Math.floor(minValue / 5);
		minValue = minValue * 5;

		date.set('minute', minValue);
		date.set('second', 0);

		url = '/api/lol/euw/v4.1/game/ids?' + 'beginDate='+  date.unix() + '&api_key='+ process.env.API_KEY;

	  request(
	    { method: 'GET'
	    , url: process.env.API_ROOT_URL + url,
	    json: true
	    }
	  , function (error, response, body) {
	      // body is the decompressed response body 

	      if (error) {
					console.log(date.format("dddd, MMMM Do YYYY, h:mm:ss a"), date.unix(),  url, 
				  									 "error: " + error.message);
	      	return; 
	      }
	      console.log(response.statusCode);

				if (response.statusCode === 200) {
						console.log('#add match ids')

						var data  = [];
						for (var i = body.length - 1; i >= 0; i--) {
							data.push( {matchId: body[i]});
						};

						db.matchId.insert(data,function (e, results) {
							if (e) {

								console.log('#add match ids error', e.message);

							};
								console.log('#add match ids ', results);

						})
			  };


				console.log(date.format("dddd, MMMM Do YYYY, h:mm:ss a"), date.unix(),  url, 
			  									 "Got response: " + response.statusCode);
	    }
	  )

  },
  start: false,
  timeZone: 'Europe/Berlin'
});

jobPullMatchIds.start();


jobPullMatchData = new CronJob({
  cronTime: '*/1 * * * *',
  onTick: function() {
		console.log('get matchs ', moment().format("dddd, MMMM Do YYYY, h:mm:ss a"))

		db.matchId.find().limit( 30 ).toArray(function(err, items) {
			var url;

			//  should be run in parallel

			url = '/api/lol/euw/v2.2/match/';
			var processmatch = function (currentMatchId, callback) {
				  request(
				    { method: 'GET',
				    	json: true,
				      url: process.env.API_ROOT_URL + url + currentMatchId +  '?api_key='+ process.env.API_KEY + '&includeTimeline=true'
				    }
				  , function (error, response, body) {
				  		console.log( process.env.API_ROOT_URL + url + '?api_key='+ process.env.API_KEY + '&includeTimeline=true');

				      if (error) {
								callback(error);
				      	return; 
				      }


							if (response.statusCode === 200) {
									db.matchs.update({matchId: body.matchId},body, {upsert: true},function(e, results){

								    if (e){
											console.log(moment().format("dddd, MMMM Do YYYY, h:mm:ss a"), moment().unix(),  url, 
										  									 "error: " + e.message);
								    	return;
								    }

								    db.matchId.remove({matchId: currentMatchId}, { w: 0 });

								    callback();
								  })
						  } else if (! (response.statusCode === 429 )) {
						  	// match is not reachable 
						  	console.log(response.statusCode);
								db.matchId.remove({matchId: currentMatchId}, { w: 0 });
								callback();
						  }else {
								console.log(moment().format("dddd, MMMM Do YYYY, h:mm:ss a"), moment().unix(),  url, 
							  									 "Got response: " + response.statusCode);

						  	callback();
						  }

				    }
				  )
			};

			async.eachLimit(items, 5, processmatch, function(err){
				console.log(moment().format("dddd, MMMM Do YYYY, h:mm:ss a"), moment().unix(),  url, 
									 "error: " + err.message);
			});

			

		});

  }, 
  start: false,
  timeZone: 'Europe/Berlin'
});

jobPullMatchData.start();
