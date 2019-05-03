var MongoClient = require('mongodb').MongoClient;
var mongoUrl = 'mongodb://localhost:27017/';

//Mqtt
const mqtt = require ('mqtt');
var client  = mqtt.connect('mqtt://192.168.43.254',{ port: 1883 });

var moment = require('moment');
moment.locale('tr');

var triggers;
var rules= new Object();
var activeTriggers= new Object();

function getTriggers(callback){
  MongoClient.connect(mongoUrl,{ useNewUrlParser: true },function(err,db){
    var dbo=db.db("SmartAgr");
    dbo.collection("Triggers").find({active:true}).toArray(function(err,result){
      if (err) throw err;
      if (result.length==0) {
        callback(false);
        console.log("No active triggers");
      }
      else{
        triggers=result;
        callback(true);
      }
      db.close;
    });
  });
};

function getLastestMeasurement(chipId, sourceType, callback){
  MongoClient.connect(mongoUrl,{ useNewUrlParser: true },function(err,db){
    var dbo=db.db("SmartAgr");
    dbo.collection("Measurements").find({chipId:chipId}).limit(1).sort({$natural:-1}).toArray(function(err,result){
      if (err) throw err;
      if (result.length==0) {
        callback(null);
      }
      else{
        if (sourceType!=null) {
          if (sourceType.temperature) {
            callback(result[0].temperature);
          }
          else if (sourceType.humidity) {
            callback(result[0].humidity);
          }
          else if (sourceType.moisture) {
            callback(result[0].moisture);
          }
        }
      }
      db.close;
    });
  });
};

function setRelayState(device,relayState,callback){//OBJE GONDEREREK ÇALIŞTIRMA DENENMEDİ
  if (typeof device === 'object' && device!=null) {
    client.publish('esp8266-in/'+device.chipId+'/', (relayState===true) ? '1' : '0');
    callback(true);
  }
  else if(typeof device==='string'){
    client.publish('esp8266-in/'+device+'/', (relayState===true) ? '1' : '0');
    callback(true);
  }
  else{
    callback(false);
  }
};

function main(){
  getTriggers(function(callback){
    if (callback) {
      for (var i = 0; i < triggers.length; i++) {
        getLastestMeasurement(triggers[i].sourceChipId,triggers[i].sourceType,function(measurement){
          console.log(measurement);
          if ((activeTriggers[triggers[this.i]._id]==null || activeTriggers[triggers[this.i]._id]==false) && triggers[this.i].fromVal >= measurement) {
            setRelayState(triggers[this.i].actionChipId,true,function(callback){
              if (callback) {
                console.log(triggers[this.i].actionChipId+" relay state: 1");
              }
            }.bind({i:this.i}));
            activeTriggers[triggers[this.i]._id]=true;
          }
          else if(activeTriggers[triggers[this.i]._id]!=null && activeTriggers[triggers[this.i]._id]==true && triggers[this.i].toVal<=measurement){
            setRelayState(triggers[this.i].actionChipId,false,function(callback){
              if (callback) {
                console.log(triggers[this.i].actionChipId+" relay state: 0");
              }
            }.bind({i:this.i}));
            activeTriggers[triggers[this.i]._id]=false;
          }
          console.log(triggers[this.i]);
          console.log(activeTriggers);
        }.bind({i:i}));
      }
    }
  });
};

setInterval(main, 2000);
// getLastestMeasurement("b7951f", null, function(callback){
//   console.log(callback);
// });
