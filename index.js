
//Database
var MongoClient = require('mongodb').MongoClient;
var mongoUrl = 'mongodb://localhost:27017/';
//Mqtt
const mqtt = require ('mqtt');
var client  = mqtt.connect('mqtt://192.168.43.254',{ port: 1883 });
//Utility
var datetime = require('node-datetime');

////////////////MAGIC_STARTS//////////////////
//subscription
client.on('connect', function () {
  console.log('Client has subscribed successfully');
  client.subscribe('esp8266-out/#');
  client.subscribe('esp8266-in/#');

});

//Listens mqtt
client.on('message', function (topic, message){
  var subject=topic.split("/");
  if (subject[2]==="data") {
    var recObj=JSON.parse(message);
    recObj.chipId=parseInt(recObj.chipId).toString(16);
    deviceExist(recObj.chipId,function(result){
      if (result==false) {
        var deviceObj=cloneObj(recObj);
        delete deviceObj.humidity;
        delete deviceObj.temperature;
        delete deviceObj.heatIndex;
        delete deviceObj.moisture;
        insertDevice(deviceObj);
      }
    });
    var measureObj=cloneObj(recObj);
    delete measureObj.type;
    measureObj.datetime=datetime.create(Date.now());
    insertMeasurement(measureObj);
  }
  else if (subject[2]==="relay") {
    var recObj=JSON.parse(message);
    recObj.chipId=parseInt(recObj.chipId).toString(16);
    deviceExist(recObj.chipId,function(result){
      if (result==false) {
        var deviceObj=cloneObj(recObj);
        //relay konusuna gelen mesajların data'sı yok, silinecek bir şey yok
        insertDevice(deviceObj);
      }
    });
    insertLastActive(recObj);
  }
});

function deviceExist(deviceId,callback){
  MongoClient.connect(mongoUrl,{ useNewUrlParser: true },function(err,db){
    var dbo=db.db("SmartAgr");
    dbo.collection("Devices").find({chipId:deviceId}).toArray(function(err,result){
      if (err) throw err;
      if (result.length==0) {
        callback(false);
      }
      else{
        callback(true);
      }
      db.close;
    });
  });
};

function insertDevice(deviceObj){
  MongoClient.connect(mongoUrl,{ useNewUrlParser: true },function(err,db){
    var currentDateTime=datetime.create(Date.now());
    deviceObj.lastActive=currentDateTime;
    var dbo=db.db("SmartAgr");
    dbo.collection("Devices").insertOne(deviceObj, function(err,result){
      if (err){
        console.log("Can't insert device");
      }
      else{
        console.log("Device inserted",result.ops[0]);
      }
    });
  });
};

function insertMeasurement(measureObj){
  MongoClient.connect(mongoUrl,{ useNewUrlParser: true },function(err,db){
    var dbo=db.db("SmartAgr");
    dbo.collection("Measurements").insertOne(measureObj, function(err,result){
      if (err){
        console.log("Can't insert measurement");
      }
      else{
        console.log("Measurement inserted",result.ops[0]);
      }
    });
  });
  insertLastActive(measureObj);
};

function insertLastActive(deviceObj){//RELAYSİZ NODELARDA BU FONKSİYON İÇİN PROBLEM VAR MI KONTROL ET
  var deviceChipId=deviceObj.chipId;
  MongoClient.connect(mongoUrl,{ useNewUrlParser: true }, function(err,db){
    var dbo=db.db("SmartAgr");
    var deviceToUpdate={chipId:deviceChipId};
    var currentDateTime=datetime.create(Date.now());
    if (deviceObj.relayState!=null) {
      var updatedValue={$set:{lastActive:currentDateTime, relayState:deviceObj.relayState}};
    }
    else{
      var updatedValue={$set:{lastActive:currentDateTime}};
    }
    dbo.collection("Devices").updateOne(deviceToUpdate,updatedValue,function(err,result){
      if (err) {
        console.log("Can't update last active");
      }
      else{
        console.log(deviceChipId+": Last active updated");
      }
    });
  });
}

function cloneObj(a) {
   return JSON.parse(JSON.stringify(a));
}

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
}


var debug=true;
if (debug) {
  setRelayState("3c93e4",false,function(result){
    if (result) {
      console.log("relayState mqtt'de değiştirildi");
    }
    else{
      console.log("başarısız.");
    }
  });
}
