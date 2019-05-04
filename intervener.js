var MongoClient = require('mongodb').MongoClient;
var mongoUrl = 'mongodb://localhost:27017/';

//Mqtt
const mqtt = require ('mqtt');
var client  = mqtt.connect('mqtt://192.168.43.254',{ port: 1883 });

var moment = require('moment');
moment.locale('tr');

var triggers;
var schedules;
var rules = new Object();
var activeTriggers = new Object();
var activeSchedules= new Object();
var scheduleRunningOn = new Object();

function getRules(callback){
  MongoClient.connect(mongoUrl,{ useNewUrlParser: true },function(err,db){
    var dbo=db.db("SmartAgr");
    dbo.collection("Triggers").find({active:true,isRule:true}).toArray(function(err,result){
      if (err) throw err;
        for (var i = 0; i < result.length; i++) {
          rules[result[i].actionChipId]=result[i];
        }
        callback(true);
      db.close;
    });
  });
};

function getTriggers(callback){
  MongoClient.connect(mongoUrl,{ useNewUrlParser: true },function(err,db){
    var dbo=db.db("SmartAgr");
    dbo.collection("Triggers").find({active:true,isRule:false}).toArray(function(err,result){
      if (err) throw err;
      if (result.length==0) {
        callback(false);
        console.log("No active trigger");
      }
      else{
        triggers=result;
        callback(true);
      }
      db.close;
    });
  });
};

function getSchedules(callback){
  MongoClient.connect(mongoUrl,{ useNewUrlParser: true },function(err,db){
    var dbo=db.db("SmartAgr");
    dbo.collection("Schedules").find({active:true}).toArray(function(err,result){
      if (err) throw err;
      if (result.length==0) {
        callback(false);
        console.log("No active schedule");
      }
      else{
        schedules=result;
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
    console.log("FAILED TO SET RELAY STATE: "+device);
    callback(false);
  }
};


function main(){
  getRules(function(callback){
    getTriggers(function(callback){//fromVal > toVal durumu eklenmedi.
      if (callback) {
        for (var i = 0; i < triggers.length; i++) {
          getLastestMeasurement(triggers[i].sourceChipId,triggers[i].sourceType,function(measurement){
            console.log("ölçüm:"+measurement," fromVal:"+ triggers[this.i].fromVal);
            if ((scheduleRunningOn[triggers[this.i].actionChipId]==null || scheduleRunningOn[triggers[this.i].actionChipId]==false) && (activeTriggers[triggers[this.i]._id]==null || activeTriggers[triggers[this.i]._id]==false) && triggers[this.i].fromVal >= measurement) {
              setRelayState(triggers[this.i].actionChipId,true,function(callback){
                if (callback) {
                  activeTriggers[triggers[this.i]._id]=true;
                  console.log(triggers[this.i].actionChipId+" relay state: 1 by trigger");
                }
              }.bind({i:this.i}));
            }
            else if((scheduleRunningOn[triggers[this.i].actionChipId]==null || scheduleRunningOn[triggers[this.i].actionChipId]==false) && activeTriggers[triggers[this.i]._id]!=null && activeTriggers[triggers[this.i]._id]==true && triggers[this.i].toVal<=measurement){
              setRelayState(triggers[this.i].actionChipId,false,function(callback){
                if (callback) {
                  activeTriggers[triggers[this.i]._id]=false;
                  console.log(triggers[this.i].actionChipId+" relay state: 0 by trigger");
                }
              }.bind({i:this.i}));
            }
            if (scheduleRunningOn[triggers[this.i].actionChipId]==true && activeTriggers[triggers[this.i]._id]==true) {
              activeTriggers[triggers[this.i]._id]=false;
            }
          }.bind({i:i}));
        }
      }
    });
    getSchedules(function(callback){
      if (callback) {
        for (var i = 0; i < schedules.length; i++) {
          if (schedules[i].repeatable) {
            var todayDayId=moment().day();
            if ((todayDayId==1 && schedules[i].occurOn.monday) || (todayDayId==2 && schedules[i].occurOn.tuesday) || todayDayId==3 && schedules[i].occurOn.wednesday || (todayDayId==4 && schedules[i].occurOn.thursday) || (todayDayId==5 && schedules[i].occurOn.friday) || (todayDayId==6 && schedules[i].occurOn.saturday) || (todayDayId==7 && schedules[i].occurOn.sunday)) {

              var rNow=moment();
              var startTime=moment(schedules[i].from);
              startTime.date(rNow.date());
              startTime.month(rNow.month());
              startTime.year(rNow.year());
              var endTime=moment(schedules[i].to);
              endTime.date(rNow.date());
              endTime.month(rNow.month());
              endTime.year(rNow.year());
              if ((activeSchedules[schedules[i]._id]==null || activeSchedules[schedules[i]._id]==false) && (startTime.isBefore(rNow) && rNow.isBefore(endTime))) { //(startTime.isBefore(rNow) && rNow.isBefore(endTime))
                if (rules[schedules[i].chipId]!=null) {
                  getLastestMeasurement(rules[schedules[i].chipId].sourceChipId,rules[schedules[i].chipId].sourceType,function(measurement){
                    if (measurement>rules[schedules[this.i].chipId].fromVal && measurement<rules[schedules[this.i].chipId].toVal) {
                      setRelayState(schedules[this.i].chipId,true,function(callback){
                        if (callback) {
                          activeSchedules[schedules[this.i]._id]=true;
                          scheduleRunningOn[schedules[this.i].chipId]=true;
                          console.log(schedules[this.i].chipId+" relay state: 1 by schedule: "+schedules[this.i]._id);
                        }
                      }.bind({i:this.i}));
                    }
                  }.bind({i:i}));
                }
                else{
                  setRelayState(schedules[i].chipId,true,function(callback){
                    if (callback) {
                      activeSchedules[schedules[this.i]._id]=true;
                      scheduleRunningOn[schedules[this.i].chipId]=true;
                      console.log(schedules[this.i].chipId+" relay state: 1 by schedule: "+schedules[this.i]._id);
                    }
                  }.bind({i:i}));
                }
              }
              else if(activeSchedules[schedules[i]._id]!=null && activeSchedules[schedules[i]._id]==true && endTime.isBefore(rNow)){
                setRelayState(schedules[i].chipId,false,function(callback){
                  if (callback) {
                    activeSchedules[schedules[this.i]._id]=false;
                    scheduleRunningOn[schedules[this.i].chipId]=false;
                    console.log(schedules[this.i].chipId+" relay state: 0 by schedule: "+schedules[this.i]._id);
                  }
                }.bind({i:i}));
              }
              if (activeSchedules[schedules[i]._id]==true) {
                getLastestMeasurement(rules[schedules[i].chipId].sourceChipId,rules[schedules[i].chipId].sourceType,function(measurement){
                  if (measurement<rules[schedules[this.i].chipId].fromVal || measurement>rules[schedules[this.i].chipId].toVal) {
                    setRelayState(schedules[this.i].chipId,false,function(callback){
                      if (callback) {
                        activeSchedules[schedules[this.i]._id]=false;
                        scheduleRunningOn[schedules[this.i].chipId]=false;
                        console.log(schedules[this.i].chipId+" relay state: 0 by schedule because of rule");
                      }
                    }.bind({i:this.i}));
                  }
                }.bind({i:i}));
              }
              if (activeSchedules[schedules[i]._id]==true && scheduleRunningOn[schedules[i].chipId]==false) {
                scheduleRunningOn[schedules[i].chipId]=true;
              }
            }
          }
          else{//REPEATED CODE - FIX IT LATER
            var rNow=moment();
            var startTime=moment(schedules[i].from);
            var endTime=moment(schedules[i].to);
            if ((activeSchedules[schedules[i]._id]==null || activeSchedules[schedules[i]._id]==false) && (startTime.isBefore(rNow) && rNow.isBefore(endTime))) {
              if (rules[schedules[i].chipId]!=null) {
                getLastestMeasurement(rules[schedules[i].chipId].sourceChipId,rules[schedules[i].chipId].sourceType,function(measurement){
                  if (measurement>rules[schedules[this.i].chipId].fromVal && measurement<rules[schedules[this.i].chipId].toVal) {
                    setRelayState(schedules[this.i].chipId,true,function(callback){
                      if (callback) {
                        activeSchedules[schedules[this.i]._id]=true;
                        scheduleRunningOn[schedules[this.i].chipId]=true;
                        console.log(schedules[this.i].chipId+" relay state: 1 by schedule: "+schedules[this.i]._id);
                      }
                    }.bind({i:this.i}));
                  }
                }.bind({i:i}));
              }
              else{
                setRelayState(schedules[i].chipId,true,function(callback){
                  if (callback) {
                    activeSchedules[schedules[this.i]._id]=true;
                    scheduleRunningOn[schedules[this.i].chipId]=true;
                    console.log(schedules[this.i].chipId+" relay state: 1 by schedule: "+schedules[this.i]._id);
                  }
                }.bind({i:i}));
              }
            }
            else if(activeSchedules[schedules[i]._id]!=null && activeSchedules[schedules[i]._id]==true && endTime.isBefore(rNow)){
              setRelayState(schedules[i].chipId,false,function(callback){
                if (callback) {
                  activeSchedules[schedules[this.i]._id]=false;
                  scheduleRunningOn[schedules[this.i].chipId]=false;
                  console.log(schedules[this.i].chipId+" relay state: 0 by schedule: "+schedules[this.i]._id);
                }
              }.bind({i:i}));
            }
            if (activeSchedules[schedules[i]._id]==true) {
              getLastestMeasurement(rules[schedules[i].chipId].sourceChipId,rules[schedules[i].chipId].sourceType,function(measurement){
                if (measurement<rules[schedules[this.i].chipId].fromVal || measurement>rules[schedules[this.i].chipId].toVal) {
                  setRelayState(schedules[this.i].chipId,false,function(callback){
                    if (callback) {
                      activeSchedules[schedules[this.i]._id]=false;
                      scheduleRunningOn[schedules[this.i].chipId]=false;
                      console.log(schedules[this.i].chipId+" relay state: 0 by schedule because of rule");
                    }
                  }.bind({i:this.i}));
                }
              }.bind({i:i}));
            }


            if (activeSchedules[schedules[i]._id]==true && scheduleRunningOn[schedules[i].chipId]==false) {
              scheduleRunningOn[schedules[i].chipId]=true;
            }
          }
        }
      }
    });
  });

};


setInterval(main, 2000);
