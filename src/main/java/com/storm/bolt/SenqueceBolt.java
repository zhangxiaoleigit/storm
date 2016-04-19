package com.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by zhangxiaolei05 on 2016/4/18.
 */
public class SenqueceBolt extends BaseRichBolt {


    OutputCollector collector = null;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
this.collector = outputCollector;
    }

    public void execute(Tuple tuple) {
        // TODO Auto-generated method stub
        String word = (String) tuple.getValue(0);
        String out = "I'm " + word +  "!";
        System.out.println("out=" + out);
        collector.emit(new Values(out));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
