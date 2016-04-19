package com.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhangxiaolei05 on 2016/4/11.
 */
public class CountWordBolt extends BaseRichBolt {
    private OutputCollector collector = null;
    private Map<String,Long> counts = null;
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.counts = new HashMap<String,Long>();
    }

    public void execute(Tuple tuple) {
        String word = tuple.getString(0);
        Long count = counts.get(word);
        if (count == null){
            count = 0L;
        }
        count ++;
        counts.put(word,count);
        this.collector.emit(new Values(word,count));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word","count"));
    }
}
