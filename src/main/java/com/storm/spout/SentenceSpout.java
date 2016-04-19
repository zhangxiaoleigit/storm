package com.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by zhangxiaolei05 on 2016/4/8.
 */
public class SentenceSpout extends BaseRichSpout {
    private SpoutOutputCollector collector = null;
    private int index = 0;
    String[] sentences = new String[] {
            "storm integrates with the queueing",
            "and database technologies you already use",
            "a storm topology consumes streams of data",
            "and processes those streams in arbitrarily complex ways",
            "repartitioning the streams between each stage of the computation however needed"
    };
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    public void nextTuple() {
        this.collector.emit(new Values(sentences[index]));
        index++;
        if (index >= sentences.length) {
            index = 0;
        }

        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
        }
    }
}
