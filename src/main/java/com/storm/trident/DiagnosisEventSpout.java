package com.storm.trident;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import storm.trident.spout.ITridentSpout;

import java.util.Map;

/**
 * Created by zhangxiaolei05 on 2016/4/12.
 */
public class DiagnosisEventSpout implements ITridentSpout {
    private BatchCoordinator batchCoordinator = new DefaultCoordinator();
    Emitter<Long> emitter = new DiagnosisEventEmitter();
    public BatchCoordinator getCoordinator(String s, Map map, TopologyContext topologyContext) {
        return batchCoordinator;
    }

    public Emitter getEmitter(String s, Map map, TopologyContext topologyContext) {
        return emitter;
    }

    public Map getComponentConfiguration() {
        return null;
    }

    public Fields getOutputFields() {
        return new Fields("event");
    }
}
