package com.storm.bolt;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.storm.spout.SentenceSpout;

/**
 * Created by zhangxiaolei05 on 2016/4/11.
 */
public class WordCountTopology {
    private static final String SENTENCE_SPOUT_ID = "sentence-spout";
    private static final String SPLIT_BOLT_ID = "split-bolt";
    private static final String COUNT_BOLT_ID = "conut-bolt";
    private static final String REPORT_BOLT_ID = "report-bolt";
    private static final String TOPOLOGY_NAME = "word-count-topology";
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        SentenceSpout spout = new SentenceSpout();
        SplitBolt spiltBolt = new SplitBolt();
        CountWordBolt countBolt = new CountWordBolt();
        ReportBolt reportBolt = new ReportBolt();
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SENTENCE_SPOUT_ID, spout); //注册数据源
        builder.setBolt(SPLIT_BOLT_ID, spiltBolt) //注册bolt
                .shuffleGrouping(SENTENCE_SPOUT_ID); //该bolt订阅spout随机均匀发射来的数据流
        builder.setBolt(COUNT_BOLT_ID, countBolt)
                .fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
        builder.setBolt(REPORT_BOLT_ID, reportBolt)
                .globalGrouping(COUNT_BOLT_ID);
        Config config = new Config();
        if (args.length == 0){
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
            try {
                Thread.sleep(10 * 1000);
            } catch (InterruptedException e) {
            }
            cluster.killTopology(TOPOLOGY_NAME);
            cluster.shutdown();
        }else {
            StormSubmitter.submitTopology(TOPOLOGY_NAME,config,builder.createTopology());
        }
    }
}
