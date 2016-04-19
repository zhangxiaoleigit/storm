package com.storm.bolt;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import storm.kafka.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zhangxiaolei05 on 2016/4/18.
 */
public class KafkaTopology {
    private static final String TOPOLOGY_NAME = "word-count-topology";
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        BrokerHosts brokerHosts =  new ZkHosts("10.95.40.21:8181");
        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, "baidu", "" , "test");
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConf.forceFromStart = true;
        spoutConf.zkServers = Arrays.asList(new String[] {"10.95.40.21"});

        spoutConf.zkPort = 8181;

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-reader", new KafkaSpout(spoutConf));
        builder.setBolt("word-splitter", new KafkaWordSplitter()).shuffleGrouping("kafka-reader");
        builder.setBolt("word-counter", new WordCounter()).fieldsGrouping("word-splitter", new Fields("word"));


        Config config = new Config();
        if (args != null && args.length > 0){
            StormSubmitter.submitTopology(TOPOLOGY_NAME,config, builder.createTopology());
        }else {
            LocalCluster cluster = new LocalCluster();
            config.setDebug(true);
            cluster.submitTopology(TOPOLOGY_NAME, config,  builder.createTopology());
            try {
                Utils.sleep(30 * 1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
            cluster.killTopology(TOPOLOGY_NAME);
            cluster.shutdown();
        }
    }
    public static class KafkaWordSplitter extends BaseRichBolt {
        private static final Log LOG = LogFactory.getLog(KafkaWordSplitter.class);
        private static final long serialVersionUID = 886149197481637894L;
        private OutputCollector collector;
        public void prepare(Map stormConf, TopologyContext context,
                            OutputCollector collector) {
            this.collector = collector;
        }
        public void execute(Tuple input) {
            String line = input.getString(0);
            System.out.println(line);
            String[] words = line.split(" ");
            for(String word : words) {
                collector.emit(new Values(word, 1));
            }
            collector.ack(input);
        }
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }
    public static class WordCounter extends BaseRichBolt {
        private static final Log LOG = LogFactory.getLog(WordCounter.class);
        private static final long serialVersionUID = 886149197481637894L;
        private OutputCollector collector;
        private Map<String, AtomicInteger> counterMap;
        public void prepare(Map stormConf, TopologyContext context,
                            OutputCollector collector) {
            this.collector = collector;
            this.counterMap = new HashMap<String, AtomicInteger>();
        }
        public void execute(Tuple input) {
            String word = input.getString(0);
            int count = input.getInteger(1);
            AtomicInteger ai = this.counterMap.get(word);
            if(ai == null) {
                ai = new AtomicInteger();
                this.counterMap.put(word, ai);
            }
            ai.addAndGet(count);
        }
        public void cleanup() {
            Iterator<Map.Entry<String, AtomicInteger>> iter = this.counterMap.entrySet().iterator();
            while(iter.hasNext()) {
                Map.Entry<String, AtomicInteger> entry = iter.next();
                LOG.info(entry.getKey() + "\t:\t" + entry.getValue().get());
            }

        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {

            //declarer.declare(new Fields("word", "count"));

        }
    }

}
