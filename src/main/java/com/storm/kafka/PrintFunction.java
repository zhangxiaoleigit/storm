package com.storm.kafka;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhangxiaolei05 on 2016/4/18.
 */
public class PrintFunction extends BaseFunction {
    List<String> list = new ArrayList<String>();
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        System.out.println(tridentTuple.getString(0));
        list.add(tridentTuple.getString(0));
    }

    @Override
    public void cleanup() {
        for (String str : list){
            System.out.println(str);
        }
    }
}
