package com.storm.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhangxiaolei05 on 2016/4/14.
 */
public class Test {
    private static final Logger logger = LoggerFactory.getLogger(Test.class);

    public static void main(String[] args) throws Exception {
        for (int i = 0;i< 100;i++){
            logger.warn("this is waring state !");
            //Thread.sleep(5000);
        }
    }
}
