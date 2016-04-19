package com.storm.trident;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.spout.ITridentSpout;

import java.io.Serializable;

/**
 * Created by zhangxiaolei05 on 2016/4/12.
 */
public class DefaultCoordinator implements ITridentSpout.BatchCoordinator<Long>,Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultCoordinator.class);
    public Long initializeTransaction(long l, Long aLong, Long x1) {
        LOG.info("Initializing Transaction [" + l + "]");
        return null;
    }

    public void success(long l) {
        LOG.info("Successful Transaction [" + l + "]");
    }

    public boolean isReady(long l) {
        return true;
    }

    public void close() {

    }
}
