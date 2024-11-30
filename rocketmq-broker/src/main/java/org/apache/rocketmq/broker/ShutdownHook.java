package org.apache.rocketmq.broker;

public interface ShutdownHook {

    /**
     * broker关闭之前执行的代码
     */
    void beforeShutdown(BrokerController controller);

}
