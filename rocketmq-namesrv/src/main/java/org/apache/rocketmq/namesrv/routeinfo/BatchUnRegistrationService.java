package org.apache.rocketmq.namesrv.routeinfo;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.header.namesrv.UnRegisterBrokerRequestHeader;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * broker注销服务
 */
public class BatchUnRegistrationService extends ServiceThread {

    private final RouteInfoManager routeInfoManager;

    private final BlockingQueue<UnRegisterBrokerRequestHeader> unRegistrationQueue;

    private static final Logger log = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    public BatchUnRegistrationService(RouteInfoManager routeInfoManager, NamesrvConfig namesrvConfig) {
        this.routeInfoManager = routeInfoManager;
        this.unRegistrationQueue = new LinkedBlockingQueue<>(namesrvConfig.getUnRegisterBrokerQueueCapacity());
    }

    /**
     * 提交注销broker请求到队列
     */
    public boolean submit(UnRegisterBrokerRequestHeader unRegisterRequest) {
        return unRegistrationQueue.offer(unRegisterRequest);
    }

    /**
     * 服务名称
     */
    @Override
    public String getServiceName() {
        return BatchUnRegistrationService.class.getName();
    }

    @Override
    public void run() {
        while (!this.isStopped()) {
            try {
                final UnRegisterBrokerRequestHeader request = unRegistrationQueue.take();
                Set<UnRegisterBrokerRequestHeader> unRegistrationRequests = new HashSet<>();
                unRegistrationQueue.drainTo(unRegistrationRequests);
                unRegistrationRequests.add(request);
                this.routeInfoManager.unRegisterBroker(unRegistrationRequests);
            } catch (Throwable e) {
                log.error("Handle unregister broker request failed", e);
            }
        }
    }

}
