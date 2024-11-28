package org.apache.rocketmq.remoting;

import org.apache.rocketmq.remoting.pipeline.RequestPipeline;

public interface RemotingService {

    void start();

    void shutdown();

    void registerRPCHook(RPCHook rpcHook);

    void setRequestPipeline(RequestPipeline pipeline);

    void clearRPCHook();

}
