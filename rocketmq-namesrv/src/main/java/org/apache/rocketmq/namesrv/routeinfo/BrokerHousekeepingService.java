package org.apache.rocketmq.namesrv.routeinfo;

import io.netty.channel.Channel;
import org.apache.rocketmq.namesrv.NameSrvController;
import org.apache.rocketmq.remoting.ChannelEventListener;

public class BrokerHousekeepingService implements ChannelEventListener {

    private final NameSrvController namesrvController;

    public BrokerHousekeepingService(NameSrvController namesrvController) {
        this.namesrvController = namesrvController;
    }

    @Override
    public void onChannelConnect(String remoteAddr, Channel channel) {
    }

    @Override
    public void onChannelClose(String remoteAddr, Channel channel) {
        this.namesrvController.getRouteInfoManager().onChannelDestroy(channel);
    }

    @Override
    public void onChannelException(String remoteAddr, Channel channel) {
        this.namesrvController.getRouteInfoManager().onChannelDestroy(channel);
    }

    @Override
    public void onChannelIdle(String remoteAddr, Channel channel) {
        this.namesrvController.getRouteInfoManager().onChannelDestroy(channel);
    }

    @Override
    public void onChannelActive(String remoteAddr, Channel channel) {

    }

}
