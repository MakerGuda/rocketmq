package org.apache.rocketmq.namesrv.processor;

import com.alibaba.fastjson.serializer.SerializerFeature;
import io.netty.channel.ChannelHandlerContext;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.namesrv.NamesrvUtil;
import org.apache.rocketmq.namesrv.NameSrvController;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.namesrv.GetRouteInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 主要功能： 获取主题下的路由数据
 */
@Slf4j
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ClientRequestProcessor implements NettyRequestProcessor {

    protected NameSrvController namesrvController;

    /**
     * 启动时间
     */
    private long startupTimeMillis;

    private AtomicBoolean needCheckNameSrvReady = new AtomicBoolean(true);

    public ClientRequestProcessor(final NameSrvController namesrvController) {
        this.namesrvController = namesrvController;
        this.startupTimeMillis = System.currentTimeMillis();
    }

    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx, final RemotingCommand request) throws Exception {
        return this.getRouteInfoByTopic(ctx, request);
    }

    /**
     * 获取topic下的路由信息
     */
    public RemotingCommand getRouteInfoByTopic(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetRouteInfoRequestHeader requestHeader = request.decodeCommandCustomHeader(GetRouteInfoRequestHeader.class);
        boolean nameSrvReady = needCheckNameSrvReady.get() && System.currentTimeMillis() - startupTimeMillis >= TimeUnit.SECONDS.toMillis(namesrvController.getNamesrvConfig().getWaitSecondsForService());
        //判断nameSrv是否准备就绪
        if (namesrvController.getNamesrvConfig().isNeedWaitForService() && !nameSrvReady) {
            log.warn("name server not ready. request code {} ", request.getCode());
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("name server not ready");
            return response;
        }
        TopicRouteData topicRouteData = this.namesrvController.getRouteInfoManager().pickupTopicRouteData(requestHeader.getTopic());
        if (topicRouteData != null) {
            //主题路由信息获取成功，禁用掉nameSrv就绪检查
            if (needCheckNameSrvReady.get()) {
                needCheckNameSrvReady.set(false);
            }
            if (this.namesrvController.getNamesrvConfig().isOrderMessageEnable()) {
                //顺序消息配置
                String orderTopicConf = this.namesrvController.getKvConfigManager().getKVConfig(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG, requestHeader.getTopic());
                topicRouteData.setOrderTopicConf(orderTopicConf);
            }
            byte[] content;
            Boolean standardJsonOnly = Optional.ofNullable(requestHeader.getAcceptStandardJsonOnly()).orElse(false);
            if (request.getVersion() >= MQVersion.Version.V4_9_4.ordinal() || standardJsonOnly) {
                content = topicRouteData.encode(SerializerFeature.BrowserCompatible, SerializerFeature.QuoteFieldNames, SerializerFeature.SkipTransientField, SerializerFeature.MapSortField);
            } else {
                content = topicRouteData.encode();
            }
            response.setBody(content);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }
        response.setCode(ResponseCode.TOPIC_NOT_EXIST);
        response.setRemark("No topic route info in name server for the topic: " + requestHeader.getTopic() + FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL));
        return response;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

}
