package org.apache.rocketmq.namesrv.processor;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MQVersion.Version;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.namesrv.NamesrvUtil;
import org.apache.rocketmq.namesrv.NameSrvController;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.*;
import org.apache.rocketmq.remoting.protocol.body.*;
import org.apache.rocketmq.remoting.protocol.header.GetBrokerMemberGroupRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetTopicsByClusterRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.*;
import org.apache.rocketmq.remoting.protocol.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 请求处理器
 */
@Slf4j
public class DefaultRequestProcessor implements NettyRequestProcessor {

    protected final NameSrvController namesrvController;

    /**
     * 配置黑名单
     */
    protected Set<String> configBlackList = new HashSet<>();

    public DefaultRequestProcessor(NameSrvController namesrvController) {
        this.namesrvController = namesrvController;
        initConfigBlackList();
    }

    private void initConfigBlackList() {
        configBlackList.add("configBlackList");
        configBlackList.add("configStorePath");
        configBlackList.add("kvConfigPath");
        configBlackList.add("rocketmqHome");
        String[] configArray = namesrvController.getNamesrvConfig().getConfigBlackList().split(";");
        configBlackList.addAll(Arrays.asList(configArray));
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        if (ctx != null) {
            log.debug("receive request, {} {} {}", request.getCode(), RemotingHelper.parseChannelRemoteAddr(ctx.channel()), request);
        }
        switch (request.getCode()) {
            //存储键值对
            case RequestCode.PUT_KV_CONFIG:
                return this.putKVConfig(ctx, request);
            //获取键值对
            case RequestCode.GET_KV_CONFIG:
                return this.getKVConfig(ctx, request);
            //删除键值对
            case RequestCode.DELETE_KV_CONFIG:
                return this.deleteKVConfig(ctx, request);
            //查询数据版本
            case RequestCode.QUERY_DATA_VERSION:
                return this.queryBrokerTopicConfig(ctx, request);
            //注册broker
            case RequestCode.REGISTER_BROKER:
                return this.registerBroker(ctx, request);
            //注销broker
            case RequestCode.UNREGISTER_BROKER:
                return this.unregisterBroker(ctx, request);
            //broker心跳检测
            case RequestCode.BROKER_HEARTBEAT:
                return this.brokerHeartbeat(ctx, request);
            //获取broker组信息
            case RequestCode.GET_BROKER_MEMBER_GROUP:
                return this.getBrokerMemberGroup(ctx, request);
            //获取broker集群信息
            case RequestCode.GET_BROKER_CLUSTER_INFO:
                return this.getBrokerClusterInfo(ctx, request);
            case RequestCode.WIPE_WRITE_PERM_OF_BROKER:
                return this.wipeWritePermOfBroker(ctx, request);
            case RequestCode.ADD_WRITE_PERM_OF_BROKER:
                return this.addWritePermOfBroker(ctx, request);
            //从namespace获取所有主题列表
            case RequestCode.GET_ALL_TOPIC_LIST_FROM_NAMESERVER:
                return this.getAllTopicListFromNameserver(ctx, request);
            //删除namespace下的主题
            case RequestCode.DELETE_TOPIC_IN_NAMESRV:
                return this.deleteTopicInNamesrv(ctx, request);
            //往namespace注册topic
            case RequestCode.REGISTER_TOPIC_IN_NAMESRV:
                return this.registerTopicToNamesrv(ctx, request);
            //从namespace获取kv列表
            case RequestCode.GET_KVLIST_BY_NAMESPACE:
                return this.getKVListByNamespace(ctx, request);
            //从集群中获取主题列表
            case RequestCode.GET_TOPICS_BY_CLUSTER:
                return this.getTopicsByCluster(ctx, request);
            //从namespace获取系统主题
            case RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_NS:
                return this.getSystemTopicListFromNs(ctx, request);
            case RequestCode.GET_UNIT_TOPIC_LIST:
                return this.getUnitTopicList(ctx, request);
            case RequestCode.GET_HAS_UNIT_SUB_TOPIC_LIST:
                return this.getHasUnitSubTopicList(ctx, request);
            case RequestCode.GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST:
                return this.getHasUnitSubUnUnitTopicList(ctx, request);
            case RequestCode.UPDATE_NAMESRV_CONFIG:
                return this.updateConfig(ctx, request);
            case RequestCode.GET_NAMESRV_CONFIG:
                return this.getConfig(ctx, request);
            default:
                String error = " request type " + request.getCode() + " not supported";
                return RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, error);
        }
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    /**
     * 存储键值对
     */
    public RemotingCommand putKVConfig(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final PutKVConfigRequestHeader requestHeader = request.decodeCommandCustomHeader(PutKVConfigRequestHeader.class);
        if (requestHeader.getNamespace() == null || requestHeader.getKey() == null) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("namespace or key is null");
            return response;
        }
        this.namesrvController.getKvConfigManager().putKVConfig(requestHeader.getNamespace(), requestHeader.getKey(), requestHeader.getValue());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 获取键值对
     */
    public RemotingCommand getKVConfig(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(GetKVConfigResponseHeader.class);
        final GetKVConfigResponseHeader responseHeader = (GetKVConfigResponseHeader) response.readCustomHeader();
        final GetKVConfigRequestHeader requestHeader = request.decodeCommandCustomHeader(GetKVConfigRequestHeader.class);
        String value = this.namesrvController.getKvConfigManager().getKVConfig(requestHeader.getNamespace(), requestHeader.getKey());
        if (value != null) {
            responseHeader.setValue(value);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }
        response.setCode(ResponseCode.QUERY_NOT_FOUND);
        response.setRemark("No config item, Namespace: " + requestHeader.getNamespace() + " Key: " + requestHeader.getKey());
        return response;
    }

    /**
     * 删除键值对
     */
    public RemotingCommand deleteKVConfig(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final DeleteKVConfigRequestHeader requestHeader = request.decodeCommandCustomHeader(DeleteKVConfigRequestHeader.class);
        this.namesrvController.getKvConfigManager().deleteKVConfig(requestHeader.getNamespace(), requestHeader.getKey());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 注册broker
     */
    public RemotingCommand registerBroker(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(RegisterBrokerResponseHeader.class);
        final RegisterBrokerResponseHeader responseHeader = (RegisterBrokerResponseHeader) response.readCustomHeader();
        final RegisterBrokerRequestHeader requestHeader = request.decodeCommandCustomHeader(RegisterBrokerRequestHeader.class);
        if (!checksum(ctx, request, requestHeader)) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("crc32 not match");
            return response;
        }
        TopicConfigSerializeWrapper topicConfigWrapper;
        List<String> filterServerList = null;
        Version brokerVersion = MQVersion.value2Version(request.getVersion());
        if (brokerVersion.ordinal() >= MQVersion.Version.V3_0_11.ordinal()) {
            final RegisterBrokerBody registerBrokerBody = extractRegisterBrokerBodyFromRequest(request, requestHeader);
            topicConfigWrapper = registerBrokerBody.getTopicConfigSerializeWrapper();
            filterServerList = registerBrokerBody.getFilterServerList();
        } else {
            topicConfigWrapper = extractRegisterTopicConfigFromRequest(request);
        }
        RegisterBrokerResult result = this.namesrvController.getRouteInfoManager().registerBroker(requestHeader.getClusterName(), requestHeader.getBrokerAddr(), requestHeader.getBrokerName(), requestHeader.getBrokerId(), requestHeader.getHaServerAddr(), request.getExtFields().get(MixAll.ZONE_NAME), requestHeader.getHeartbeatTimeoutMillis(), requestHeader.getEnableActingMaster(), topicConfigWrapper, filterServerList, ctx.channel());
        if (result == null) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("register broker failed");
            return response;
        }
        responseHeader.setHaServerAddr(result.getHaServerAddr());
        responseHeader.setMasterAddr(result.getMasterAddr());
        if (this.namesrvController.getNamesrvConfig().isReturnOrderTopicConfigToBroker()) {
            byte[] jsonValue = this.namesrvController.getKvConfigManager().getKVListByNamespace(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG);
            response.setBody(jsonValue);
        }
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private TopicConfigSerializeWrapper extractRegisterTopicConfigFromRequest(final RemotingCommand request) {
        TopicConfigSerializeWrapper topicConfigWrapper;
        if (request.getBody() != null) {
            topicConfigWrapper = TopicConfigSerializeWrapper.decode(request.getBody(), TopicConfigSerializeWrapper.class);
        } else {
            topicConfigWrapper = new TopicConfigSerializeWrapper();
            topicConfigWrapper.getDataVersion().setCounter(new AtomicLong(0));
            topicConfigWrapper.getDataVersion().setTimestamp(0L);
            topicConfigWrapper.getDataVersion().setStateVersion(0L);
        }
        return topicConfigWrapper;
    }

    private RegisterBrokerBody extractRegisterBrokerBodyFromRequest(RemotingCommand request,
        RegisterBrokerRequestHeader requestHeader) throws RemotingCommandException {
        RegisterBrokerBody registerBrokerBody = new RegisterBrokerBody();

        if (request.getBody() != null) {
            try {
                Version brokerVersion = MQVersion.value2Version(request.getVersion());
                registerBrokerBody = RegisterBrokerBody.decode(request.getBody(), requestHeader.isCompressed(), brokerVersion);
            } catch (Exception e) {
                throw new RemotingCommandException("Failed to decode RegisterBrokerBody", e);
            }
        } else {
            registerBrokerBody.getTopicConfigSerializeWrapper().getDataVersion().setCounter(new AtomicLong(0));
            registerBrokerBody.getTopicConfigSerializeWrapper().getDataVersion().setTimestamp(0L);
            registerBrokerBody.getTopicConfigSerializeWrapper().getDataVersion().setStateVersion(0L);
        }
        return registerBrokerBody;
    }

    /**
     * 获取broker组信息
     */
    private RemotingCommand getBrokerMemberGroup(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        GetBrokerMemberGroupRequestHeader requestHeader = request.decodeCommandCustomHeader(GetBrokerMemberGroupRequestHeader.class);
        BrokerMemberGroup memberGroup = this.namesrvController.getRouteInfoManager().getBrokerMemberGroup(requestHeader.getClusterName(), requestHeader.getBrokerName());
        GetBrokerMemberGroupResponseBody responseBody = new GetBrokerMemberGroupResponseBody();
        responseBody.setBrokerMemberGroup(memberGroup);
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(ResponseCode.SUCCESS);
        response.setBody(responseBody.encode());
        return response;
    }

    private boolean checksum(ChannelHandlerContext ctx, RemotingCommand request,
        RegisterBrokerRequestHeader requestHeader) {
        if (requestHeader.getBodyCrc32() != 0) {
            final int crc32 = UtilAll.crc32(request.getBody());
            if (crc32 != requestHeader.getBodyCrc32()) {
                log.warn("receive registerBroker request,crc32 not match,from {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
                return false;
            }
        }
        return true;
    }

    /**
     * 查询数据版本
     */
    public RemotingCommand queryBrokerTopicConfig(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(QueryDataVersionResponseHeader.class);
        final QueryDataVersionResponseHeader responseHeader = (QueryDataVersionResponseHeader) response.readCustomHeader();
        final QueryDataVersionRequestHeader requestHeader = request.decodeCommandCustomHeader(QueryDataVersionRequestHeader.class);
        DataVersion dataVersion = DataVersion.decode(request.getBody(), DataVersion.class);
        String clusterName = requestHeader.getClusterName();
        String brokerAddr = requestHeader.getBrokerAddr();
        Boolean changed = this.namesrvController.getRouteInfoManager().isBrokerTopicConfigChanged(clusterName, brokerAddr, dataVersion);
        this.namesrvController.getRouteInfoManager().updateBrokerInfoUpdateTimestamp(clusterName, brokerAddr);
        DataVersion nameSeverDataVersion = this.namesrvController.getRouteInfoManager().queryBrokerTopicConfig(clusterName, brokerAddr);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        if (nameSeverDataVersion != null) {
            response.setBody(nameSeverDataVersion.encode());
        }
        responseHeader.setChanged(changed);
        return response;
    }

    /**
     * 注销broker
     */
    public RemotingCommand unregisterBroker(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final UnRegisterBrokerRequestHeader requestHeader = request.decodeCommandCustomHeader(UnRegisterBrokerRequestHeader.class);
        if (!this.namesrvController.getRouteInfoManager().submitUnRegisterBrokerRequest(requestHeader)) {
            log.warn("Couldn't submit the unregister broker request to handler, broker info: {}", requestHeader);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(null);
            return response;
        }
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * broker心跳检测
     */
    public RemotingCommand brokerHeartbeat(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final BrokerHeartbeatRequestHeader requestHeader = request.decodeCommandCustomHeader(BrokerHeartbeatRequestHeader.class);
        this.namesrvController.getRouteInfoManager().updateBrokerInfoUpdateTimestamp(requestHeader.getClusterName(), requestHeader.getBrokerAddr());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 获取所有集群信息
     */
    private RemotingCommand getBrokerClusterInfo(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        byte[] content = this.namesrvController.getRouteInfoManager().getAllClusterInfo().encode();
        response.setBody(content);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand wipeWritePermOfBroker(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(WipeWritePermOfBrokerResponseHeader.class);
        final WipeWritePermOfBrokerResponseHeader responseHeader = (WipeWritePermOfBrokerResponseHeader) response.readCustomHeader();
        final WipeWritePermOfBrokerRequestHeader requestHeader = request.decodeCommandCustomHeader(WipeWritePermOfBrokerRequestHeader.class);
        int wipeTopicCnt = this.namesrvController.getRouteInfoManager().wipeWritePermOfBrokerByLock(requestHeader.getBrokerName());
        if (ctx != null) {
            log.info("wipe write perm of broker[{}], client: {}, {}", requestHeader.getBrokerName(), RemotingHelper.parseChannelRemoteAddr(ctx.channel()), wipeTopicCnt);
        }
        responseHeader.setWipeTopicCount(wipeTopicCnt);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand addWritePermOfBroker(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(AddWritePermOfBrokerResponseHeader.class);
        final AddWritePermOfBrokerResponseHeader responseHeader = (AddWritePermOfBrokerResponseHeader) response.readCustomHeader();
        final AddWritePermOfBrokerRequestHeader requestHeader = request.decodeCommandCustomHeader(AddWritePermOfBrokerRequestHeader.class);
        int addTopicCnt = this.namesrvController.getRouteInfoManager().addWritePermOfBrokerByLock(requestHeader.getBrokerName());
        log.info("add write perm of broker[{}], client: {}, {}", requestHeader.getBrokerName(), RemotingHelper.parseChannelRemoteAddr(ctx.channel()), addTopicCnt);
        responseHeader.setAddTopicCount(addTopicCnt);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 从namespace获取所有主题列表
     */
    private RemotingCommand getAllTopicListFromNameserver(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        boolean enableAllTopicList = namesrvController.getNamesrvConfig().isEnableAllTopicList();
        log.warn("getAllTopicListFromNameserver {} enable {}", ctx.channel().remoteAddress(), enableAllTopicList);
        if (enableAllTopicList) {
            byte[] body = this.namesrvController.getRouteInfoManager().getAllTopicList().encode();
            response.setBody(body);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("disable");
        }
        return response;
    }

    private RemotingCommand registerTopicToNamesrv(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final RegisterTopicRequestHeader requestHeader = request.decodeCommandCustomHeader(RegisterTopicRequestHeader.class);
        TopicRouteData topicRouteData = TopicRouteData.decode(request.getBody(), TopicRouteData.class);
        if (topicRouteData != null && topicRouteData.getQueueDatas() != null && !topicRouteData.getQueueDatas().isEmpty()) {
            this.namesrvController.getRouteInfoManager().registerTopic(requestHeader.getTopic(), topicRouteData.getQueueDatas());
        }
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 删除namespace下的主题
     */
    private RemotingCommand deleteTopicInNamesrv(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final DeleteTopicFromNamesrvRequestHeader requestHeader = request.decodeCommandCustomHeader(DeleteTopicFromNamesrvRequestHeader.class);
        if (requestHeader.getClusterName() != null && !requestHeader.getClusterName().isEmpty()) {
            this.namesrvController.getRouteInfoManager().deleteTopic(requestHeader.getTopic(), requestHeader.getClusterName());
        } else {
            this.namesrvController.getRouteInfoManager().deleteTopic(requestHeader.getTopic());
        }
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 从namespace获取kv列表
     */
    private RemotingCommand getKVListByNamespace(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetKVListByNamespaceRequestHeader requestHeader = request.decodeCommandCustomHeader(GetKVListByNamespaceRequestHeader.class);
        byte[] jsonValue = this.namesrvController.getKvConfigManager().getKVListByNamespace(requestHeader.getNamespace());
        if (null != jsonValue) {
            response.setBody(jsonValue);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }
        response.setCode(ResponseCode.QUERY_NOT_FOUND);
        response.setRemark("No config item, Namespace: " + requestHeader.getNamespace());
        return response;
    }

    /**
     * 从集群中获取主题列表
     */
    private RemotingCommand getTopicsByCluster(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        boolean enableTopicList = namesrvController.getNamesrvConfig().isEnableTopicList();
        if (!enableTopicList) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("disable");
            return response;
        }
        final GetTopicsByClusterRequestHeader requestHeader = request.decodeCommandCustomHeader(GetTopicsByClusterRequestHeader.class);
        TopicList topicsByCluster = this.namesrvController.getRouteInfoManager().getTopicsByCluster(requestHeader.getCluster());
        byte[] body = topicsByCluster.encode();
        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getSystemTopicListFromNs(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        TopicList systemTopicList = this.namesrvController.getRouteInfoManager().getSystemTopicList();
        byte[] body = systemTopicList.encode();
        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getUnitTopicList(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        boolean enableTopicList = namesrvController.getNamesrvConfig().isEnableTopicList();
        if (enableTopicList) {
            TopicList unitTopicList = this.namesrvController.getRouteInfoManager().getUnitTopics();
            byte[] body = unitTopicList.encode();
            response.setBody(body);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("disable");
        }
        return response;
    }

    private RemotingCommand getHasUnitSubTopicList(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        boolean enableTopicList = namesrvController.getNamesrvConfig().isEnableTopicList();
        if (enableTopicList) {
            TopicList hasUnitSubTopicList = this.namesrvController.getRouteInfoManager().getHasUnitSubTopicList();
            byte[] body = hasUnitSubTopicList.encode();
            response.setBody(body);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("disable");
        }
        return response;
    }

    private RemotingCommand getHasUnitSubUnUnitTopicList(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        boolean enableTopicList = namesrvController.getNamesrvConfig().isEnableTopicList();
        if (enableTopicList) {
            TopicList hasUnitSubUnUnitTopicList = this.namesrvController.getRouteInfoManager().getHasUnitSubUnUnitTopicList();
            byte[] body = hasUnitSubUnUnitTopicList.encode();
            response.setBody(body);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("disable");
        }
        return response;
    }

    private RemotingCommand updateConfig(ChannelHandlerContext ctx, RemotingCommand request) {
        if (ctx != null) {
            log.info("updateConfig called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
        }
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        byte[] body = request.getBody();
        if (body != null) {
            String bodyStr;
            try {
                bodyStr = new String(body, MixAll.DEFAULT_CHARSET);
            } catch (UnsupportedEncodingException e) {
                log.error("updateConfig byte array to string error: ", e);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
            Properties properties = MixAll.string2Properties(bodyStr);
            if (properties == null) {
                log.error("updateConfig MixAll.string2Properties error {}", bodyStr);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("string2Properties error");
                return response;
            }
            if (validateBlackListConfigExist(properties)) {
                response.setCode(ResponseCode.NO_PERMISSION);
                response.setRemark("Can not update config in black list.");
                return response;
            }
            this.namesrvController.getConfiguration().update(properties);
        }
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getConfig(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        String content = this.namesrvController.getConfiguration().getAllConfigsFormatString();
        if (StringUtils.isNotBlank(content)) {
            try {
                response.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                log.error("getConfig error, ", e);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        }
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private boolean validateBlackListConfigExist(Properties properties) {
        for (String blackConfig : configBlackList) {
            if (properties.containsKey(blackConfig)) {
                return true;
            }
        }
        return false;
    }

}
