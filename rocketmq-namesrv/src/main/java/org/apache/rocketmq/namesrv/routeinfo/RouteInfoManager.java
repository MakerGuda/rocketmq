package org.apache.rocketmq.namesrv.routeinfo;

import com.google.common.collect.Sets;
import io.netty.channel.Channel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.ConcurrentHashMapUtils;
import org.apache.rocketmq.namesrv.NameSrvController;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.body.*;
import org.apache.rocketmq.remoting.protocol.header.NotifyMinBrokerIdChangeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.UnRegisterBrokerRequestHeader;
import org.apache.rocketmq.remoting.protocol.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
@Getter
@Setter
@AllArgsConstructor
public class RouteInfoManager {

    /**
     * 默认broker过期时间
     */
    private static final long DEFAULT_BROKER_CHANNEL_EXPIRED_TIME = 1000 * 60 * 2;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * key: topic  value:{ key: brokerName value: 当前broker上的队列数据}
     */
    private final Map<String, Map<String, QueueData>> topicQueueTable;

    /**
     * key: brokerName
     */
    private final Map<String, BrokerData> brokerAddrTable;

    /**
     * key: clusterName value: 集群下的所有broker名称
     */
    private final Map<String, Set<String>> clusterAddrTable;

    /**
     * broker地址和broker存活信息
     */
    private final Map<BrokerAddrInfo, BrokerLiveInfo> brokerLiveTable;

    /**
     * broker服务和其上的过滤服务
     */
    private final Map<BrokerAddrInfo, List<String>> filterServerTable;

    /**
     * key: 主题   value:{ key: brokerName value: 主题队列映射关系}
     */
    private final Map<String, Map<String, TopicQueueMappingInfo>> topicQueueMappingInfoTable;

    /**
     * 批量broker注销服务
     */
    private final BatchUnRegistrationService unRegisterService;

    private final NameSrvController namesrvController;

    private final NamesrvConfig namesrvConfig;

    public RouteInfoManager(final NamesrvConfig namesrvConfig, NameSrvController namesrvController) {
        this.topicQueueTable = new ConcurrentHashMap<>(1024);
        this.brokerAddrTable = new ConcurrentHashMap<>(128);
        this.clusterAddrTable = new ConcurrentHashMap<>(32);
        this.brokerLiveTable = new ConcurrentHashMap<>(256);
        this.filterServerTable = new ConcurrentHashMap<>(256);
        this.topicQueueMappingInfoTable = new ConcurrentHashMap<>(1024);
        this.unRegisterService = new BatchUnRegistrationService(this, namesrvConfig);
        this.namesrvConfig = namesrvConfig;
        this.namesrvController = namesrvController;
    }

    /**
     * 启动broker注销服务线程
     */
    public void start() {
        this.unRegisterService.start();
    }

    /**
     * 关闭
     */
    public void shutdown() {
        this.unRegisterService.shutdown(true);
    }

    /**
     * 提交broker注销请求
     */
    public boolean submitUnRegisterBrokerRequest(UnRegisterBrokerRequestHeader unRegisterRequest) {
        return this.unRegisterService.submit(unRegisterRequest);
    }

    public ClusterInfo getAllClusterInfo() {
        ClusterInfo clusterInfoSerializeWrapper = new ClusterInfo();
        clusterInfoSerializeWrapper.setBrokerAddrTable(this.brokerAddrTable);
        clusterInfoSerializeWrapper.setClusterAddrTable(this.clusterAddrTable);
        return clusterInfoSerializeWrapper;
    }

    /**
     * 注册topic
     */
    public void registerTopic(final String topic, List<QueueData> queueDatas) {
        if (queueDatas == null || queueDatas.isEmpty()) {
            return;
        }
        try {
            this.lock.writeLock().lockInterruptibly();
            //已存在的topic
            if (this.topicQueueTable.containsKey(topic)) {
                //获取历史的队列信息
                Map<String, QueueData> queueDataMap  = this.topicQueueTable.get(topic);
                for (QueueData queueData : queueDatas) {
                    //当前存在未注册的brokerName
                    if (!this.brokerAddrTable.containsKey(queueData.getBrokerName())) {
                        return;
                    }
                    queueDataMap.put(queueData.getBrokerName(), queueData);
                }
                log.info("Topic route already exist.{}, {}", topic, this.topicQueueTable.get(topic));
            } else {
                Map<String, QueueData> queueDataMap = new HashMap<>();
                for (QueueData queueData : queueDatas) {
                    if (!this.brokerAddrTable.containsKey(queueData.getBrokerName())) {
                        log.warn("Register topic contains illegal broker, {}, {}", topic, queueData);
                        return;
                    }
                    queueDataMap.put(queueData.getBrokerName(), queueData);
                }
                this.topicQueueTable.put(topic, queueDataMap);
                log.info("Register topic route:{}, {}", topic, queueDatas);
            }
        } catch (Exception e) {
            log.error("registerTopic Exception", e);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    /**
     * 删除topic
     */
    public void deleteTopic(final String topic) {
        try {
            this.lock.writeLock().lockInterruptibly();
            this.topicQueueTable.remove(topic);
        } catch (Exception e) {
            log.error("deleteTopic Exception", e);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    /**
     * 删除topic
     */
    public void deleteTopic(final String topic, final String clusterName) {
        try {
            this.lock.writeLock().lockInterruptibly();
            Set<String> brokerNames = this.clusterAddrTable.get(clusterName);
            if (brokerNames == null || brokerNames.isEmpty()) {
                return;
            }
            Map<String, QueueData> queueDataMap = this.topicQueueTable.get(topic);
            if (queueDataMap != null) {
                if (queueDataMap.isEmpty()) {
                    log.info("deleteTopic, remove the topic all queue {} {}", clusterName, topic);
                    this.topicQueueTable.remove(topic);
                }
            }
        } catch (Exception e) {
            log.error("deleteTopic Exception", e);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    /**
     * 获取主题列表
     */
    public TopicList getAllTopicList() {
        TopicList topicList = new TopicList();
        try {
            this.lock.readLock().lockInterruptibly();
            topicList.getTopicList().addAll(this.topicQueueTable.keySet());
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        } finally {
            this.lock.readLock().unlock();
        }
        return topicList;
    }

    /**
     * 注册broker
     *
     * @param clusterName        集群名称
     * @param brokerAddr         集群地址
     * @param brokerName         broker名称
     * @param brokerId           brokerId
     * @param haServerAddr       主从复制服务
     * @param zoneName           空间名称
     * @param timeoutMillis      超时时间
     * @param enableActingMaster 是否允许激活为master
     * @param topicConfigWrapper 主题配置包装器
     * @param filterServerList   过滤服务列表
     * @param channel            channel
     */
    public RegisterBrokerResult registerBroker(final String clusterName, final String brokerAddr, final String brokerName, final long brokerId, final String haServerAddr, final String zoneName, final Long timeoutMillis, final Boolean enableActingMaster, final TopicConfigSerializeWrapper topicConfigWrapper, final List<String> filterServerList, final Channel channel) {
        RegisterBrokerResult result = new RegisterBrokerResult();
        try {
            this.lock.writeLock().lockInterruptibly();
            Set<String> brokerNames = ConcurrentHashMapUtils.computeIfAbsent((ConcurrentHashMap<String, Set<String>>) this.clusterAddrTable, clusterName, k -> new HashSet<>());
            assert brokerNames != null;
            brokerNames.add(brokerName);
            boolean registerFirst = false;
            BrokerData brokerData = this.brokerAddrTable.get(brokerName);
            if (null == brokerData) {
                registerFirst = true;
                brokerData = new BrokerData(clusterName, brokerName, new HashMap<>());
                this.brokerAddrTable.put(brokerName, brokerData);
            }
            boolean isOldVersionBroker = enableActingMaster == null;
            brokerData.setEnableActingMaster(!isOldVersionBroker && enableActingMaster);
            brokerData.setZoneName(zoneName);
            Map<Long, String> brokerAddrsMap = brokerData.getBrokerAddrs();
            boolean isMinBrokerIdChanged = false;
            long prevMinBrokerId = 0;
            if (!brokerAddrsMap.isEmpty()) {
                prevMinBrokerId = Collections.min(brokerAddrsMap.keySet());
            }
            if (brokerId < prevMinBrokerId) {
                isMinBrokerIdChanged = true;
            }
            brokerAddrsMap.entrySet().removeIf(item -> null != brokerAddr && brokerAddr.equals(item.getValue()) && brokerId != item.getKey());
            String oldBrokerAddr = brokerAddrsMap.get(brokerId);
            if (null != oldBrokerAddr && !oldBrokerAddr.equals(brokerAddr)) {
                BrokerLiveInfo oldBrokerInfo = brokerLiveTable.get(new BrokerAddrInfo(clusterName, oldBrokerAddr));
                if (null != oldBrokerInfo) {
                    long oldStateVersion = oldBrokerInfo.getDataVersion().getStateVersion();
                    long newStateVersion = topicConfigWrapper.getDataVersion().getStateVersion();
                    if (oldStateVersion > newStateVersion) {
                        log.warn("Registering Broker conflicts with the existed one, just ignore.: Cluster:{}, BrokerName:{}, BrokerId:{}, " + "Old BrokerAddr:{}, Old Version:{}, New BrokerAddr:{}, New Version:{}.", clusterName, brokerName, brokerId, oldBrokerAddr, oldStateVersion, brokerAddr, newStateVersion);
                        brokerLiveTable.remove(new BrokerAddrInfo(clusterName, brokerAddr));
                        return result;
                    }
                }
            }
            if (!brokerAddrsMap.containsKey(brokerId) && topicConfigWrapper.getTopicConfigTable().size() == 1) {
                log.warn("Can't register topicConfigWrapper={} because broker[{}]={} has not registered.", topicConfigWrapper.getTopicConfigTable(), brokerId, brokerAddr);
                return null;
            }
            String oldAddr = brokerAddrsMap.put(brokerId, brokerAddr);
            registerFirst = registerFirst || (StringUtils.isEmpty(oldAddr));
            boolean isMaster = MixAll.MASTER_ID == brokerId;
            boolean isPrimeSlave = !isOldVersionBroker && !isMaster && brokerId == Collections.min(brokerAddrsMap.keySet());
            if (null != topicConfigWrapper && (isMaster || isPrimeSlave)) {
                ConcurrentMap<String, TopicConfig> tcTable = topicConfigWrapper.getTopicConfigTable();
                if (tcTable != null) {
                    TopicConfigAndMappingSerializeWrapper mappingSerializeWrapper = TopicConfigAndMappingSerializeWrapper.from(topicConfigWrapper);
                    Map<String, TopicQueueMappingInfo> topicQueueMappingInfoMap = mappingSerializeWrapper.getTopicQueueMappingInfoMap();
                    if (namesrvConfig.isDeleteTopicWithBrokerRegistration() && topicQueueMappingInfoMap.isEmpty()) {
                        final Set<String> oldTopicSet = topicSetOfBrokerName(brokerName);
                        final Set<String> newTopicSet = tcTable.keySet();
                        final Sets.SetView<String> toDeleteTopics = Sets.difference(oldTopicSet, newTopicSet);
                        for (final String toDeleteTopic : toDeleteTopics) {
                            Map<String, QueueData> queueDataMap = topicQueueTable.get(toDeleteTopic);
                            final QueueData removedQD = queueDataMap.remove(brokerName);
                            if (removedQD != null) {
                                log.info("deleteTopic, remove one broker's topic {} {} {}", brokerName, toDeleteTopic, removedQD);
                            }
                            if (queueDataMap.isEmpty()) {
                                log.info("deleteTopic, remove the topic all queue {}", toDeleteTopic);
                                topicQueueTable.remove(toDeleteTopic);
                            }
                        }
                    }
                    for (Map.Entry<String, TopicConfig> entry : tcTable.entrySet()) {
                        if (registerFirst || this.isTopicConfigChanged(clusterName, brokerAddr,
                            topicConfigWrapper.getDataVersion(), brokerName,
                            entry.getValue().getTopicName())) {
                            final TopicConfig topicConfig = entry.getValue();
                            if (isPrimeSlave && brokerData.isEnableActingMaster()) {
                                topicConfig.setPerm(topicConfig.getPerm() & (~PermName.PERM_WRITE));
                            }
                            this.createAndUpdateQueueData(brokerName, topicConfig);
                        }
                    }
                    if (this.isBrokerTopicConfigChanged(clusterName, brokerAddr, topicConfigWrapper.getDataVersion()) || registerFirst) {
                        for (Map.Entry<String, TopicQueueMappingInfo> entry : topicQueueMappingInfoMap.entrySet()) {
                            if (!topicQueueMappingInfoTable.containsKey(entry.getKey())) {
                                topicQueueMappingInfoTable.put(entry.getKey(), new HashMap<>());
                            }
                            topicQueueMappingInfoTable.get(entry.getKey()).put(entry.getValue().getBname(), entry.getValue());
                        }
                    }
                }
            }

            BrokerAddrInfo brokerAddrInfo = new BrokerAddrInfo(clusterName, brokerAddr);
            BrokerLiveInfo prevBrokerLiveInfo = this.brokerLiveTable.put(brokerAddrInfo, new BrokerLiveInfo(System.currentTimeMillis(), timeoutMillis == null ? DEFAULT_BROKER_CHANNEL_EXPIRED_TIME : timeoutMillis, topicConfigWrapper == null ? new DataVersion() : topicConfigWrapper.getDataVersion(), channel, haServerAddr));
            if (null == prevBrokerLiveInfo) {
                log.info("new broker registered, {} HAService: {}", brokerAddrInfo, haServerAddr);
            }

            if (filterServerList != null) {
                if (filterServerList.isEmpty()) {
                    this.filterServerTable.remove(brokerAddrInfo);
                } else {
                    this.filterServerTable.put(brokerAddrInfo, filterServerList);
                }
            }

            if (MixAll.MASTER_ID != brokerId) {
                String masterAddr = brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
                if (masterAddr != null) {
                    BrokerAddrInfo masterAddrInfo = new BrokerAddrInfo(clusterName, masterAddr);
                    BrokerLiveInfo masterLiveInfo = this.brokerLiveTable.get(masterAddrInfo);
                    if (masterLiveInfo != null) {
                        result.setHaServerAddr(masterLiveInfo.getHaServerAddr());
                        result.setMasterAddr(masterAddr);
                    }
                }
            }

            if (isMinBrokerIdChanged && namesrvConfig.isNotifyMinBrokerIdChanged()) {
                notifyMinBrokerIdChanged(brokerAddrsMap, null, this.brokerLiveTable.get(brokerAddrInfo).getHaServerAddr());
            }
        } catch (Exception e) {
            log.error("registerBroker Exception", e);
        } finally {
            this.lock.writeLock().unlock();
        }
        return result;
    }

    /**
     * 获取当前brokerName上的所有topic列表
     */
    private Set<String> topicSetOfBrokerName(final String brokerName) {
        Set<String> topicOfBroker = new HashSet<>();
        for (final Entry<String, Map<String, QueueData>> entry : this.topicQueueTable.entrySet()) {
            if (entry.getValue().containsKey(brokerName)) {
                topicOfBroker.add(entry.getKey());
            }
        }
        return topicOfBroker;
    }

    /**
     * 获取broker组信息
     */
    public BrokerMemberGroup getBrokerMemberGroup(String clusterName, String brokerName) {
        BrokerMemberGroup groupMember = new BrokerMemberGroup(clusterName, brokerName);
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                final BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                if (brokerData != null) {
                    groupMember.getBrokerAddrs().putAll(brokerData.getBrokerAddrs());
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("Get broker member group exception", e);
        }
        return groupMember;
    }

    public boolean isBrokerTopicConfigChanged(final String clusterName, final String brokerAddr, final DataVersion dataVersion) {
        DataVersion prev = queryBrokerTopicConfig(clusterName, brokerAddr);
        return null == prev || !prev.equals(dataVersion);
    }

    public boolean isTopicConfigChanged(final String clusterName, final String brokerAddr, final DataVersion dataVersion, String brokerName, String topic) {
        boolean isChange = isBrokerTopicConfigChanged(clusterName, brokerAddr, dataVersion);
        if (isChange) {
            return true;
        }
        final Map<String, QueueData> queueDataMap = this.topicQueueTable.get(topic);
        if (queueDataMap == null || queueDataMap.isEmpty()) {
            return true;
        }
        return !queueDataMap.containsKey(brokerName);
    }

    public DataVersion queryBrokerTopicConfig(final String clusterName, final String brokerAddr) {
        BrokerAddrInfo addrInfo = new BrokerAddrInfo(clusterName, brokerAddr);
        BrokerLiveInfo prev = this.brokerLiveTable.get(addrInfo);
        if (prev != null) {
            return prev.getDataVersion();
        }
        return null;
    }

    /**
     * 更新broker的最新心跳时间
     */
    public void updateBrokerInfoUpdateTimestamp(final String clusterName, final String brokerAddr) {
        BrokerAddrInfo addrInfo = new BrokerAddrInfo(clusterName, brokerAddr);
        BrokerLiveInfo prev = this.brokerLiveTable.get(addrInfo);
        if (prev != null) {
            prev.setLastUpdateTimestamp(System.currentTimeMillis());
        }
    }

    /**
     * 创建并且更新queue
     */
    private void createAndUpdateQueueData(final String brokerName, final TopicConfig topicConfig) {
        QueueData queueData = new QueueData();
        queueData.setBrokerName(brokerName);
        queueData.setWriteQueueNums(topicConfig.getWriteQueueNums());
        queueData.setReadQueueNums(topicConfig.getReadQueueNums());
        queueData.setPerm(topicConfig.getPerm());
        queueData.setTopicSysFlag(topicConfig.getTopicSysFlag());
        Map<String, QueueData> queueDataMap = this.topicQueueTable.get(topicConfig.getTopicName());
        //当前topic下没有queue的情况，直接初始化进去
        if (null == queueDataMap) {
            queueDataMap = new HashMap<>();
            queueDataMap.put(brokerName, queueData);
            this.topicQueueTable.put(topicConfig.getTopicName(), queueDataMap);
            log.info("new topic registered, {} {}", topicConfig.getTopicName(), queueData);
        } else {
            final QueueData existedQD = queueDataMap.get(brokerName);
            if (existedQD == null) {
                queueDataMap.put(brokerName, queueData);
            } else if (!existedQD.equals(queueData)) {
                log.info("topic changed, {} OLD: {} NEW: {}", topicConfig.getTopicName(), existedQD, queueData);
                //更新queue
                queueDataMap.put(brokerName, queueData);
            }
        }
    }

    public int wipeWritePermOfBrokerByLock(final String brokerName) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                return operateWritePermOfBroker(brokerName, RequestCode.WIPE_WRITE_PERM_OF_BROKER);
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("wipeWritePermOfBrokerByLock Exception", e);
        }
        return 0;
    }

    public int addWritePermOfBrokerByLock(final String brokerName) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                return operateWritePermOfBroker(brokerName, RequestCode.ADD_WRITE_PERM_OF_BROKER);
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("addWritePermOfBrokerByLock Exception", e);
        }
        return 0;
    }

    private int operateWritePermOfBroker(final String brokerName, final int requestCode) {
        int topicCnt = 0;
        for (Entry<String, Map<String, QueueData>> entry : this.topicQueueTable.entrySet()) {
            Map<String, QueueData> qdMap = entry.getValue();
            final QueueData qd = qdMap.get(brokerName);
            if (qd == null) {
                continue;
            }
            int perm = qd.getPerm();
            switch (requestCode) {
                case RequestCode.WIPE_WRITE_PERM_OF_BROKER:
                    perm &= ~PermName.PERM_WRITE;
                    break;
                case RequestCode.ADD_WRITE_PERM_OF_BROKER:
                    perm = PermName.PERM_READ | PermName.PERM_WRITE;
                    break;
            }
            qd.setPerm(perm);
            topicCnt++;
        }
        return topicCnt;
    }

    /**
     * 注销broker
     */
    public void unregisterBroker(final String clusterName, final String brokerAddr, final String brokerName, final long brokerId) {
        UnRegisterBrokerRequestHeader unRegisterBrokerRequest = new UnRegisterBrokerRequestHeader();
        unRegisterBrokerRequest.setClusterName(clusterName);
        unRegisterBrokerRequest.setBrokerAddr(brokerAddr);
        unRegisterBrokerRequest.setBrokerName(brokerName);
        unRegisterBrokerRequest.setBrokerId(brokerId);
        unRegisterBroker(Sets.newHashSet(unRegisterBrokerRequest));
    }

    /**
     * 注销broker
     */
    public void unRegisterBroker(Set<UnRegisterBrokerRequestHeader> unRegisterRequests) {
        try {
            Set<String> removedBroker = new HashSet<>();
            Set<String> reducedBroker = new HashSet<>();
            Map<String, BrokerStatusChangeInfo> needNotifyBrokerMap = new HashMap<>();
            this.lock.writeLock().lockInterruptibly();
            for (final UnRegisterBrokerRequestHeader unRegisterRequest : unRegisterRequests) {
                final String brokerName = unRegisterRequest.getBrokerName();
                final String clusterName = unRegisterRequest.getClusterName();
                final String brokerAddr = unRegisterRequest.getBrokerAddr();
                BrokerAddrInfo brokerAddrInfo = new BrokerAddrInfo(clusterName, brokerAddr);
                //从broker信息存活表中移除当前broker信息
                BrokerLiveInfo brokerLiveInfo = this.brokerLiveTable.remove(brokerAddrInfo);
                log.info("unregisterBroker, remove from brokerLiveTable {}, {}", brokerLiveInfo != null ? "OK" : "Failed", brokerAddrInfo);
                this.filterServerTable.remove(brokerAddrInfo);

                boolean removeBrokerName = false;
                boolean isMinBrokerIdChanged = false;
                BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                //判断最小brokerId是否被改变
                if (null != brokerData) {
                    if (!brokerData.getBrokerAddrs().isEmpty() && unRegisterRequest.getBrokerId().equals(Collections.min(brokerData.getBrokerAddrs().keySet()))) {
                        isMinBrokerIdChanged = true;
                    }
                    boolean removed = brokerData.getBrokerAddrs().entrySet().removeIf(item -> item.getValue().equals(brokerAddr));
                    log.info("unregisterBroker, remove addr from brokerAddrTable {}, {}", removed ? "OK" : "Failed", brokerAddrInfo);
                    if (brokerData.getBrokerAddrs().isEmpty()) {
                        this.brokerAddrTable.remove(brokerName);
                        log.info("unregisterBroker, remove name from brokerAddrTable OK, {}", brokerName);
                        removeBrokerName = true;
                    } else if (isMinBrokerIdChanged) {
                        needNotifyBrokerMap.put(brokerName, new BrokerStatusChangeInfo(brokerData.getBrokerAddrs(), brokerAddr, null));
                    }
                }

                if (removeBrokerName) {
                    Set<String> nameSet = this.clusterAddrTable.get(clusterName);
                    if (nameSet != null) {
                        boolean removed = nameSet.remove(brokerName);
                        log.info("unregisterBroker, remove name from clusterAddrTable {}, {}", removed ? "OK" : "Failed", brokerName);
                        if (nameSet.isEmpty()) {
                            this.clusterAddrTable.remove(clusterName);
                            log.info("unregisterBroker, remove cluster from clusterAddrTable {}", clusterName);
                        }
                    }
                    removedBroker.add(brokerName);
                } else {
                    reducedBroker.add(brokerName);
                }
            }
            cleanTopicByUnRegisterRequests(removedBroker, reducedBroker);
            if (!needNotifyBrokerMap.isEmpty() && namesrvConfig.isNotifyMinBrokerIdChanged()) {
                notifyMinBrokerIdChanged(needNotifyBrokerMap);
            }
        } catch (Exception e) {
            log.error("unregisterBroker Exception", e);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    private void cleanTopicByUnRegisterRequests(Set<String> removedBroker, Set<String> reducedBroker) {
        Iterator<Entry<String, Map<String, QueueData>>> itMap = this.topicQueueTable.entrySet().iterator();
        while (itMap.hasNext()) {
            Entry<String, Map<String, QueueData>> entry = itMap.next();
            String topic = entry.getKey();
            Map<String, QueueData> queueDataMap = entry.getValue();
            for (final String brokerName : removedBroker) {
                final QueueData removedQD = queueDataMap.remove(brokerName);
                if (removedQD != null) {
                    log.debug("removeTopicByBrokerName, remove one broker's topic {} {}", topic, removedQD);
                }
            }
            if (queueDataMap.isEmpty()) {
                log.debug("removeTopicByBrokerName, remove the topic all queue {}", topic);
                itMap.remove();
            }
            for (final String brokerName : reducedBroker) {
                final QueueData queueData = queueDataMap.get(brokerName);
                if (queueData != null) {
                    if (this.brokerAddrTable.get(brokerName).isEnableActingMaster()) {
                        if (isNoMasterExists(brokerName)) {
                            queueData.setPerm(queueData.getPerm() & (~PermName.PERM_WRITE));
                        }
                    }
                }
            }
        }
    }

    /**
     * 判断master是否不存在
     */
    private boolean isNoMasterExists(String brokerName) {
        final BrokerData brokerData = this.brokerAddrTable.get(brokerName);
        if (brokerData == null) {
            return true;
        }
        if (brokerData.getBrokerAddrs().isEmpty()) {
            return true;
        }
        return Collections.min(brokerData.getBrokerAddrs().keySet()) > 0;
    }

    /**
     * 通过topic查询主题路由信息
     */
    public TopicRouteData pickupTopicRouteData(final String topic) {
        TopicRouteData topicRouteData = new TopicRouteData();
        boolean foundBrokerData = false;
        List<BrokerData> brokerDataList = new LinkedList<>();
        topicRouteData.setBrokerDatas(brokerDataList);
        HashMap<String, List<String>> filterServerMap = new HashMap<>();
        topicRouteData.setFilterServerTable(filterServerMap);
        try {
            this.lock.readLock().lockInterruptibly();
            Map<String, QueueData> queueDataMap = this.topicQueueTable.get(topic);
            if (queueDataMap != null) {
                topicRouteData.setQueueDatas(new ArrayList<>(queueDataMap.values()));
                Set<String> brokerNameSet = new HashSet<>(queueDataMap.keySet());
                for (String brokerName : brokerNameSet) {
                    BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                    if (null == brokerData) {
                        continue;
                    }
                    BrokerData brokerDataClone = new BrokerData(brokerData);
                    brokerDataList.add(brokerDataClone);
                    foundBrokerData = true;
                    if (filterServerTable.isEmpty()) {
                        continue;
                    }
                    for (final String brokerAddr : brokerDataClone.getBrokerAddrs().values()) {
                        BrokerAddrInfo brokerAddrInfo = new BrokerAddrInfo(brokerDataClone.getCluster(), brokerAddr);
                        List<String> filterServerList = this.filterServerTable.get(brokerAddrInfo);
                        filterServerMap.put(brokerAddr, filterServerList);
                    }
                }
            }
        } catch (Exception e) {
            log.error("pickupTopicRouteData Exception", e);
        } finally {
            this.lock.readLock().unlock();
        }
        log.debug("pickupTopicRouteData {} {}", topic, topicRouteData);

        if (foundBrokerData) {
            topicRouteData.setTopicQueueMappingByBroker(this.topicQueueMappingInfoTable.get(topic));
            if (!namesrvConfig.isSupportActingMaster()) {
                return topicRouteData;
            }
            if (topic.startsWith(TopicValidator.SYNC_BROKER_MEMBER_GROUP_PREFIX)) {
                return topicRouteData;
            }
            if (topicRouteData.getBrokerDatas().isEmpty() || topicRouteData.getQueueDatas().isEmpty()) {
                return topicRouteData;
            }

            //是否需要激活master broker
            boolean needActingMaster = false;
            for (final BrokerData brokerData : topicRouteData.getBrokerDatas()) {
                if (!brokerData.getBrokerAddrs().isEmpty() && !brokerData.getBrokerAddrs().containsKey(MixAll.MASTER_ID)) {
                    needActingMaster = true;
                    break;
                }
            }
            if (!needActingMaster) {
                return topicRouteData;
            }
            for (final BrokerData brokerData : topicRouteData.getBrokerDatas()) {
                final HashMap<Long, String> brokerAddrs = brokerData.getBrokerAddrs();
                if (brokerAddrs.isEmpty() || brokerAddrs.containsKey(MixAll.MASTER_ID) || !brokerData.isEnableActingMaster()) {
                    continue;
                }
                //不存在master且需要激活的情况
                for (final QueueData queueData : topicRouteData.getQueueDatas()) {
                    if (queueData.getBrokerName().equals(brokerData.getBrokerName())) {
                        if (!PermName.isWriteable(queueData.getPerm())) {
                            //从当前broker列表中获取brokerId最小的
                            final Long minBrokerId = Collections.min(brokerAddrs.keySet());
                            final String actingMasterAddr = brokerAddrs.remove(minBrokerId);
                            brokerAddrs.put(MixAll.MASTER_ID, actingMasterAddr);
                        }
                        break;
                    }
                }
            }
            return topicRouteData;
        }
        return null;
    }

    /**
     * 扫描非活跃状态的broker
     */
    public void scanNotActiveBroker() {
        try {
            log.info("start scanNotActiveBroker");
            for (Entry<BrokerAddrInfo, BrokerLiveInfo> next : this.brokerLiveTable.entrySet()) {
                //获取当前注册表中broker的最后一次更新时间
                long last = next.getValue().getLastUpdateTimestamp();
                //心跳检测超时时间
                long timeoutMillis = next.getValue().getHeartbeatTimeoutMillis();
                if ((last + timeoutMillis) < System.currentTimeMillis()) {
                    //关闭与当前broker的channel
                    RemotingHelper.closeChannel(next.getValue().getChannel());
                    log.warn("The broker channel expired, {} {}ms", next.getKey(), timeoutMillis);
                    this.onChannelDestroy(next.getKey());
                }
            }
        } catch (Exception e) {
            log.error("scanNotActiveBroker exception", e);
        }
    }

    /**
     * broker channel销毁
     */
    public void onChannelDestroy(BrokerAddrInfo brokerAddrInfo) {
        UnRegisterBrokerRequestHeader unRegisterRequest = new UnRegisterBrokerRequestHeader();
        boolean needUnRegister = false;
        if (brokerAddrInfo != null) {
            try {
                try {
                    this.lock.readLock().lockInterruptibly();
                    needUnRegister = setupUnRegisterRequest(unRegisterRequest, brokerAddrInfo);
                } finally {
                    this.lock.readLock().unlock();
                }
            } catch (Exception e) {
                log.error("onChannelDestroy Exception", e);
            }
        }
        if (needUnRegister) {
            this.submitUnRegisterBrokerRequest(unRegisterRequest);
        }
    }

    public void onChannelDestroy(Channel channel) {
        UnRegisterBrokerRequestHeader unRegisterRequest = new UnRegisterBrokerRequestHeader();
        BrokerAddrInfo brokerAddrFound = null;
        boolean needUnRegister = false;
        if (channel != null) {
            try {
                try {
                    this.lock.readLock().lockInterruptibly();
                    for (Entry<BrokerAddrInfo, BrokerLiveInfo> entry : this.brokerLiveTable.entrySet()) {
                        if (entry.getValue().getChannel() == channel) {
                            brokerAddrFound = entry.getKey();
                            break;
                        }
                    }
                    if (brokerAddrFound != null) {
                        needUnRegister = setupUnRegisterRequest(unRegisterRequest, brokerAddrFound);
                    }
                } finally {
                    this.lock.readLock().unlock();
                }
            } catch (Exception e) {
                log.error("onChannelDestroy Exception", e);
            }
        }
        if (needUnRegister) {
            boolean result = this.submitUnRegisterBrokerRequest(unRegisterRequest);
            log.info("the broker's channel destroyed, submit the unregister request at once, " + "broker info: {}, submit result: {}", unRegisterRequest, result);
        }
    }

    /**
     * 设置注销broker请求
     */
    private boolean setupUnRegisterRequest(UnRegisterBrokerRequestHeader unRegisterRequest, BrokerAddrInfo brokerAddrInfo) {
        unRegisterRequest.setClusterName(brokerAddrInfo.getClusterName());
        unRegisterRequest.setBrokerAddr(brokerAddrInfo.getBrokerAddr());
        for (Entry<String, BrokerData> stringBrokerDataEntry : this.brokerAddrTable.entrySet()) {
            BrokerData brokerData = stringBrokerDataEntry.getValue();
            if (!brokerAddrInfo.getClusterName().equals(brokerData.getCluster())) {
                continue;
            }
            for (Entry<Long, String> entry : brokerData.getBrokerAddrs().entrySet()) {
                Long brokerId = entry.getKey();
                String brokerAddr = entry.getValue();
                if (brokerAddr.equals(brokerAddrInfo.getBrokerAddr())) {
                    unRegisterRequest.setBrokerName(brokerData.getBrokerName());
                    unRegisterRequest.setBrokerId(brokerId);
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 最小brokerId发生变化时通知
     */
    private void notifyMinBrokerIdChanged(Map<String, BrokerStatusChangeInfo> needNotifyBrokerMap) throws InterruptedException, RemotingConnectException, RemotingTimeoutException, RemotingSendRequestException, RemotingTooMuchRequestException {
        for (String brokerName : needNotifyBrokerMap.keySet()) {
            BrokerStatusChangeInfo brokerStatusChangeInfo = needNotifyBrokerMap.get(brokerName);
            BrokerData brokerData = brokerAddrTable.get(brokerName);
            if (brokerData != null && brokerData.isEnableActingMaster()) {
                notifyMinBrokerIdChanged(brokerStatusChangeInfo.getBrokerAddrs(), brokerStatusChangeInfo.getOfflineBrokerAddr(), brokerStatusChangeInfo.getHaBrokerAddr());
            }
        }
    }

    /**
     * 最小brokerId变更时通知
     */
    private void notifyMinBrokerIdChanged(Map<Long, String> brokerAddrMap, String offlineBrokerAddr, String haBrokerAddr) throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException, RemotingTooMuchRequestException, RemotingConnectException {
        if (brokerAddrMap == null || brokerAddrMap.isEmpty() || this.namesrvController == null) {
            return;
        }
        NotifyMinBrokerIdChangeRequestHeader requestHeader = new NotifyMinBrokerIdChangeRequestHeader();
        long minBrokerId = Collections.min(brokerAddrMap.keySet());
        requestHeader.setMinBrokerId(minBrokerId);
        requestHeader.setMinBrokerAddr(brokerAddrMap.get(minBrokerId));
        requestHeader.setOfflineBrokerAddr(offlineBrokerAddr);
        requestHeader.setHaBrokerAddr(haBrokerAddr);
        List<String> brokerAddrsNotify = chooseBrokerAddrsToNotify(brokerAddrMap, offlineBrokerAddr);
        log.info("min broker id changed to {}, notify {}, offline broker addr {}", minBrokerId, brokerAddrsNotify, offlineBrokerAddr);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.NOTIFY_MIN_BROKER_ID_CHANGE, requestHeader);
        for (String brokerAddr : brokerAddrsNotify) {
            this.namesrvController.getRemotingClient().invokeOneway(brokerAddr, request, 300);
        }
    }

    /**
     * 选择要通知的broker列表
     */
    private List<String> chooseBrokerAddrsToNotify(Map<Long, String> brokerAddrMap, String offlineBrokerAddr) {
        if (offlineBrokerAddr != null || brokerAddrMap.size() == 1) {
            return new ArrayList<>(brokerAddrMap.values());
        }
        long minBrokerId = Collections.min(brokerAddrMap.keySet());
        List<String> brokerAddrList = new ArrayList<>();
        for (Long brokerId : brokerAddrMap.keySet()) {
            if (brokerId != minBrokerId) {
                brokerAddrList.add(brokerAddrMap.get(brokerId));
            }
        }
        return brokerAddrList;
    }

    /**
     * 获取系统主题列表
     */
    public TopicList getSystemTopicList() {
        TopicList topicList = new TopicList();
        try {
            this.lock.readLock().lockInterruptibly();
            for (Map.Entry<String, Set<String>> entry : clusterAddrTable.entrySet()) {
                topicList.getTopicList().add(entry.getKey());
                topicList.getTopicList().addAll(entry.getValue());
            }
            if (!brokerAddrTable.isEmpty()) {
                for (String s : brokerAddrTable.keySet()) {
                    BrokerData bd = brokerAddrTable.get(s);
                    HashMap<Long, String> brokerAddrs = bd.getBrokerAddrs();
                    if (brokerAddrs != null && !brokerAddrs.isEmpty()) {
                        Iterator<Long> it2 = brokerAddrs.keySet().iterator();
                        topicList.setBrokerAddr(brokerAddrs.get(it2.next()));
                        break;
                    }
                }
            }
        } catch (Exception e) {
            log.error("getSystemTopicList Exception", e);
        } finally {
            this.lock.readLock().unlock();
        }
        return topicList;
    }

    /**
     * 通过集群名称获取主题列表
     */
    public TopicList getTopicsByCluster(String cluster) {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                Set<String> brokerNameSet = this.clusterAddrTable.get(cluster);
                for (String brokerName : brokerNameSet) {
                    for (Entry<String, Map<String, QueueData>> topicEntry : this.topicQueueTable.entrySet()) {
                        String topic = topicEntry.getKey();
                        Map<String, QueueData> queueDataMap = topicEntry.getValue();
                        final QueueData qd = queueDataMap.get(brokerName);
                        if (qd != null) {
                            topicList.getTopicList().add(topic);
                        }
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getTopicsByCluster Exception", e);
        }
        return topicList;
    }

    public TopicList getUnitTopics() {
        TopicList topicList = new TopicList();
        try {
            this.lock.readLock().lockInterruptibly();
            for (Entry<String, Map<String, QueueData>> topicEntry : this.topicQueueTable.entrySet()) {
                String topic = topicEntry.getKey();
                Map<String, QueueData> queueDatas = topicEntry.getValue();
                if (queueDatas != null && !queueDatas.isEmpty() && TopicSysFlag.hasUnitFlag(queueDatas.values().iterator().next().getTopicSysFlag())) {
                    topicList.getTopicList().add(topic);
                }
            }
        } catch (Exception e) {
            log.error("getUnitTopics Exception", e);
        } finally {
            this.lock.readLock().unlock();
        }
        return topicList;
    }

    public TopicList getHasUnitSubTopicList() {
        TopicList topicList = new TopicList();
        try {
            this.lock.readLock().lockInterruptibly();
            for (Entry<String, Map<String, QueueData>> topicEntry : this.topicQueueTable.entrySet()) {
                String topic = topicEntry.getKey();
                Map<String, QueueData> queueDatas = topicEntry.getValue();
                if (queueDatas != null && !queueDatas.isEmpty() && TopicSysFlag.hasUnitSubFlag(queueDatas.values().iterator().next().getTopicSysFlag())) {
                    topicList.getTopicList().add(topic);
                }
            }
        } catch (Exception e) {
            log.error("getHasUnitSubTopicList Exception", e);
        } finally {
            this.lock.readLock().unlock();
        }
        return topicList;
    }

    public TopicList getHasUnitSubUnUnitTopicList() {
        TopicList topicList = new TopicList();
        try {
            this.lock.readLock().lockInterruptibly();
            for (Entry<String, Map<String, QueueData>> topicEntry : this.topicQueueTable.entrySet()) {
                String topic = topicEntry.getKey();
                Map<String, QueueData> queueDatas = topicEntry.getValue();
                if (queueDatas != null && !queueDatas.isEmpty() && !TopicSysFlag.hasUnitFlag(queueDatas.values().iterator().next().getTopicSysFlag()) && TopicSysFlag.hasUnitSubFlag(queueDatas.values().iterator().next().getTopicSysFlag())) {
                    topicList.getTopicList().add(topic);
                }
            }
        } catch (Exception e) {
            log.error("getHasUnitSubUnUnitTopicList Exception", e);
        } finally {
            this.lock.readLock().unlock();
        }
        return topicList;
    }

}

/**
 * broker地址信息
 */
@Getter
@Setter
class BrokerAddrInfo {

    /**
     * 集群名称
     */
    private String clusterName;

    /**
     * broker地址
     */
    private String brokerAddr;

    private int hash;

    public BrokerAddrInfo(String clusterName, String brokerAddr) {
        this.clusterName = clusterName;
        this.brokerAddr = brokerAddr;
    }

    public boolean isEmpty() {
        return clusterName.isEmpty() && brokerAddr.isEmpty();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (obj instanceof BrokerAddrInfo) {
            BrokerAddrInfo addr = (BrokerAddrInfo) obj;
            return clusterName.equals(addr.clusterName) && brokerAddr.equals(addr.brokerAddr);
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = hash;
        if (h == 0 && clusterName.length() + brokerAddr.length() > 0) {
            for (int i = 0; i < clusterName.length(); i++) {
                h = 31 * h + clusterName.charAt(i);
            }
            h = 31 * h + '_';
            for (int i = 0; i < brokerAddr.length(); i++) {
                h = 31 * h + brokerAddr.charAt(i);
            }
            hash = h;
        }
        return h;
    }

}

/**
 * broker存活信息
 */
@Getter
@Setter
class BrokerLiveInfo {

    /**
     * 最后一次更新时间
     */
    private long lastUpdateTimestamp;

    /**
     * 心跳检测超时时间
     */
    private long heartbeatTimeoutMillis;

    /**
     * 数据版本
     */
    private DataVersion dataVersion;

    /**
     * channel
     */
    private Channel channel;

    /**
     * 主从备份信息
     */
    private String haServerAddr;

    public BrokerLiveInfo(long lastUpdateTimestamp, long heartbeatTimeoutMillis, DataVersion dataVersion, Channel channel, String haServerAddr) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
        this.heartbeatTimeoutMillis = heartbeatTimeoutMillis;
        this.dataVersion = dataVersion;
        this.channel = channel;
        this.haServerAddr = haServerAddr;
    }

}


/**
 * broker状态变更信息
 */
@Getter
@Setter
class BrokerStatusChangeInfo {

    /**
     * brokerId和brokerAddr映射关系
     */
    Map<Long, String> brokerAddrs;

    /**
     * 离线broker地址
     */
    String offlineBrokerAddr;

    /**
     * 主从备份服务地址
     */
    String haBrokerAddr;

    public BrokerStatusChangeInfo(Map<Long, String> brokerAddrs, String offlineBrokerAddr, String haBrokerAddr) {
        this.brokerAddrs = brokerAddrs;
        this.offlineBrokerAddr = offlineBrokerAddr;
        this.haBrokerAddr = haBrokerAddr;
    }

}
