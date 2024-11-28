package org.apache.rocketmq.namesrv;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.future.FutureTaskExt;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.namesrv.kvconfig.KVConfigManager;
import org.apache.rocketmq.namesrv.processor.ClientRequestProcessor;
import org.apache.rocketmq.namesrv.processor.ClusterTestRequestProcessor;
import org.apache.rocketmq.namesrv.processor.DefaultRequestProcessor;
import org.apache.rocketmq.namesrv.route.ZoneRouteRPCHook;
import org.apache.rocketmq.namesrv.routeinfo.BrokerHousekeepingService;
import org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager;
import org.apache.rocketmq.remoting.Configuration;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.netty.*;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.srvutil.FileWatchService;

import java.util.Collections;
import java.util.concurrent.*;

@Getter
@Setter
public class NameSrvController {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    private static final Logger WATER_MARK_LOG = LoggerFactory.getLogger(LoggerName.NAMESRV_WATER_MARK_LOGGER_NAME);

    private final NamesrvConfig namesrvConfig;

    private final NettyServerConfig nettyServerConfig;

    private final NettyClientConfig nettyClientConfig;

    private final ScheduledExecutorService scheduledExecutorService = ThreadUtils.newScheduledThreadPool(1, new BasicThreadFactory.Builder().namingPattern("NSScheduledThread").daemon(true).build());

    private final ScheduledExecutorService scanExecutorService = ThreadUtils.newScheduledThreadPool(1, new BasicThreadFactory.Builder().namingPattern("NSScanScheduledThread").daemon(true).build());

    private final KVConfigManager kvConfigManager;

    private final RouteInfoManager routeInfoManager;

    private RemotingClient remotingClient;

    private RemotingServer remotingServer;

    private final BrokerHousekeepingService brokerHousekeepingService;

    private ExecutorService defaultExecutor;

    private ExecutorService clientRequestExecutor;

    private BlockingQueue<Runnable> defaultThreadPoolQueue;

    private BlockingQueue<Runnable> clientRequestThreadPoolQueue;

    private final Configuration configuration;

    private FileWatchService fileWatchService;

    public NameSrvController(NamesrvConfig namesrvConfig, NettyServerConfig nettyServerConfig) {
        this(namesrvConfig, nettyServerConfig, new NettyClientConfig());
    }

    public NameSrvController(NamesrvConfig namesrvConfig, NettyServerConfig nettyServerConfig, NettyClientConfig nettyClientConfig) {
        this.namesrvConfig = namesrvConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.nettyClientConfig = nettyClientConfig;
        this.kvConfigManager = new KVConfigManager(this);
        this.brokerHousekeepingService = new BrokerHousekeepingService(this);
        this.routeInfoManager = new RouteInfoManager(namesrvConfig, this);
        this.configuration = new Configuration(LOGGER, this.namesrvConfig, this.nettyServerConfig);
        this.configuration.setStorePathFromConfig(this.namesrvConfig, "configStorePath");
    }

    public boolean initialize() {
        loadConfig();
        initiateNetworkComponents();
        initiateThreadExecutors();
        registerProcessor();
        startScheduleService();
        initiateSslContext();
        initiateRpcHooks();
        return true;
    }

    /**
     * 加载字典配置
     */
    private void loadConfig() {
        this.kvConfigManager.load();
    }

    /**
     * 启动定时任务
     */
    private void startScheduleService() {
        //定时扫描非活跃状态的broker
        this.scanExecutorService.scheduleAtFixedRate(NameSrvController.this.routeInfoManager::scanNotActiveBroker, 5, this.namesrvConfig.getScanNotActiveBrokerInterval(), TimeUnit.MILLISECONDS);
        this.scheduledExecutorService.scheduleAtFixedRate(NameSrvController.this.kvConfigManager::printAllPeriodically, 1, 10, TimeUnit.MINUTES);
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                NameSrvController.this.printWaterMark();
            } catch (Throwable e) {
                LOGGER.error("printWaterMark error.", e);
            }
        }, 10, 1, TimeUnit.SECONDS);
    }

    /**
     * 初始化网络组建
     */
    private void initiateNetworkComponents() {
        this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.brokerHousekeepingService);
        this.remotingClient = new NettyRemotingClient(this.nettyClientConfig);
    }

    /**
     * 初始化线程池
     */
    private void initiateThreadExecutors() {
        this.defaultThreadPoolQueue = new LinkedBlockingQueue<>(this.namesrvConfig.getDefaultThreadPoolQueueCapacity());
        this.defaultExecutor = ThreadUtils.newThreadPoolExecutor(this.namesrvConfig.getDefaultThreadPoolNums(), this.namesrvConfig.getDefaultThreadPoolNums(), 1000 * 60, TimeUnit.MILLISECONDS, this.defaultThreadPoolQueue, new ThreadFactoryImpl("RemotingExecutorThread_"));
        this.clientRequestThreadPoolQueue = new LinkedBlockingQueue<>(this.namesrvConfig.getClientRequestThreadPoolQueueCapacity());
        this.clientRequestExecutor = ThreadUtils.newThreadPoolExecutor(this.namesrvConfig.getClientRequestThreadPoolNums(), this.namesrvConfig.getClientRequestThreadPoolNums(), 1000 * 60, TimeUnit.MILLISECONDS, this.clientRequestThreadPoolQueue, new ThreadFactoryImpl("ClientRequestExecutorThread_"));
    }

    private void initiateSslContext() {
        if (TlsSystemConfig.tlsMode == TlsMode.DISABLED) {
            return;
        }
        String[] watchFiles = {TlsSystemConfig.tlsServerCertPath, TlsSystemConfig.tlsServerKeyPath, TlsSystemConfig.tlsServerTrustCertPath};
        FileWatchService.Listener listener = new FileWatchService.Listener() {
            boolean certChanged, keyChanged = false;
            @Override
            public void onChanged(String path) {
                if (path.equals(TlsSystemConfig.tlsServerTrustCertPath)) {
                    LOGGER.info("The trust certificate changed, reload the ssl context");
                    ((NettyRemotingServer) remotingServer).loadSslContext();
                }
                if (path.equals(TlsSystemConfig.tlsServerCertPath)) {
                    certChanged = true;
                }
                if (path.equals(TlsSystemConfig.tlsServerKeyPath)) {
                    keyChanged = true;
                }
                if (certChanged && keyChanged) {
                    LOGGER.info("The certificate and private key changed, reload the ssl context");
                    certChanged = keyChanged = false;
                    ((NettyRemotingServer) remotingServer).loadSslContext();
                }
            }
        };

        try {
            fileWatchService = new FileWatchService(watchFiles, listener);
        } catch (Exception e) {
            LOGGER.warn("FileWatchService created error, can't load the certificate dynamically");
        }
    }

    private void printWaterMark() {
        WATER_MARK_LOG.info("[WATERMARK] ClientQueueSize:{} ClientQueueSlowTime:{} " + "DefaultQueueSize:{} DefaultQueueSlowTime:{}", this.clientRequestThreadPoolQueue.size(), headSlowTimeMills(this.clientRequestThreadPoolQueue), this.defaultThreadPoolQueue.size(), headSlowTimeMills(this.defaultThreadPoolQueue));
    }

    private long headSlowTimeMills(BlockingQueue<Runnable> q) {
        long slowTimeMills = 0;
        final Runnable firstRunnable = q.peek();

        if (firstRunnable instanceof FutureTaskExt) {
            final Runnable inner = ((FutureTaskExt<?>) firstRunnable).getRunnable();
            if (inner instanceof RequestTask) {
                slowTimeMills = System.currentTimeMillis() - ((RequestTask) inner).getCreateTimestamp();
            }
        }

        if (slowTimeMills < 0) {
            slowTimeMills = 0;
        }

        return slowTimeMills;
    }

    /**
     * 注册处理器
     */
    private void registerProcessor() {
        if (namesrvConfig.isClusterTest()) {
            this.remotingServer.registerDefaultProcessor(new ClusterTestRequestProcessor(this, namesrvConfig.getProductEnvName()), this.defaultExecutor);
        } else {
            ClientRequestProcessor clientRequestProcessor = new ClientRequestProcessor(this);
            this.remotingServer.registerProcessor(RequestCode.GET_ROUTEINFO_BY_TOPIC, clientRequestProcessor, this.clientRequestExecutor);
            this.remotingServer.registerDefaultProcessor(new DefaultRequestProcessor(this), this.defaultExecutor);
        }
    }

    private void initiateRpcHooks() {
        this.remotingServer.registerRPCHook(new ZoneRouteRPCHook());
    }

    /**
     * 启动
     */
    public void start() throws Exception {
        this.remotingServer.start();
        if (0 == nettyServerConfig.getListenPort()) {
            nettyServerConfig.setListenPort(this.remotingServer.localListenPort());
        }

        this.remotingClient.updateNameServerAddressList(Collections.singletonList(NetworkUtil.getLocalAddress() + ":" + nettyServerConfig.getListenPort()));
        this.remotingClient.start();
        if (this.fileWatchService != null) {
            this.fileWatchService.start();
        }
        this.routeInfoManager.start();
    }

    public void shutdown() {
        this.remotingClient.shutdown();
        this.remotingServer.shutdown();
        this.defaultExecutor.shutdown();
        this.clientRequestExecutor.shutdown();
        this.scheduledExecutorService.shutdown();
        this.scanExecutorService.shutdown();
        this.routeInfoManager.shutdown();
        if (this.fileWatchService != null) {
            this.fileWatchService.shutdown();
        }
    }

}
