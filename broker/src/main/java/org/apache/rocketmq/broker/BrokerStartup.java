/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.broker;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.store.config.MessageStoreConfig;

/**
 * Broker 进程入口：解析命令行与配置文件，构建并启动 {@link BrokerController}。
 * <p>
 * 启动链路一般为：{@link #parseCmdLine(String[])} 或 {@link #configFileToConfigContext(String)} 得到
 * {@link ConfigContext} → {@link #buildBrokerController(ConfigContext)} 构造控制器 →
 * {@link #createBrokerController(String[])} 中调用 {@link BrokerController#initialize()} 并完成
 * {@link Runtime#addShutdownHook(Thread)} 注册，最后 {@link #start(BrokerController)} 拉起服务。
 */
public class BrokerStartup {

    public static Logger log;

    /**
     * JVM 入口：创建控制器并启动 Broker。
     *
     * @param args 命令行参数，支持 -c 配置文件、-p/-m 打印配置等（见本类私有方法 {@code buildCommandlineOptions}）
     */
    public static void main(String[] args) {
        start(createBrokerController(args));
    }

    /**
     * 启动已初始化完成的 {@link BrokerController}，打印成功提示（含 Broker 地址、序列化方式、NameServer 等）。
     *
     * @param controller 已完成 {@link BrokerController#initialize()} 的实例
     * @return 启动成功时返回同一实例；异常时进程退出且可能返回 {@code null}
     */
    public static BrokerController start(BrokerController controller) {
        try {
            controller.start();

            String tip = String.format("The broker[%s, %s] boot success. serializeType=%s",
                controller.getBrokerConfig().getBrokerName(), controller.getBrokerAddr(),
                RemotingCommand.getSerializeTypeConfigInThisServer());

            if (null != controller.getBrokerConfig().getNamesrvAddr()) {
                tip += " and name server is " + controller.getBrokerConfig().getNamesrvAddr();
            }

            log.info(tip);
            System.out.printf("%s%n", tip);
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    /**
     * 优雅关闭 Broker，委托 {@link BrokerController#shutdown()}。
     *
     * @param controller 可为 null，为 null 时不做任何操作
     */
    public static void shutdown(final BrokerController controller) {
        if (null != controller) {
            controller.shutdown();
        }
    }

    /**
     * 解析命令行：读取 -c 指定配置文件载入 {@link ConfigContext}，再将命令行上的覆盖项写入 {@link BrokerConfig}。
     * <p>
     * 若指定 -p 则打印全部配置项后退出；若指定 -m 则仅打印重要字段后退出。
     *
     * @param args 与 {@link #main(String[])} 相同
     * @return 聚合了 Broker/Netty/Store/Auth 等配置的上下文
     */
    public static ConfigContext parseCmdLine(String[] args) throws Exception {
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        CommandLine commandLine = ServerUtil.parseCmdLine(
            "mqbroker", args, buildCommandlineOptions(options), new DefaultParser());
        if (null == commandLine) {
            System.exit(-1);
        }

        ConfigContext configContext;
        String filePath = null;
        if (commandLine.hasOption('c')) {
            filePath = commandLine.getOptionValue('c');
        }

        configContext = configFileToConfigContext(filePath);

        if (commandLine.hasOption('p') && configContext != null) {
            Logger console = LoggerFactory.getLogger(LoggerName.BROKER_CONSOLE_NAME);
            MixAll.printObjectProperties(console, configContext.getBrokerConfig());
            MixAll.printObjectProperties(console, configContext.getNettyServerConfig());
            MixAll.printObjectProperties(console, configContext.getNettyClientConfig());
            MixAll.printObjectProperties(console, configContext.getAuthConfig());
            System.exit(0);
        } else if (commandLine.hasOption('m') && configContext != null) {
            Logger console = LoggerFactory.getLogger(LoggerName.BROKER_CONSOLE_NAME);
            MixAll.printObjectProperties(console, configContext.getBrokerConfig(), true);
            MixAll.printObjectProperties(console, configContext.getNettyServerConfig(), true);
            MixAll.printObjectProperties(console, configContext.getNettyClientConfig(), true);
            MixAll.printObjectProperties(console, configContext.getAuthConfig(), true);
            System.exit(0);
        }

        assert configContext != null;
        MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), configContext.getBrokerConfig());

        return configContext;
    }

    /**
     * 从可选的配置文件路径加载 Properties，并填充到 Broker、Netty 服务端/客户端、消息存储与鉴权配置对象中。
     * <p>
     * 若 {@code filePath} 非空，会同步更新 {@link BrokerPathConfigHelper#setBrokerConfigPath(String)}，
     * 供其它组件解析相对路径配置。
     *
     * @param filePath 配置文件路径，可为空（仅使用各类配置的默认值）
     */
    public static ConfigContext configFileToConfigContext(String filePath) throws Exception {
        SystemConfigFileHelper systemConfigFileHelper = new SystemConfigFileHelper();
        BrokerConfig brokerConfig = new BrokerConfig();
        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        NettyClientConfig nettyClientConfig = new NettyClientConfig();
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        AuthConfig authConfig = new AuthConfig();

        nettyServerConfig.setListenPort(10911);
        messageStoreConfig.setHaListenPort(0);

        Properties properties = new Properties();
        if (StringUtils.isNotBlank(filePath)) {
            systemConfigFileHelper.setFile(filePath);
            BrokerPathConfigHelper.setBrokerConfigPath(filePath);
            properties = systemConfigFileHelper.loadConfig();
        }

        if (properties != null) {
            properties2SystemEnv(properties);
            MixAll.properties2Object(properties, brokerConfig);
            MixAll.properties2Object(properties, nettyServerConfig);
            MixAll.properties2Object(properties, nettyClientConfig);
            MixAll.properties2Object(properties, messageStoreConfig);
            MixAll.properties2Object(properties, authConfig);
        }

        return new ConfigContext.Builder()
            .configFilePath(filePath)
            .properties(properties)
            .brokerConfig(brokerConfig)
            .messageStoreConfig(messageStoreConfig)
            .nettyServerConfig(nettyServerConfig)
            .nettyClientConfig(nettyClientConfig)
            .authConfig(authConfig)
            .build();
    }

    /**
     * 基于已加载的 {@link ConfigContext} 构造 {@link BrokerController}：校验环境变量与关键配置、
     * 推导 HA 端口与 BrokerId、打印配置摘要，并将原始 Properties 注册到控制器的 {@link org.apache.rocketmq.remoting.Configuration} 以防丢失。
     *
     * @param configContext 由配置文件与命令行得到的聚合上下文
     * @return 已关联 {@link ConfigContext}、尚未 {@link BrokerController#initialize()} 的控制器实例
     */
    public static BrokerController buildBrokerController(ConfigContext configContext) {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));

        BrokerConfig brokerConfig = configContext.getBrokerConfig();
        MessageStoreConfig messageStoreConfig = configContext.getMessageStoreConfig();
        NettyClientConfig nettyClientConfig = configContext.getNettyClientConfig();
        NettyServerConfig nettyServerConfig = configContext.getNettyServerConfig();
        AuthConfig authConfig = configContext.getAuthConfig();
        Properties properties = configContext.getProperties();

        if (null == brokerConfig.getRocketmqHome()) {
            System.out.printf("Please set the %s variable in your environment " +
                "to match the location of the RocketMQ installation", MixAll.ROCKETMQ_HOME_ENV);
            System.exit(-2);
        }

        // Validate namesrvAddr
        String namesrvAddr = brokerConfig.getNamesrvAddr();
        if (StringUtils.isNotBlank(namesrvAddr)) {
            try {
                String[] addrArray = namesrvAddr.split(";");
                for (String addr : addrArray) {
                    NetworkUtil.string2SocketAddress(addr);
                }
            } catch (Exception e) {
                System.out.printf("The Name Server Address[%s] illegal, please set it as follows, " +
                    "\"127.0.0.1:9876;192.168.0.1:9876\"%n", namesrvAddr);
                System.exit(-3);
            }
        }

        // Set broker role according to ha config
        if (!brokerConfig.isEnableControllerMode()) {
            switch (messageStoreConfig.getBrokerRole()) {
                case ASYNC_MASTER:
                case SYNC_MASTER:
                    brokerConfig.setBrokerId(MixAll.MASTER_ID);
                    break;
                case SLAVE:
                    if (brokerConfig.getBrokerId() <= MixAll.MASTER_ID) {
                        System.out.printf("Slave's brokerId must be > 0%n");
                        System.exit(-3);
                    }
                    break;
                default:
                    break;
            }
        }

        if (messageStoreConfig.isEnableDLegerCommitLog()) {
            brokerConfig.setBrokerId(-1);
        }

        if (brokerConfig.isEnableControllerMode() && messageStoreConfig.isEnableDLegerCommitLog()) {
            System.out.printf("The config enableControllerMode and enableDLegerCommitLog cannot both be true.%n");
            System.exit(-4);
        }

        if (messageStoreConfig.getHaListenPort() <= 0) {
            messageStoreConfig.setHaListenPort(nettyServerConfig.getListenPort() + 1);
        }

        brokerConfig.setInBrokerContainer(false);

        System.setProperty("brokerLogDir", "");
        if (brokerConfig.isIsolateLogEnable()) {
            System.setProperty("brokerLogDir", brokerConfig.getBrokerName() + "_" + brokerConfig.getBrokerId());
        }
        if (brokerConfig.isIsolateLogEnable() && messageStoreConfig.isEnableDLegerCommitLog()) {
            System.setProperty("brokerLogDir", brokerConfig.getBrokerName() + "_" + messageStoreConfig.getdLegerSelfId());
        }

        log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
        MixAll.printObjectProperties(log, brokerConfig);
        MixAll.printObjectProperties(log, nettyServerConfig);
        MixAll.printObjectProperties(log, nettyClientConfig);
        MixAll.printObjectProperties(log, messageStoreConfig);

        authConfig.setConfigName(brokerConfig.getBrokerName());
        authConfig.setClusterName(brokerConfig.getBrokerClusterName());
        authConfig.setAuthConfigPath(messageStoreConfig.getStorePathRootDir() + File.separator + "config");

        final BrokerController controller = new BrokerController(
            brokerConfig, nettyServerConfig, nettyClientConfig, messageStoreConfig, authConfig);

        // Remember all configs to prevent discard
        controller.getConfiguration().registerConfig(properties);

        controller.setConfigContext(configContext);

        return controller;
    }

    /**
     * 供 {@link Runtime#addShutdownHook(Thread)} 使用：在 JVM 退出时调用 {@link BrokerController#shutdown()}，
     * 并保证钩子逻辑在多线程触发时仅执行一次。
     *
     * @param brokerController 运行中的 Broker 控制器
     * @return 可在新线程中执行的关闭任务
     */
    public static Runnable buildShutdownHook(BrokerController brokerController) {
        return new Runnable() {
            private volatile boolean hasShutdown = false;
            private final AtomicInteger shutdownTimes = new AtomicInteger(0);

            @Override
            public void run() {
                synchronized (this) {
                    log.info("Shutdown hook was invoked, {}", this.shutdownTimes.incrementAndGet());
                    if (!this.hasShutdown) {
                        this.hasShutdown = true;
                        long beginTime = System.currentTimeMillis();
                        brokerController.shutdown();
                        long consumingTimeTotal = System.currentTimeMillis() - beginTime;
                        log.info("Shutdown hook over, consuming total time(ms): {}", consumingTimeTotal);
                    }
                }
            }
        };
    }

    /**
     * 完整工厂方法：解析参数 → 构建控制器 → {@link BrokerController#initialize()} → 注册 shutdown hook。
     *
     * @param args 命令行参数
     * @return 已初始化且已注册关闭钩子的控制器；异常时进程直接退出
     */
    public static BrokerController createBrokerController(String[] args) {
        try {
            ConfigContext configContext = parseCmdLine(args);
            BrokerController controller = buildBrokerController(configContext);
            boolean initResult = controller.initialize();
            if (!initResult) {
                controller.shutdown();
                System.exit(-3);
            }
            Runtime.getRuntime().addShutdownHook(new Thread(buildShutdownHook(controller)));
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return null;
    }

    private static void properties2SystemEnv(Properties properties) {
        if (properties == null) {
            return;
        }
        String rmqAddressServerDomain = properties.getProperty("rmqAddressServerDomain", MixAll.WS_DOMAIN_NAME);
        String rmqAddressServerSubGroup = properties.getProperty("rmqAddressServerSubGroup", MixAll.WS_DOMAIN_SUBGROUP);
        System.setProperty("rocketmq.namesrv.domain", rmqAddressServerDomain);
        System.setProperty("rocketmq.namesrv.domain.subgroup", rmqAddressServerSubGroup);
    }

    private static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Broker config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("m", "printImportantConfig", false, "Print important config item");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    public static class SystemConfigFileHelper {
        private static final Logger LOGGER = LoggerFactory.getLogger(SystemConfigFileHelper.class);

        private String file;

        public SystemConfigFileHelper() {
        }

        public Properties loadConfig() throws Exception {
            Properties properties = new Properties();
            try (InputStream in = new BufferedInputStream(Files.newInputStream(Paths.get(file)))) {
                properties.load(in);
            }
            return properties;
        }

        public void update(Properties properties) throws Exception {
            LOGGER.error("[SystemConfigFileHelper] update no thing.");
        }

        public void setFile(String file) {
            this.file = file;
        }

        public String getFile() {
            return file;
        }
    }
}
