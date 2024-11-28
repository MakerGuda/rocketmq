package org.apache.rocketmq.namesrv;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.common.JraftConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.controller.ControllerManager;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.srvutil.ShutdownHookThread;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.Callable;

public class NameSrvStartup {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    private static final Logger logConsole = LoggerFactory.getLogger(LoggerName.NAMESRV_CONSOLE_LOGGER_NAME);

    private static Properties properties = null;

    private static NamesrvConfig namesrvConfig = null;

    private static NettyServerConfig nettyServerConfig = null;

    private static NettyClientConfig nettyClientConfig = null;

    private static ControllerConfig controllerConfig = null;

    public static void main(String[] args) {
        main0(args);
        controllerManagerMain();
    }

    public static void main0(String[] args) {
        try {
            parseCommandlineAndConfigFile(args);
            createAndStartNameSrvController();
        } catch (Throwable e) {
            System.exit(-1);
        }
    }

    public static void controllerManagerMain() {
        try {
            if (namesrvConfig.isEnableControllerInNamesrv()) {
                createAndStartControllerManager();
            }
        } catch (Throwable e) {
            System.exit(-1);
        }
    }

    public static void parseCommandlineAndConfigFile(String[] args) throws Exception {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        CommandLine commandLine = ServerUtil.parseCmdLine("mq name srv", args, buildCommandlineOptions(options), new DefaultParser());
        if (null == commandLine) {
            System.exit(-1);
            return;
        }
        namesrvConfig = new NamesrvConfig();
        nettyServerConfig = new NettyServerConfig();
        nettyClientConfig = new NettyClientConfig();
        nettyServerConfig.setListenPort(9876);
        if (commandLine.hasOption('c')) {
            String file = commandLine.getOptionValue('c');
            if (file != null) {
                InputStream in = new BufferedInputStream(Files.newInputStream(Paths.get(file)));
                properties = new Properties();
                properties.load(in);
                MixAll.properties2Object(properties, namesrvConfig);
                MixAll.properties2Object(properties, nettyServerConfig);
                MixAll.properties2Object(properties, nettyClientConfig);
                if (namesrvConfig.isEnableControllerInNamesrv()) {
                    controllerConfig = new ControllerConfig();
                    JraftConfig jraftConfig = new JraftConfig();
                    controllerConfig.setJraftConfig(jraftConfig);
                    MixAll.properties2Object(properties, controllerConfig);
                    MixAll.properties2Object(properties, jraftConfig);
                }
                namesrvConfig.setConfigStorePath(file);
                System.out.printf("load config properties file OK, %s%n", file);
                in.close();
            }
        }
        MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), namesrvConfig);
        if (commandLine.hasOption('p')) {
            MixAll.printObjectProperties(logConsole, namesrvConfig);
            MixAll.printObjectProperties(logConsole, nettyServerConfig);
            MixAll.printObjectProperties(logConsole, nettyClientConfig);
            if (namesrvConfig.isEnableControllerInNamesrv()) {
                MixAll.printObjectProperties(logConsole, controllerConfig);
            }
            System.exit(0);
        }
        if (null == namesrvConfig.getRocketmqHome()) {
            System.out.printf("Please set the %s variable in your environment to match the location of the RocketMQ installation%n", MixAll.ROCKETMQ_HOME_ENV);
            System.exit(-2);
        }
        MixAll.printObjectProperties(log, namesrvConfig);
        MixAll.printObjectProperties(log, nettyServerConfig);
    }

    public static void createAndStartNameSrvController() throws Exception {
        NameSrvController controller = createNamesrvController();
        start(controller);
        NettyServerConfig serverConfig = controller.getNettyServerConfig();
        String tip = String.format("The Name Server boot success. serializeType=%s, address %s:%d", RemotingCommand.getSerializeTypeConfigInThisServer(), serverConfig.getBindAddress(), serverConfig.getListenPort());
        log.info(tip);
        System.out.printf("%s%n", tip);
    }

    public static NameSrvController createNamesrvController() {
        final NameSrvController controller = new NameSrvController(namesrvConfig, nettyServerConfig, nettyClientConfig);
        controller.getConfiguration().registerConfig(properties);
        return controller;
    }

    public static NameSrvController start(final NameSrvController controller) throws Exception {
        if (null == controller) {
            throw new IllegalArgumentException("Name Srv Controller is null");
        }
        boolean initResult = controller.initialize();
        if (!initResult) {
            controller.shutdown();
            System.exit(-3);
        }
        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(log, (Callable<Void>) () -> {
            controller.shutdown();
            return null;
        }));
        controller.start();
        return controller;
    }

    public static void createAndStartControllerManager() throws Exception {
        ControllerManager controllerManager = createControllerManager();
        start(controllerManager);
        String tip = "The ControllerManager boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();
        log.info(tip);
        System.out.printf("%s%n", tip);
    }

    public static ControllerManager createControllerManager() throws Exception {
        NettyServerConfig controllerNettyServerConfig = (NettyServerConfig) nettyServerConfig.clone();
        ControllerManager controllerManager = new ControllerManager(controllerConfig, controllerNettyServerConfig, nettyClientConfig);
        controllerManager.getConfiguration().registerConfig(properties);
        return controllerManager;
    }

    public static ControllerManager start(final ControllerManager controllerManager) throws Exception {
        if (null == controllerManager) {
            throw new IllegalArgumentException("ControllerManager is null");
        }
        boolean initResult = controllerManager.initialize();
        if (!initResult) {
            controllerManager.shutdown();
            System.exit(-3);
        }
        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(log, (Callable<Void>) () -> {
            controllerManager.shutdown();
            return null;
        }));
        controllerManager.start();
        return controllerManager;
    }

    public static void shutdown(final NameSrvController controller) {
        controller.shutdown();
    }

    public static void shutdown(final ControllerManager controllerManager) {
        controllerManager.shutdown();
    }

    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Name server config properties file");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option("p", "printConfigItem", false, "Print all config items");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }

}
