package org.apache.rocketmq.acl.plain;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.PermissionChecker;
import org.apache.rocketmq.acl.common.*;
import org.apache.rocketmq.common.AclConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.srvutil.AclFileWatchService;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class PlainPermissionManager {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private final String fileHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV));

    private final String defaultAclDir;

    private final String defaultAclFile;

    private Map<String, Map<String, PlainAccessResource>> aclPlainAccessResourceMap = new HashMap<>();

    private Map<String, String> accessKeyTable = new HashMap<>();

    private List<RemoteAddressStrategy> globalWhiteRemoteAddressStrategy = new ArrayList<>();

    private final RemoteAddressStrategyFactory remoteAddressStrategyFactory = new RemoteAddressStrategyFactory();

    private Map<String, List<RemoteAddressStrategy>> globalWhiteRemoteAddressStrategyMap = new HashMap<>();

    private boolean isWatchStart;

    private Map<String, DataVersion> dataVersionMap = new HashMap<>();

    private final DataVersion dataVersion = new DataVersion();

    private List<String> fileList = new ArrayList<>();

    private final PermissionChecker permissionChecker = new PlainPermissionChecker();

    public PlainPermissionManager() {
        this.defaultAclDir = MixAll.dealFilePath(fileHome + File.separator + "conf" + File.separator + "acl");
        this.defaultAclFile = MixAll.dealFilePath(fileHome + File.separator + System.getProperty("rocketmq.acl.plain.file", "conf" + File.separator + "plain_acl.yml"));
        load();
        watch();
    }

    public List<String> getAllAclFiles(String path) {
        if (!new File(path).exists()) {
            log.info("The default acl dir {} is not exist", path);
            return new ArrayList<>();
        }
        List<String> allAclFileFullPath = new ArrayList<>();
        File file = new File(path);
        File[] files = file.listFiles();
        for (int i = 0; files != null && i < files.length; i++) {
            String fileName = files[i].getAbsolutePath();
            File f = new File(fileName);
            if (fileName.endsWith(".yml") || fileName.endsWith(".yaml")) {
                allAclFileFullPath.add(fileName);
            } else if (f.isDirectory()) {
                allAclFileFullPath.addAll(getAllAclFiles(fileName));
            }
        }
        return allAclFileFullPath;
    }

    public void load() {
        if (fileHome == null || fileHome.isEmpty()) {
            return;
        }
        Map<String, Map<String, PlainAccessResource>> aclPlainAccessResourceMap = new HashMap<>();
        Map<String, String> accessKeyTable = new HashMap<>();
        List<RemoteAddressStrategy> globalWhiteRemoteAddressStrategy = new ArrayList<>();
        Map<String, List<RemoteAddressStrategy>> globalWhiteRemoteAddressStrategyMap = new HashMap<>();
        Map<String, DataVersion> dataVersionMap = new HashMap<>();
        assureAclConfigFilesExist();
        fileList = getAllAclFiles(defaultAclDir);
        if (new File(defaultAclFile).exists() && !fileList.contains(defaultAclFile)) {
            fileList.add(defaultAclFile);
        }
        for (String path : fileList) {
            final String currentFile = MixAll.dealFilePath(path);
            PlainAccessData plainAclConfData = AclUtils.getYamlDataObject(currentFile, PlainAccessData.class);
            if (plainAclConfData == null) {
                log.warn("No data in file {}", currentFile);
                continue;
            }
            log.info("Broker plain acl conf data is : {}", plainAclConfData.toString());
            List<RemoteAddressStrategy> globalWhiteRemoteAddressStrategyList = new ArrayList<>();
            List<String> globalWhiteRemoteAddressesList = plainAclConfData.getGlobalWhiteRemoteAddresses();
            if (globalWhiteRemoteAddressesList != null && !globalWhiteRemoteAddressesList.isEmpty()) {
                for (String address : globalWhiteRemoteAddressesList) {
                    globalWhiteRemoteAddressStrategyList.add(remoteAddressStrategyFactory.getRemoteAddressStrategy(address));
                }
            }
            if (!globalWhiteRemoteAddressStrategyList.isEmpty()) {
                globalWhiteRemoteAddressStrategyMap.put(currentFile, globalWhiteRemoteAddressStrategyList);
                globalWhiteRemoteAddressStrategy.addAll(globalWhiteRemoteAddressStrategyList);
            }
            List<PlainAccessConfig> accounts = plainAclConfData.getAccounts();
            Map<String, PlainAccessResource> plainAccessResourceMap = new HashMap<>();
            if (accounts != null && !accounts.isEmpty()) {
                for (PlainAccessConfig plainAccessConfig : accounts) {
                    PlainAccessResource plainAccessResource = buildPlainAccessResource(plainAccessConfig);
                    if (accessKeyTable.get(plainAccessResource.getAccessKey()) == null) {
                        plainAccessResourceMap.put(plainAccessResource.getAccessKey(), plainAccessResource);
                        accessKeyTable.put(plainAccessResource.getAccessKey(), currentFile);
                    } else {
                        log.warn("The accessKey {} is repeated in multiple ACL files", plainAccessResource.getAccessKey());
                    }
                }
            }
            if (!plainAccessResourceMap.isEmpty()) {
                aclPlainAccessResourceMap.put(currentFile, plainAccessResourceMap);
            }
            List<PlainAccessData.DataVersion> dataVersions = plainAclConfData.getDataVersion();
            DataVersion dataVersion = new DataVersion();
            if (dataVersions != null && !dataVersions.isEmpty()) {
                DataVersion firstElement = new DataVersion();
                firstElement.setCounter(new AtomicLong(dataVersions.get(0).getCounter()));
                firstElement.setTimestamp(dataVersions.get(0).getTimestamp());
                dataVersion.assignNewOne(firstElement);
            }
            dataVersionMap.put(currentFile, dataVersion);
        }
        if (dataVersionMap.containsKey(defaultAclFile)) {
            this.dataVersion.assignNewOne(dataVersionMap.get(defaultAclFile));
        }
        this.dataVersionMap = dataVersionMap;
        this.globalWhiteRemoteAddressStrategyMap = globalWhiteRemoteAddressStrategyMap;
        this.globalWhiteRemoteAddressStrategy = globalWhiteRemoteAddressStrategy;
        this.aclPlainAccessResourceMap = aclPlainAccessResourceMap;
        this.accessKeyTable = accessKeyTable;
    }

    /**
     * Currently GlobalWhiteAddress is defined in {@link #defaultAclFile}, so make sure it exists.
     */
    private void assureAclConfigFilesExist() {
        final Path defaultAclFilePath = Paths.get(this.defaultAclFile);
        if (!Files.exists(defaultAclFilePath)) {
            try {
                Files.createFile(defaultAclFilePath);
            } catch (FileAlreadyExistsException e) {
                // Maybe created by other threads
            } catch (IOException e) {
                log.error("Error in creating " + this.defaultAclFile, e);
                throw new AclException(e.getMessage());
            }
        }
    }

    public void load(String aclFilePath) {
        aclFilePath = MixAll.dealFilePath(aclFilePath);
        Map<String, PlainAccessResource> plainAccessResourceMap = new HashMap<>();
        List<RemoteAddressStrategy> globalWhiteRemoteAddressStrategy = new ArrayList<>();
        PlainAccessData plainAclConfData = AclUtils.getYamlDataObject(aclFilePath, PlainAccessData.class);
        if (plainAclConfData == null) {
            log.warn("No data in {}, skip it", aclFilePath);
            return;
        }
        log.info("Broker plain acl conf data is : {}", plainAclConfData.toString());
        List<String> globalWhiteRemoteAddressesList = plainAclConfData.getGlobalWhiteRemoteAddresses();
        if (globalWhiteRemoteAddressesList != null && !globalWhiteRemoteAddressesList.isEmpty()) {
            for (String address : globalWhiteRemoteAddressesList) {
                globalWhiteRemoteAddressStrategy.add(remoteAddressStrategyFactory.getRemoteAddressStrategy(address));
            }
        }
        this.globalWhiteRemoteAddressStrategy.addAll(globalWhiteRemoteAddressStrategy);
        if (this.globalWhiteRemoteAddressStrategyMap.get(aclFilePath) != null) {
            List<RemoteAddressStrategy> remoteAddressStrategyList = this.globalWhiteRemoteAddressStrategyMap.get(aclFilePath);
            for (RemoteAddressStrategy remoteAddressStrategy : remoteAddressStrategyList) {
                this.globalWhiteRemoteAddressStrategy.remove(remoteAddressStrategy);
            }
            this.globalWhiteRemoteAddressStrategyMap.put(aclFilePath, globalWhiteRemoteAddressStrategy);
        }
        List<PlainAccessConfig> accounts = plainAclConfData.getAccounts();
        if (accounts != null && !accounts.isEmpty()) {
            for (PlainAccessConfig plainAccessConfig : accounts) {
                PlainAccessResource plainAccessResource = buildPlainAccessResource(plainAccessConfig);
                String oldPath = this.accessKeyTable.get(plainAccessResource.getAccessKey());
                if (oldPath == null || aclFilePath.equals(oldPath)) {
                    plainAccessResourceMap.put(plainAccessResource.getAccessKey(), plainAccessResource);
                    this.accessKeyTable.put(plainAccessResource.getAccessKey(), aclFilePath);
                } else {
                    log.warn("The accessKey {} is repeated in multiple ACL files", plainAccessResource.getAccessKey());
                }
            }
        }
        List<PlainAccessData.DataVersion> dataVersions = plainAclConfData.getDataVersion();
        DataVersion dataVersion = new DataVersion();
        if (dataVersions != null && !dataVersions.isEmpty()) {
            DataVersion firstElement = new DataVersion();
            firstElement.setCounter(new AtomicLong(dataVersions.get(0).getCounter()));
            firstElement.setTimestamp(dataVersions.get(0).getTimestamp());
            dataVersion.assignNewOne(firstElement);
        }
        this.aclPlainAccessResourceMap.put(aclFilePath, plainAccessResourceMap);
        this.dataVersionMap.put(aclFilePath, dataVersion);
        if (aclFilePath.equals(defaultAclFile)) {
            this.dataVersion.assignNewOne(dataVersion);
        }
    }

    public String getAclConfigDataVersion() {
        return this.dataVersion.toJson();
    }

    public PlainAccessData updateAclConfigFileVersion(String aclFileName, PlainAccessData updateAclConfigMap) {
        List<PlainAccessData.DataVersion> dataVersions = updateAclConfigMap.getDataVersion();
        DataVersion dataVersion = new DataVersion();
        if (dataVersions != null && !dataVersions.isEmpty()) {
            dataVersion.setTimestamp(dataVersions.get(0).getTimestamp());
            dataVersion.setCounter(new AtomicLong(dataVersions.get(0).getCounter()));
        }
        dataVersion.nextVersion();
        List<PlainAccessData.DataVersion> versionElement = new ArrayList<>();
        PlainAccessData.DataVersion dataVersionNew = new PlainAccessData.DataVersion();
        dataVersionNew.setTimestamp(dataVersion.getTimestamp());
        dataVersionNew.setCounter(dataVersion.getCounter().get());
        versionElement.add(dataVersionNew);
        updateAclConfigMap.setDataVersion(versionElement);
        dataVersionMap.put(aclFileName, dataVersion);
        return updateAclConfigMap;
    }

    public boolean updateAccessConfig(PlainAccessConfig plainAccessConfig) {
        if (plainAccessConfig == null) {
            log.error("Parameter value plainAccessConfig is null,Please check your parameter");
            throw new AclException("Parameter value plainAccessConfig is null, Please check your parameter");
        }
        checkPlainAccessConfig(plainAccessConfig);
        Permission.checkResourcePerms(plainAccessConfig.getTopicPerms());
        Permission.checkResourcePerms(plainAccessConfig.getGroupPerms());
        if (accessKeyTable.containsKey(plainAccessConfig.getAccessKey())) {
            PlainAccessConfig updateAccountMap = null;
            String aclFileName = accessKeyTable.get(plainAccessConfig.getAccessKey());
            PlainAccessData aclAccessConfigMap = AclUtils.getYamlDataObject(aclFileName, PlainAccessData.class);
            List<PlainAccessConfig> accounts = aclAccessConfigMap.getAccounts();
            if (null != accounts) {
                for (PlainAccessConfig account : accounts) {
                    if (account.getAccessKey().equals(plainAccessConfig.getAccessKey())) {
                        accounts.remove(account);
                        updateAccountMap = createAclAccessConfigMap(account, plainAccessConfig);
                        accounts.add(updateAccountMap);
                        aclAccessConfigMap.setAccounts(accounts);
                        break;
                    }
                }
            } else {
                accounts = new LinkedList<>();
                updateAccountMap = createAclAccessConfigMap(null, plainAccessConfig);
                accounts.add(updateAccountMap);
                aclAccessConfigMap.setAccounts(accounts);
            }
            Map<String, PlainAccessResource> accountMap = aclPlainAccessResourceMap.get(aclFileName);
            if (accountMap == null) {
                accountMap = new HashMap<>(1);
                accountMap.put(plainAccessConfig.getAccessKey(), buildPlainAccessResource(plainAccessConfig));
            } else if (accountMap.isEmpty()) {
                accountMap.put(plainAccessConfig.getAccessKey(), buildPlainAccessResource(plainAccessConfig));
            } else {
                for (Map.Entry<String, PlainAccessResource> entry : accountMap.entrySet()) {
                    if (entry.getValue().getAccessKey().equals(plainAccessConfig.getAccessKey())) {
                        PlainAccessResource plainAccessResource = buildPlainAccessResource(plainAccessConfig);
                        accountMap.put(entry.getKey(), plainAccessResource);
                        break;
                    }
                }
            }
            aclPlainAccessResourceMap.put(aclFileName, accountMap);
            return AclUtils.writeDataObject(aclFileName, updateAclConfigFileVersion(aclFileName, aclAccessConfigMap));
        } else {
            String fileName = MixAll.dealFilePath(defaultAclFile);
            if (aclPlainAccessResourceMap.get(defaultAclFile) == null || aclPlainAccessResourceMap.get(defaultAclFile).isEmpty()) {
                try {
                    File defaultAclFile = new File(fileName);
                    if (!defaultAclFile.exists()) {
                        defaultAclFile.createNewFile();
                    }
                } catch (IOException e) {
                    log.warn("create default acl file has exception when update accessConfig. ", e);
                }
            }
            PlainAccessData aclAccessConfigMap = AclUtils.getYamlDataObject(defaultAclFile, PlainAccessData.class);
            if (aclAccessConfigMap == null) {
                aclAccessConfigMap = new PlainAccessData();
            }
            List<PlainAccessConfig> accounts = aclAccessConfigMap.getAccounts();
            if (null == accounts) {
                accounts = new ArrayList<>();
            }
            accounts.add(createAclAccessConfigMap(null, plainAccessConfig));
            aclAccessConfigMap.setAccounts(accounts);
            accessKeyTable.put(plainAccessConfig.getAccessKey(), fileName);
            if (aclPlainAccessResourceMap.get(fileName) == null) {
                Map<String, PlainAccessResource> plainAccessResourceMap = new HashMap<>(1);
                plainAccessResourceMap.put(plainAccessConfig.getAccessKey(), buildPlainAccessResource(plainAccessConfig));
                aclPlainAccessResourceMap.put(fileName, plainAccessResourceMap);
            } else {
                Map<String, PlainAccessResource> plainAccessResourceMap = aclPlainAccessResourceMap.get(fileName);
                plainAccessResourceMap.put(plainAccessConfig.getAccessKey(), buildPlainAccessResource(plainAccessConfig));
                aclPlainAccessResourceMap.put(fileName, plainAccessResourceMap);
            }
            return AclUtils.writeDataObject(defaultAclFile, updateAclConfigFileVersion(defaultAclFile, aclAccessConfigMap));
        }
    }

    public PlainAccessConfig createAclAccessConfigMap(PlainAccessConfig existedAccountMap,
        PlainAccessConfig plainAccessConfig) {
        PlainAccessConfig newAccountsMap;
        if (existedAccountMap == null) {
            newAccountsMap = new PlainAccessConfig();
        } else {
            newAccountsMap = existedAccountMap;
        }
        if (StringUtils.isEmpty(plainAccessConfig.getAccessKey()) ||
            plainAccessConfig.getAccessKey().length() <= AclConstants.ACCESS_KEY_MIN_LENGTH) {
            throw new AclException(String.format("The accessKey=%s cannot be null and length should longer than 6", plainAccessConfig.getAccessKey()));
        }
        newAccountsMap.setAccessKey(plainAccessConfig.getAccessKey());
        if (!StringUtils.isEmpty(plainAccessConfig.getSecretKey())) {
            if (plainAccessConfig.getSecretKey().length() <= AclConstants.SECRET_KEY_MIN_LENGTH) {
                throw new AclException(String.format("The secretKey=%s value length should longer than 6", plainAccessConfig.getSecretKey()));
            }
            newAccountsMap.setSecretKey(plainAccessConfig.getSecretKey());
        }
        if (plainAccessConfig.getWhiteRemoteAddress() != null) {
            newAccountsMap.setWhiteRemoteAddress(plainAccessConfig.getWhiteRemoteAddress());
        }
        if (!StringUtils.isEmpty(String.valueOf(plainAccessConfig.isAdmin()))) {
            newAccountsMap.setAdmin(plainAccessConfig.isAdmin());
        }
        if (!StringUtils.isEmpty(plainAccessConfig.getDefaultTopicPerm())) {
            newAccountsMap.setDefaultTopicPerm(plainAccessConfig.getDefaultTopicPerm());
        }
        if (!StringUtils.isEmpty(plainAccessConfig.getDefaultGroupPerm())) {
            newAccountsMap.setDefaultGroupPerm(plainAccessConfig.getDefaultGroupPerm());
        }
        if (plainAccessConfig.getTopicPerms() != null) {
            newAccountsMap.setTopicPerms(plainAccessConfig.getTopicPerms());
        }
        if (plainAccessConfig.getGroupPerms() != null) {
            newAccountsMap.setGroupPerms(plainAccessConfig.getGroupPerms());
        }
        return newAccountsMap;
    }

    public boolean deleteAccessConfig(String accessKey) {
        if (StringUtils.isEmpty(accessKey)) {
            log.error("Parameter value accessKey is null or empty String,Please check your parameter");
            return false;
        }
        if (accessKeyTable.containsKey(accessKey)) {
            String aclFileName = accessKeyTable.get(accessKey);
            PlainAccessData aclAccessConfigData = AclUtils.getYamlDataObject(aclFileName, PlainAccessData.class);
            if (aclAccessConfigData == null) {
                log.warn("No data found in {} when deleting access config of {}", aclFileName, accessKey);
                return true;
            }
            List<PlainAccessConfig> accounts = aclAccessConfigData.getAccounts();
            Iterator<PlainAccessConfig> itemIterator = accounts.iterator();
            while (itemIterator.hasNext()) {
                if (itemIterator.next().getAccessKey().equals(accessKey)) {
                    itemIterator.remove();
                    accessKeyTable.remove(accessKey);
                    aclAccessConfigData.setAccounts(accounts);
                    return AclUtils.writeDataObject(aclFileName, updateAclConfigFileVersion(aclFileName, aclAccessConfigData));
                }
            }
        }
        return false;
    }

    public boolean updateGlobalWhiteAddrsConfig(List<String> globalWhiteAddrsList) {
        return this.updateGlobalWhiteAddrsConfig(globalWhiteAddrsList, this.defaultAclFile);
    }

    public boolean updateGlobalWhiteAddrsConfig(List<String> globalWhiteAddrsList, String fileName) {
        if (fileName == null || fileName.isEmpty()) {
            fileName = this.defaultAclFile;
        }
        if (globalWhiteAddrsList == null) {
            log.error("Parameter value globalWhiteAddrsList is null,Please check your parameter");
            return false;
        }
        File file = new File(fileName);
        if (!file.exists() || file.isDirectory()) {
            log.error("Parameter value " + fileName + " is not exist or is a directory, please check your parameter");
            return false;
        }
        if (!file.getAbsolutePath().startsWith(fileHome)) {
            log.error("Parameter value " + fileName + " is not in the directory rocketmq.home.dir " + fileHome);
            return false;
        }
        if (!fileName.endsWith(".yml") && fileName.endsWith(".yaml")) {
            log.error("Parameter value " + fileName + " is not a ACL configuration file");
            return false;
        }
        PlainAccessData aclAccessConfigMap = AclUtils.getYamlDataObject(fileName, PlainAccessData.class);
        if (aclAccessConfigMap == null) {
            aclAccessConfigMap = new PlainAccessData();
            log.info("No data in {}, create a new aclAccessConfigMap", fileName);
        }
        aclAccessConfigMap.setGlobalWhiteRemoteAddresses(new ArrayList<>(globalWhiteAddrsList));
        return AclUtils.writeDataObject(fileName, updateAclConfigFileVersion(fileName, aclAccessConfigMap));
    }

    public AclConfig getAllAclConfig() {
        AclConfig aclConfig = new AclConfig();
        List<PlainAccessConfig> configs = new ArrayList<>();
        List<String> whiteAddrs = new ArrayList<>();
        Set<String> accessKeySets = new HashSet<>();
        for (String path : fileList) {
            PlainAccessData plainAclConfData = AclUtils.getYamlDataObject(path, PlainAccessData.class);
            if (plainAclConfData == null) {
                continue;
            }
            List<String> globalWhiteAddrs = plainAclConfData.getGlobalWhiteRemoteAddresses();
            if (globalWhiteAddrs != null && !globalWhiteAddrs.isEmpty()) {
                whiteAddrs.addAll(globalWhiteAddrs);
            }
            List<PlainAccessConfig> plainAccessConfigs = plainAclConfData.getAccounts();
            if (plainAccessConfigs != null && !plainAccessConfigs.isEmpty()) {
                for (PlainAccessConfig accessConfig : plainAccessConfigs) {
                    if (!accessKeySets.contains(accessConfig.getAccessKey())) {
                        accessKeySets.add(accessConfig.getAccessKey());
                        PlainAccessConfig plainAccessConfig = new PlainAccessConfig();
                        plainAccessConfig.setGroupPerms(accessConfig.getGroupPerms());
                        plainAccessConfig.setDefaultTopicPerm(accessConfig.getDefaultTopicPerm());
                        plainAccessConfig.setDefaultGroupPerm(accessConfig.getDefaultGroupPerm());
                        plainAccessConfig.setAccessKey(accessConfig.getAccessKey());
                        plainAccessConfig.setSecretKey(accessConfig.getSecretKey());
                        plainAccessConfig.setAdmin(accessConfig.isAdmin());
                        plainAccessConfig.setTopicPerms(accessConfig.getTopicPerms());
                        plainAccessConfig.setWhiteRemoteAddress(accessConfig.getWhiteRemoteAddress());
                        configs.add(plainAccessConfig);
                    }
                }
            }
        }
        aclConfig.setPlainAccessConfigs(configs);
        aclConfig.setGlobalWhiteAddrs(whiteAddrs);
        return aclConfig;
    }

    private void watch() {
        try {
            AclFileWatchService aclFileWatchService = new AclFileWatchService(defaultAclDir, defaultAclFile, new AclFileWatchService.Listener() {
                @Override
                public void onFileChanged(String aclFileName) {
                    load(aclFileName);
                }

                @Override
                public void onFileNumChanged(String path) {
                    load();
                }
            });
            aclFileWatchService.start();
            log.info("Succeed to start AclFileWatchService");
            this.isWatchStart = true;
        } catch (Exception e) {
            log.error("Failed to start AclWatcherService", e);
        }

    }

    void checkPerm(PlainAccessResource needCheckedAccess, PlainAccessResource ownedAccess) {
        permissionChecker.check(needCheckedAccess, ownedAccess);
    }

    public void checkPlainAccessConfig(PlainAccessConfig plainAccessConfig) throws AclException {
        if (plainAccessConfig.getAccessKey() == null || plainAccessConfig.getSecretKey() == null || plainAccessConfig.getAccessKey().length() <= AclConstants.ACCESS_KEY_MIN_LENGTH || plainAccessConfig.getSecretKey().length() <= AclConstants.SECRET_KEY_MIN_LENGTH) {
            throw new AclException(String.format("The accessKey=%s and secretKey=%s cannot be null and length should longer than 6", plainAccessConfig.getAccessKey(), plainAccessConfig.getSecretKey()));
        }
    }

    public PlainAccessResource buildPlainAccessResource(PlainAccessConfig plainAccessConfig) throws AclException {
        checkPlainAccessConfig(plainAccessConfig);
        return PlainAccessResource.build(plainAccessConfig, remoteAddressStrategyFactory.getRemoteAddressStrategy(plainAccessConfig.getWhiteRemoteAddress()));
    }

    public void validate(PlainAccessResource plainAccessResource) {
        for (RemoteAddressStrategy remoteAddressStrategy : globalWhiteRemoteAddressStrategy) {
            if (remoteAddressStrategy.match(plainAccessResource)) {
                return;
            }
        }
        if (plainAccessResource.getAccessKey() == null) {
            throw new AclException("No accessKey is configured");
        }
        if (!accessKeyTable.containsKey(plainAccessResource.getAccessKey())) {
            throw new AclException(String.format("No acl config for %s", plainAccessResource.getAccessKey()));
        }
        String aclFileName = accessKeyTable.get(plainAccessResource.getAccessKey());
        PlainAccessResource ownedAccess = aclPlainAccessResourceMap.getOrDefault(aclFileName, new HashMap<>()).get(plainAccessResource.getAccessKey());
        if (ownedAccess == null) {
            throw new AclException(String.format("No PlainAccessResource for accessKey=%s", plainAccessResource.getAccessKey()));
        }
        if (ownedAccess.getRemoteAddressStrategy().match(plainAccessResource)) {
            return;
        }
        String signature = AclUtils.calSignature(plainAccessResource.getContent(), ownedAccess.getSecretKey());
        if (plainAccessResource.getSignature() == null || !MessageDigest.isEqual(signature.getBytes(AclSigner.DEFAULT_CHARSET), plainAccessResource.getSignature().getBytes(AclSigner.DEFAULT_CHARSET))) {
            throw new AclException(String.format("Check signature failed for accessKey=%s", plainAccessResource.getAccessKey()));
        }
        Map<String, Byte> resourcePermMap = plainAccessResource.getResourcePermMap();
        if (resourcePermMap != null) {
            Byte permission = resourcePermMap.get(TopicValidator.RMQ_SYS_TRACE_TOPIC);
            if (permission != null && permission == Permission.PUB) {
                return;
            }
        }
        checkPerm(plainAccessResource, ownedAccess);
    }

}
