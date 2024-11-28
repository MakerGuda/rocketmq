package org.apache.rocketmq.namesrv.kvconfig;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.namesrv.NameSrvController;
import org.apache.rocketmq.remoting.protocol.body.KVTable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
@AllArgsConstructor
public class KVConfigManager {

    private final NameSrvController namesrvController;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * key: namespace
     */
    private final HashMap<String, HashMap<String, String>> configTable = new HashMap<>();

    /**
     * 从字典配置读取文件并放入到内存字典中
     */
    public void load() {
        String content = null;
        try {
            content = MixAll.file2String(this.namesrvController.getNamesrvConfig().getKvConfigPath());
        } catch (IOException e) {
            log.warn("Load KV config table exception", e);
        }
        if (content != null) {
            KVConfigSerializeWrapper kvConfigSerializeWrapper = KVConfigSerializeWrapper.fromJson(content, KVConfigSerializeWrapper.class);
            if (null != kvConfigSerializeWrapper) {
                this.configTable.putAll(kvConfigSerializeWrapper.getConfigTable());
                log.info("load KV config table OK");
            }
        }
    }

    /**
     * 更新配置文件字典
     */
    public void putKVConfig(final String namespace, final String key, final String value) {
        try {
            this.lock.writeLock().lockInterruptibly();
            try {
                HashMap<String, String> kvTable = this.configTable.get(namespace);
                if (null == kvTable) {
                    kvTable = new HashMap<>();
                    this.configTable.put(namespace, kvTable);
                    log.info("putKVConfig create new Namespace {}", namespace);
                }
                final String prev = kvTable.put(key, value);
                if (null != prev) {
                    log.info("putKVConfig update config item, Namespace: {} Key: {} Value: {}", namespace, key, value);
                } else {
                    log.info("putKVConfig create new config item, Namespace: {} Key: {} Value: {}", namespace, key, value);
                }
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("putKVConfig InterruptedException", e);
        }
        this.persist();
    }

    /**
     * 将字典持久化到磁盘
     */
    public void persist() {
        try {
            this.lock.readLock().lockInterruptibly();
            try {
                KVConfigSerializeWrapper kvConfigSerializeWrapper = new KVConfigSerializeWrapper();
                kvConfigSerializeWrapper.setConfigTable(this.configTable);
                String content = kvConfigSerializeWrapper.toJson();
                if (null != content) {
                    MixAll.string2File(content, this.namesrvController.getNamesrvConfig().getKvConfigPath());
                }
            } catch (IOException e) {
                log.error("persist kv config Exception, {}", this.namesrvController.getNamesrvConfig().getKvConfigPath(), e);
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("persist InterruptedException", e);
        }
    }

    /**
     * 删除namespace下的制定key-value
     */
    public void deleteKVConfig(final String namespace, final String key) {
        try {
            this.lock.writeLock().lockInterruptibly();
            try {
                HashMap<String, String> kvTable = this.configTable.get(namespace);
                if (null != kvTable) {
                    String value = kvTable.remove(key);
                    log.info("deleteKVConfig delete a config item, Namespace: {} Key: {} Value: {}", namespace, key, value);
                }
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("deleteKVConfig InterruptedException", e);
        }
        this.persist();
    }

    /**
     * 获取namespace下的所有字典
     */
    public byte[] getKVListByNamespace(final String namespace) {
        try {
            this.lock.readLock().lockInterruptibly();
            try {
                HashMap<String, String> kvTable = this.configTable.get(namespace);
                if (null != kvTable) {
                    KVTable table = new KVTable();
                    table.setTable(kvTable);
                    return table.encode();
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getKVListByNamespace InterruptedException", e);
        }
        return null;
    }

    /**
     * 从配置字典中读取值
     */
    public String getKVConfig(final String namespace, final String key) {
        try {
            this.lock.readLock().lockInterruptibly();
            try {
                HashMap<String, String> kvTable = this.configTable.get(namespace);
                if (null != kvTable) {
                    return kvTable.get(key);
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getKVConfig InterruptedException", e);
        }
        return null;
    }

    /**
     * 打印所有的字典
     */
    public void printAllPeriodically() {
        try {
            this.lock.readLock().lockInterruptibly();
            try {
                log.info("--------------------------------------------------------");
                {
                    log.info("configTable SIZE: {}", this.configTable.size());
                    for (Entry<String, HashMap<String, String>> next : this.configTable.entrySet()) {
                        for (Entry<String, String> nextSub : next.getValue().entrySet()) {
                            log.info("configTable NS: {} Key: {} Value: {}", next.getKey(), nextSub.getKey(), nextSub.getValue());
                        }
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("printAllPeriodically InterruptedException", e);
        }
    }

}