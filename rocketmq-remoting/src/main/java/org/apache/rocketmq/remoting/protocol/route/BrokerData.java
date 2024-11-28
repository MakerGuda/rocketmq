package org.apache.rocketmq.remoting.protocol.route;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class BrokerData implements Comparable<BrokerData> {

    /**
     * broker所在的集群名称
     */
    private String cluster;

    /**
     * broker名称
     */
    private String brokerName;

    /**
     * key: brokerId value: brokerAddr
     * brokerId = 0表示master
     */
    private HashMap<Long, String> brokerAddrs;

    private String zoneName;

    private final Random random = new Random();

    /**
     * Enable acting master or not, used for old version HA adaption,
     */
    private boolean enableActingMaster = false;

    public BrokerData(BrokerData brokerData) {
        this.cluster = brokerData.cluster;
        this.brokerName = brokerData.brokerName;
        if (brokerData.brokerAddrs != null) {
            this.brokerAddrs = new HashMap<>(brokerData.brokerAddrs);
        }
        this.zoneName = brokerData.zoneName;
        this.enableActingMaster = brokerData.enableActingMaster;
    }

    public BrokerData(String cluster, String brokerName, HashMap<Long, String> brokerAddrs) {
        this.cluster = cluster;
        this.brokerName = brokerName;
        this.brokerAddrs = brokerAddrs;
    }

    public BrokerData(String cluster, String brokerName, HashMap<Long, String> brokerAddrs,
        boolean enableActingMaster) {
        this.cluster = cluster;
        this.brokerName = brokerName;
        this.brokerAddrs = brokerAddrs;
        this.enableActingMaster = enableActingMaster;
    }

    public BrokerData(String cluster, String brokerName, HashMap<Long, String> brokerAddrs, boolean enableActingMaster,
        String zoneName) {
        this.cluster = cluster;
        this.brokerName = brokerName;
        this.brokerAddrs = brokerAddrs;
        this.enableActingMaster = enableActingMaster;
        this.zoneName = zoneName;
    }

    /**
     * 有限获取master broker地址，master找不到时，随机获取slave
     */
    public String selectBrokerAddr() {
        String masterAddress = this.brokerAddrs.get(MixAll.MASTER_ID);
        if (masterAddress == null) {
            List<String> addrs = new ArrayList<>(brokerAddrs.values());
            return addrs.get(random.nextInt(addrs.size()));
        }
        return masterAddress;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((brokerAddrs == null) ? 0 : brokerAddrs.hashCode());
        result = prime * result + ((brokerName == null) ? 0 : brokerName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        BrokerData other = (BrokerData) obj;
        if (brokerAddrs == null) {
            if (other.brokerAddrs != null) {
                return false;
            }
        } else if (!brokerAddrs.equals(other.brokerAddrs)) {
            return false;
        }
        return StringUtils.equals(brokerName, other.brokerName);
    }

    @Override
    public int compareTo(BrokerData o) {
        return this.brokerName.compareTo(o.getBrokerName());
    }

}
