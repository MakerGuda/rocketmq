package org.apache.rocketmq.remoting.protocol.body;

import com.google.common.base.Objects;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Getter
@Setter
public class ClusterInfo extends RemotingSerializable {

    /**
     * key:brokerName
     */
    private Map<String, BrokerData> brokerAddrTable;

    /**
     * key: clusterName value: 集群下所有的broker名称
     */
    private Map<String, Set<String>> clusterAddrTable;

    public String[] retrieveAllAddrByCluster(String cluster) {
        List<String> addrs = new ArrayList<>();
        if (clusterAddrTable.containsKey(cluster)) {
            Set<String> brokerNames = clusterAddrTable.get(cluster);
            for (String brokerName : brokerNames) {
                BrokerData brokerData = brokerAddrTable.get(brokerName);
                if (null != brokerData) {
                    addrs.addAll(brokerData.getBrokerAddrs().values());
                }
            }
        }
        return addrs.toArray(new String[] {});
    }

    public String[] retrieveAllClusterNames() {
        return clusterAddrTable.keySet().toArray(new String[] {});
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ClusterInfo info = (ClusterInfo) o;
        return Objects.equal(brokerAddrTable, info.brokerAddrTable) && Objects.equal(clusterAddrTable, info.clusterAddrTable);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(brokerAddrTable, clusterAddrTable);
    }

}
