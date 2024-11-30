package org.apache.rocketmq.acl.plain;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.PlainAccessConfig;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Getter
@Setter
public class PlainAccessData implements Serializable {

    private static final long serialVersionUID = -7971775135605117152L;

    private List<String> globalWhiteRemoteAddresses = new ArrayList<>();

    private List<PlainAccessConfig> accounts = new ArrayList<>();

    private List<DataVersion> dataVersion = new ArrayList<>();

    @Setter
    @Getter
    public static class DataVersion implements Serializable {

        private static final long serialVersionUID = 6437361970079056954L;

        private long timestamp;

        private long counter;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DataVersion that = (DataVersion) o;
            return timestamp == that.timestamp && counter == that.counter;
        }

        @Override
        public int hashCode() {
            return Objects.hash(timestamp, counter);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PlainAccessData that = (PlainAccessData) o;
        return Objects.equals(globalWhiteRemoteAddresses, that.globalWhiteRemoteAddresses) && Objects.equals(accounts, that.accounts) && Objects.equals(dataVersion, that.dataVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(globalWhiteRemoteAddresses, accounts, dataVersion);
    }

}
