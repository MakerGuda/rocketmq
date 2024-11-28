package org.apache.rocketmq.remoting.protocol.route;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class QueueData implements Comparable<QueueData> {

    /**
     * 当前队列所在broker名称
     */
    private String brokerName;

    /**
     * 可读队列数量
     */
    private int readQueueNums;

    /**
     * 可写队列数量
     */
    private int writeQueueNums;

    private int perm;

    private int topicSysFlag;

    public QueueData() {

    }

    public QueueData(QueueData queueData) {
        this.brokerName = queueData.brokerName;
        this.readQueueNums = queueData.readQueueNums;
        this.writeQueueNums = queueData.writeQueueNums;
        this.perm = queueData.perm;
        this.topicSysFlag = queueData.topicSysFlag;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((brokerName == null) ? 0 : brokerName.hashCode());
        result = prime * result + perm;
        result = prime * result + readQueueNums;
        result = prime * result + writeQueueNums;
        result = prime * result + topicSysFlag;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        QueueData other = (QueueData) obj;
        if (brokerName == null) {
            if (other.brokerName != null)
                return false;
        } else if (!brokerName.equals(other.brokerName))
            return false;
        if (perm != other.perm)
            return false;
        if (readQueueNums != other.readQueueNums)
            return false;
        if (writeQueueNums != other.writeQueueNums)
            return false;
        return topicSysFlag == other.topicSysFlag;
    }

    @Override
    public int compareTo(QueueData o) {
        return this.brokerName.compareTo(o.getBrokerName());
    }

}
