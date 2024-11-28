package org.apache.rocketmq.remoting.protocol.body;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Getter
@Setter
public class TopicList extends RemotingSerializable {

    private Set<String> topicList = ConcurrentHashMap.newKeySet();

    private String brokerAddr;

}
