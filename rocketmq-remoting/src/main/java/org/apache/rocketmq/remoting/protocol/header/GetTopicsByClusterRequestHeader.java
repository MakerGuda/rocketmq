package org.apache.rocketmq.remoting.protocol.header;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.action.RocketMQAction;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RequestCode;

@Getter
@Setter
@RocketMQAction(value = RequestCode.GET_TOPICS_BY_CLUSTER, resource = ResourceType.TOPIC, action = Action.LIST)
public class GetTopicsByClusterRequestHeader implements CommandCustomHeader {

    @CFNotNull
    private String cluster;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

}
