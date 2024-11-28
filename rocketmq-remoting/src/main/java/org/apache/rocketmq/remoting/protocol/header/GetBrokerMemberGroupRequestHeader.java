package org.apache.rocketmq.remoting.protocol.header;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.action.RocketMQAction;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.common.resource.RocketMQResource;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RequestCode;

@Getter
@Setter
@RocketMQAction(value = RequestCode.GET_BROKER_MEMBER_GROUP, action = Action.GET)
public class GetBrokerMemberGroupRequestHeader implements CommandCustomHeader {

    @CFNotNull
    @RocketMQResource(ResourceType.CLUSTER)
    private String clusterName;

    @CFNotNull
    private String brokerName;

    @Override
    public void checkFields() throws RemotingCommandException {

    }

}
