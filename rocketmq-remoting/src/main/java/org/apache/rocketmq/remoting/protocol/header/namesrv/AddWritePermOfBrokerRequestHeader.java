package org.apache.rocketmq.remoting.protocol.header.namesrv;

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
@RocketMQAction(value = RequestCode.ADD_WRITE_PERM_OF_BROKER, resource = ResourceType.CLUSTER, action = Action.UPDATE)
public class AddWritePermOfBrokerRequestHeader implements CommandCustomHeader {

    @CFNotNull
    private String brokerName;

    @Override
    public void checkFields() throws RemotingCommandException {

    }

}
