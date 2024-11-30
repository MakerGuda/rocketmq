package org.apache.rocketmq.remoting.protocol.header;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.action.RocketMQAction;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.common.resource.RocketMQResource;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.rpc.RpcRequestHeader;

@Getter
@Setter
@RocketMQAction(value = RequestCode.UNREGISTER_CLIENT, action = {Action.PUB, Action.SUB})
public class UnregisterClientRequestHeader extends RpcRequestHeader {

    @CFNotNull
    private String clientID;

    @CFNullable
    private String producerGroup;

    @CFNullable
    @RocketMQResource(ResourceType.GROUP)
    private String consumerGroup;

    @Override
    public void checkFields() throws RemotingCommandException {

    }

}
