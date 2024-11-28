package org.apache.rocketmq.remoting.protocol.header;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.action.RocketMQAction;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RequestCode;

@Getter
@Setter
@RocketMQAction(value = RequestCode.NOTIFY_MIN_BROKER_ID_CHANGE, resource = ResourceType.CLUSTER, action = Action.UPDATE)
public class NotifyMinBrokerIdChangeRequestHeader implements CommandCustomHeader {

    @CFNullable
    private Long minBrokerId;

    @CFNullable
    private String brokerName;

    @CFNullable
    private String minBrokerAddr;

    @CFNullable
    private String offlineBrokerAddr;

    @CFNullable
    private String haBrokerAddr;

    @Override
    public void checkFields() throws RemotingCommandException {

    }

}
