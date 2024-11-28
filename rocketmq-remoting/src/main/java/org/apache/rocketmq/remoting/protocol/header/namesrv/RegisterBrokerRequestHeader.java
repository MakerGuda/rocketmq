package org.apache.rocketmq.remoting.protocol.header.namesrv;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.action.RocketMQAction;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.common.resource.RocketMQResource;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RequestCode;

@Getter
@Setter
@RocketMQAction(value = RequestCode.REGISTER_BROKER, resource = ResourceType.CLUSTER, action = Action.UPDATE)
public class RegisterBrokerRequestHeader implements CommandCustomHeader {

    @CFNotNull
    private String brokerName;

    @CFNotNull
    private String brokerAddr;

    @CFNotNull
    @RocketMQResource(ResourceType.CLUSTER)
    private String clusterName;

    @CFNotNull
    private String haServerAddr;

    @CFNotNull
    private Long brokerId;

    @CFNullable
    private Long heartbeatTimeoutMillis;

    @CFNullable
    private Boolean enableActingMaster;

    private boolean compressed;

    private Integer bodyCrc32 = 0;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

}
