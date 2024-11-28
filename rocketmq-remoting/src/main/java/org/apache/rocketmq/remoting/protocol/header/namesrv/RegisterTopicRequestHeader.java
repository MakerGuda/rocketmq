package org.apache.rocketmq.remoting.protocol.header.namesrv;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.action.RocketMQAction;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.rpc.TopicRequestHeader;

@Getter
@Setter
@RocketMQAction(value = RequestCode.REGISTER_TOPIC_IN_NAMESRV, resource = ResourceType.CLUSTER, action = Action.UPDATE)
public class RegisterTopicRequestHeader extends TopicRequestHeader {

    @CFNotNull
    private String topic;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

}
