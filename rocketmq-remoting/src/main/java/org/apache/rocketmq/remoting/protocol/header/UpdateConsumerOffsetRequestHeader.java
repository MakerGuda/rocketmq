package org.apache.rocketmq.remoting.protocol.header;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.action.RocketMQAction;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.common.resource.RocketMQResource;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.rpc.TopicQueueRequestHeader;

@Getter
@Setter
@RocketMQAction(value = RequestCode.UPDATE_CONSUMER_OFFSET, action = Action.SUB)
public class UpdateConsumerOffsetRequestHeader extends TopicQueueRequestHeader {

    @CFNotNull
    @RocketMQResource(ResourceType.GROUP)
    private String consumerGroup;

    @CFNotNull
    @RocketMQResource(ResourceType.TOPIC)
    private String topic;

    @CFNotNull
    private Integer queueId;

    @CFNotNull
    private Long commitOffset;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

}
