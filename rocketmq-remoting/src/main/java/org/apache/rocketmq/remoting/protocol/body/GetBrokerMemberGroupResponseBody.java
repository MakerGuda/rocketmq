package org.apache.rocketmq.remoting.protocol.body;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

@Getter
@Setter
public class GetBrokerMemberGroupResponseBody extends RemotingSerializable {

    private BrokerMemberGroup brokerMemberGroup;

}
