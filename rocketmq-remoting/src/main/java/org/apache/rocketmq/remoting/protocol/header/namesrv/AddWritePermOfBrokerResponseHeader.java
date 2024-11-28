package org.apache.rocketmq.remoting.protocol.header.namesrv;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

@Getter
@Setter
public class AddWritePermOfBrokerResponseHeader implements CommandCustomHeader {

    @CFNotNull
    private Integer addTopicCount;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

}
