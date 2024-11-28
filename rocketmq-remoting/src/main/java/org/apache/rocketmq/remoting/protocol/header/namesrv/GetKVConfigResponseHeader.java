package org.apache.rocketmq.remoting.protocol.header.namesrv;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

@Getter
@Setter
public class GetKVConfigResponseHeader implements CommandCustomHeader {

    @CFNullable
    private String value;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

}
