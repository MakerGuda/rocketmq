package org.apache.rocketmq.remoting.protocol.namesrv;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.protocol.body.KVTable;

@Getter
@Setter
public class RegisterBrokerResult {

    private String haServerAddr;

    private String masterAddr;

    private KVTable kvTable;

}
