package org.apache.rocketmq.acl.plain;

public interface RemoteAddressStrategy {

    boolean match(PlainAccessResource plainAccessResource);

}
