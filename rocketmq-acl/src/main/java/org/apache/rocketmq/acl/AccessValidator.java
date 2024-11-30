package org.apache.rocketmq.acl;

import com.google.protobuf.GeneratedMessageV3;
import org.apache.rocketmq.acl.common.AuthenticationHeader;
import org.apache.rocketmq.common.AclConfig;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.List;
import java.util.Map;

public interface AccessValidator {

    /**
     * Parse to get the AccessResource(user, resource, needed permission)
     *
     * @param request
     * @param remoteAddr
     * @return Plain access resource result,include access key,signature and some other access attributes.
     */
    AccessResource parse(RemotingCommand request, String remoteAddr);

    /**
     * Parse to get the AccessResource from gRPC protocol
     * @param messageV3
     * @param header
     * @return Plain access resource
     */
    AccessResource parse(GeneratedMessageV3 messageV3, AuthenticationHeader header);

    /**
     * Validate the access resource.
     *
     * @param accessResource
     */
    void validate(AccessResource accessResource);

    /**
     * Update the access resource config
     *
     * @param plainAccessConfig
     * @return
     */
    boolean updateAccessConfig(PlainAccessConfig plainAccessConfig);

    /**
     * Delete the access resource config
     *
     * @return
     */
    boolean deleteAccessConfig(String accessKey);

    /**
     * Get the access resource config version information
     *
     * @return
     */
    @Deprecated
    String getAclConfigVersion();

    /**
     * Update globalWhiteRemoteAddresses in acl yaml config file
     *
     * @return
     */
    boolean updateGlobalWhiteAddrsConfig(List<String> globalWhiteAddrsList);

    boolean updateGlobalWhiteAddrsConfig(List<String> globalWhiteAddrsList, String aclFileFullPath);

    /**
     * get broker cluster acl config information
     *
     * @return
     */
    AclConfig getAllAclConfig();

    /**
     * get all access resource config version information
     *
     * @return
     */
    Map<String, DataVersion> getAllAclConfigVersion();
}
