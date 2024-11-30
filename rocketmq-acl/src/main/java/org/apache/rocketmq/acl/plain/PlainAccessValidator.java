package org.apache.rocketmq.acl.plain;

import com.google.protobuf.GeneratedMessageV3;
import org.apache.rocketmq.acl.AccessResource;
import org.apache.rocketmq.acl.AccessValidator;
import org.apache.rocketmq.acl.common.AuthenticationHeader;
import org.apache.rocketmq.common.AclConfig;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.List;
import java.util.Map;

public class PlainAccessValidator implements AccessValidator {

    private final PlainPermissionManager aclPlugEngine;

    public PlainAccessValidator() {
        aclPlugEngine = new PlainPermissionManager();
    }

    @Override
    public AccessResource parse(RemotingCommand request, String remoteAddr) {
        return PlainAccessResource.parse(request, remoteAddr);
    }

    @Override
    public AccessResource parse(GeneratedMessageV3 messageV3, AuthenticationHeader header) {
        return PlainAccessResource.parse(messageV3, header);
    }

    @Override
    public void validate(AccessResource accessResource) {
        aclPlugEngine.validate((PlainAccessResource) accessResource);
    }

    @Override
    public boolean updateAccessConfig(PlainAccessConfig plainAccessConfig) {
        return aclPlugEngine.updateAccessConfig(plainAccessConfig);
    }

    @Override
    public boolean deleteAccessConfig(String accessKey) {
        return aclPlugEngine.deleteAccessConfig(accessKey);
    }

    @Override
    public String getAclConfigVersion() {
        return aclPlugEngine.getAclConfigDataVersion();
    }

    @Override
    public boolean updateGlobalWhiteAddrsConfig(List<String> globalWhiteAddrsList) {
        return aclPlugEngine.updateGlobalWhiteAddrsConfig(globalWhiteAddrsList);
    }

    @Override
    public boolean updateGlobalWhiteAddrsConfig(List<String> globalWhiteAddrsList, String aclFileFullPath) {
        return aclPlugEngine.updateGlobalWhiteAddrsConfig(globalWhiteAddrsList, aclFileFullPath);
    }

    @Override
    public AclConfig getAllAclConfig() {
        return aclPlugEngine.getAllAclConfig();
    }

    @Override
    public Map<String, DataVersion> getAllAclConfigVersion() {
        return aclPlugEngine.getDataVersionMap();
    }

}
