package org.apache.rocketmq.acl.common;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.apache.rocketmq.acl.common.SessionCredentials.*;

@Getter
@Setter
public class AclClientRPCHook implements RPCHook {

    private final SessionCredentials sessionCredentials;

    public AclClientRPCHook(SessionCredentials sessionCredentials) {
        this.sessionCredentials = sessionCredentials;
    }

    @Override
    public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
        request.addExtField(ACCESS_KEY, sessionCredentials.getAccessKey());
        if (sessionCredentials.getSecurityToken() != null) {
            request.addExtField(SECURITY_TOKEN, sessionCredentials.getSecurityToken());
        }
        byte[] total = AclUtils.combineRequestContent(request, parseRequestContent(request));
        String signature = AclUtils.calSignature(total, sessionCredentials.getSecretKey());
        request.addExtField(SIGNATURE, signature);
    }

    @Override
    public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {

    }

    protected SortedMap<String, String> parseRequestContent(RemotingCommand request) {
        request.makeCustomHeaderToNet();
        Map<String, String> extFields = request.getExtFields();
        return new TreeMap<>(extFields);
    }

}
