package org.apache.rocketmq.acl.plain;

import apache.rocketmq.v2.*;
import com.google.protobuf.GeneratedMessageV3;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.rocketmq.acl.AccessResource;
import org.apache.rocketmq.acl.common.Permission;
import org.apache.rocketmq.acl.common.*;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UnregisterClientRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.remoting.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

import java.nio.charset.StandardCharsets;
import java.util.*;

@Getter
@Setter
public class PlainAccessResource implements AccessResource {

    private String accessKey;

    private String secretKey;

    private String whiteRemoteAddress;

    private boolean admin;

    private byte defaultTopicPerm = 1;

    private byte defaultGroupPerm = 1;

    private Map<String, Byte> resourcePermMap;

    private RemoteAddressStrategy remoteAddressStrategy;

    private int requestCode;

    private byte[] content;

    private String signature;

    private String secretToken;

    private String recognition;

    public PlainAccessResource() {
    }

    public static PlainAccessResource parse(RemotingCommand request, String remoteAddr) {
        PlainAccessResource accessResource = new PlainAccessResource();
        if (remoteAddr != null && remoteAddr.contains(":")) {
            accessResource.setWhiteRemoteAddress(remoteAddr.substring(0, remoteAddr.lastIndexOf(':')));
        } else {
            accessResource.setWhiteRemoteAddress(remoteAddr);
        }
        accessResource.setRequestCode(request.getCode());
        if (request.getExtFields() == null) {
            return accessResource;
        }
        accessResource.setAccessKey(request.getExtFields().get(SessionCredentials.ACCESS_KEY));
        accessResource.setSignature(request.getExtFields().get(SessionCredentials.SIGNATURE));
        accessResource.setSecretToken(request.getExtFields().get(SessionCredentials.SECURITY_TOKEN));
        try {
            switch (request.getCode()) {
                case RequestCode.SEND_MESSAGE:
                    final String topic = request.getExtFields().get("topic");
                    accessResource.addResourceAndPerm(topic, PlainAccessResource.isRetryTopic(topic) ? Permission.SUB : Permission.PUB);
                    break;
                case RequestCode.SEND_MESSAGE_V2:
                case RequestCode.SEND_BATCH_MESSAGE:
                    final String topicV2 = request.getExtFields().get("b");
                    accessResource.addResourceAndPerm(topicV2, PlainAccessResource.isRetryTopic(topicV2) ? Permission.SUB : Permission.PUB);
                    break;
                case RequestCode.CONSUMER_SEND_MSG_BACK:
                    accessResource.addResourceAndPerm(getRetryTopic(request.getExtFields().get("group")), Permission.SUB);
                    break;
                case RequestCode.PULL_MESSAGE:
                    accessResource.addResourceAndPerm(request.getExtFields().get("topic"), Permission.SUB);
                    accessResource.addResourceAndPerm(getRetryTopic(request.getExtFields().get("consumerGroup")), Permission.SUB);
                    break;
                case RequestCode.QUERY_MESSAGE:
                    accessResource.addResourceAndPerm(request.getExtFields().get("topic"), Permission.SUB);
                    break;
                case RequestCode.HEART_BEAT:
                    HeartbeatData heartbeatData = HeartbeatData.decode(request.getBody(), HeartbeatData.class);
                    for (ConsumerData data : heartbeatData.getConsumerDataSet()) {
                        accessResource.addResourceAndPerm(getRetryTopic(data.getGroupName()), Permission.SUB);
                        for (SubscriptionData subscriptionData : data.getSubscriptionDataSet()) {
                            accessResource.addResourceAndPerm(subscriptionData.getTopic(), Permission.SUB);
                        }
                    }
                    break;
                case RequestCode.UNREGISTER_CLIENT:
                    final UnregisterClientRequestHeader unregisterClientRequestHeader = request.decodeCommandCustomHeader(UnregisterClientRequestHeader.class);
                    accessResource.addResourceAndPerm(getRetryTopic(unregisterClientRequestHeader.getConsumerGroup()), Permission.SUB);
                    break;
                case RequestCode.GET_CONSUMER_LIST_BY_GROUP:
                    final GetConsumerListByGroupRequestHeader getConsumerListByGroupRequestHeader = request.decodeCommandCustomHeader(GetConsumerListByGroupRequestHeader.class);
                    accessResource.addResourceAndPerm(getRetryTopic(getConsumerListByGroupRequestHeader.getConsumerGroup()), Permission.SUB);
                    break;
                case RequestCode.UPDATE_CONSUMER_OFFSET:
                    final UpdateConsumerOffsetRequestHeader updateConsumerOffsetRequestHeader = request.decodeCommandCustomHeader(UpdateConsumerOffsetRequestHeader.class);
                    accessResource.addResourceAndPerm(getRetryTopic(updateConsumerOffsetRequestHeader.getConsumerGroup()), Permission.SUB);
                    accessResource.addResourceAndPerm(updateConsumerOffsetRequestHeader.getTopic(), Permission.SUB);
                    break;
                default:
                    break;

            }
        } catch (Throwable t) {
            throw new AclException(t.getMessage(), t);
        }

        SortedMap<String, String> map = new TreeMap<>();
        for (Map.Entry<String, String> entry : request.getExtFields().entrySet()) {
            if (request.getVersion() <= MQVersion.Version.V4_9_3.ordinal() && MixAll.UNIQUE_MSG_QUERY_FLAG.equals(entry.getKey())) {
                continue;
            }
            if (!SessionCredentials.SIGNATURE.equals(entry.getKey())) {
                map.put(entry.getKey(), entry.getValue());
            }
        }
        accessResource.setContent(AclUtils.combineRequestContent(request, map));
        return accessResource;
    }

    public static PlainAccessResource parse(GeneratedMessageV3 messageV3, AuthenticationHeader header) {
        PlainAccessResource accessResource = new PlainAccessResource();
        String remoteAddress = header.getRemoteAddress();
        if (remoteAddress != null && remoteAddress.contains(":")) {
            accessResource.setWhiteRemoteAddress(RemotingHelper.parseHostFromAddress(remoteAddress));
        } else {
            accessResource.setWhiteRemoteAddress(remoteAddress);
        }
        try {
            AuthorizationHeader authorizationHeader = new AuthorizationHeader(header.getAuthorization());
            accessResource.setAccessKey(authorizationHeader.getAccessKey());
            accessResource.setSignature(authorizationHeader.getSignature());
        } catch (DecoderException e) {
            throw new AclException(e.getMessage(), e);
        }
        accessResource.setSecretToken(header.getSessionToken());
        accessResource.setRequestCode(header.getRequestCode());
        accessResource.setContent(header.getDatetime().getBytes(StandardCharsets.UTF_8));
        try {
            String rpcFullName = messageV3.getDescriptorForType().getFullName();
            if (HeartbeatRequest.getDescriptor().getFullName().equals(rpcFullName)) {
                HeartbeatRequest request = (HeartbeatRequest) messageV3;
                if (ClientType.PUSH_CONSUMER.equals(request.getClientType()) || ClientType.SIMPLE_CONSUMER.equals(request.getClientType())) {
                    if (!request.hasGroup()) {
                        throw new AclException("Consumer heartbeat doesn't have group");
                    } else {
                        accessResource.addGroupResourceAndPerm(request.getGroup());
                    }
                }
            } else if (SendMessageRequest.getDescriptor().getFullName().equals(rpcFullName)) {
                SendMessageRequest request = (SendMessageRequest) messageV3;
                if (request.getMessagesCount() <= 0) {
                    throw new AclException("SendMessageRequest, messageCount is zero", ResponseCode.MESSAGE_ILLEGAL);
                }
                Resource topic = request.getMessages(0).getTopic();
                for (Message message : request.getMessagesList()) {
                    if (!message.getTopic().equals(topic)) {
                        throw new AclException("SendMessageRequest, messages' topic is not consistent", ResponseCode.MESSAGE_ILLEGAL);
                    }
                }
                accessResource.addResourceAndPerm(topic, Permission.PUB);
            } else if (ReceiveMessageRequest.getDescriptor().getFullName().equals(rpcFullName)) {
                ReceiveMessageRequest request = (ReceiveMessageRequest) messageV3;
                accessResource.addGroupResourceAndPerm(request.getGroup());
                accessResource.addResourceAndPerm(request.getMessageQueue().getTopic(), Permission.SUB);
            } else if (AckMessageRequest.getDescriptor().getFullName().equals(rpcFullName)) {
                AckMessageRequest request = (AckMessageRequest) messageV3;
                accessResource.addGroupResourceAndPerm(request.getGroup());
                accessResource.addResourceAndPerm(request.getTopic(), Permission.SUB);
            } else if (ForwardMessageToDeadLetterQueueRequest.getDescriptor().getFullName().equals(rpcFullName)) {
                ForwardMessageToDeadLetterQueueRequest request = (ForwardMessageToDeadLetterQueueRequest) messageV3;
                accessResource.addGroupResourceAndPerm(request.getGroup());
                accessResource.addResourceAndPerm(request.getTopic(), Permission.SUB);
            } else if (EndTransactionRequest.getDescriptor().getFullName().equals(rpcFullName)) {
                EndTransactionRequest request = (EndTransactionRequest) messageV3;
                accessResource.addResourceAndPerm(request.getTopic(), Permission.PUB);
            } else if (TelemetryCommand.getDescriptor().getFullName().equals(rpcFullName)) {
                TelemetryCommand command = (TelemetryCommand) messageV3;
                if (command.getCommandCase() == TelemetryCommand.CommandCase.SETTINGS) {
                    if (command.getSettings().hasPublishing()) {
                        List<Resource> topicList = command.getSettings().getPublishing().getTopicsList();
                        for (Resource topic : topicList) {
                            accessResource.addResourceAndPerm(topic, Permission.PUB);
                        }
                    }
                    if (command.getSettings().hasSubscription()) {
                        Subscription subscription = command.getSettings().getSubscription();
                        accessResource.addGroupResourceAndPerm(subscription.getGroup());
                        for (SubscriptionEntry entry : subscription.getSubscriptionsList()) {
                            accessResource.addResourceAndPerm(entry.getTopic(), Permission.SUB);
                        }
                    }
                    if (!command.getSettings().hasPublishing() && !command.getSettings().hasSubscription()) {
                        throw new AclException("settings command doesn't have publishing or subscription");
                    }
                }
            } else if (NotifyClientTerminationRequest.getDescriptor().getFullName().equals(rpcFullName)) {
                NotifyClientTerminationRequest request = (NotifyClientTerminationRequest) messageV3;
                if (StringUtils.isNotBlank(request.getGroup().getName())) {
                    accessResource.addGroupResourceAndPerm(request.getGroup());
                }
            } else if (QueryRouteRequest.getDescriptor().getFullName().equals(rpcFullName)) {
                QueryRouteRequest request = (QueryRouteRequest) messageV3;
                accessResource.addResourceAndPerm(request.getTopic(), Permission.ANY);
            } else if (QueryAssignmentRequest.getDescriptor().getFullName().equals(rpcFullName)) {
                QueryAssignmentRequest request = (QueryAssignmentRequest) messageV3;
                accessResource.addGroupResourceAndPerm(request.getGroup());
                accessResource.addResourceAndPerm(request.getTopic(), Permission.SUB);
            } else if (ChangeInvisibleDurationRequest.getDescriptor().getFullName().equals(rpcFullName)) {
                ChangeInvisibleDurationRequest request = (ChangeInvisibleDurationRequest) messageV3;
                accessResource.addGroupResourceAndPerm(request.getGroup());
                accessResource.addResourceAndPerm(request.getTopic(), Permission.SUB);
            }
        } catch (Throwable t) {
            throw new AclException(t.getMessage(), t);
        }
        return accessResource;
    }

    private void addResourceAndPerm(Resource resource, byte permission) {
        String resourceName = NamespaceUtil.wrapNamespace(resource.getResourceNamespace(), resource.getName());
        addResourceAndPerm(resourceName, permission);
    }

    private void addGroupResourceAndPerm(Resource resource) {
        String resourceName = NamespaceUtil.wrapNamespace(resource.getResourceNamespace(), resource.getName());
        addResourceAndPerm(getRetryTopic(resourceName), Permission.SUB);
    }

    public static PlainAccessResource build(PlainAccessConfig plainAccessConfig, RemoteAddressStrategy remoteAddressStrategy) {
        PlainAccessResource plainAccessResource = new PlainAccessResource();
        plainAccessResource.setAccessKey(plainAccessConfig.getAccessKey());
        plainAccessResource.setSecretKey(plainAccessConfig.getSecretKey());
        plainAccessResource.setWhiteRemoteAddress(plainAccessConfig.getWhiteRemoteAddress());
        plainAccessResource.setAdmin(plainAccessConfig.isAdmin());
        plainAccessResource.setDefaultGroupPerm(Permission.parsePermFromString(plainAccessConfig.getDefaultGroupPerm()));
        plainAccessResource.setDefaultTopicPerm(Permission.parsePermFromString(plainAccessConfig.getDefaultTopicPerm()));
        Permission.parseResourcePerms(plainAccessResource, false, plainAccessConfig.getGroupPerms());
        Permission.parseResourcePerms(plainAccessResource, true, plainAccessConfig.getTopicPerms());
        plainAccessResource.setRemoteAddressStrategy(remoteAddressStrategy);
        return plainAccessResource;
    }

    public static boolean isRetryTopic(String topic) {
        return null != topic && topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX);
    }

    public static String printStr(String resource, boolean isGroup) {
        if (resource == null) {
            return null;
        }
        if (isGroup) {
            return String.format("%s:%s", "group", getGroupFromRetryTopic(resource));
        } else {
            return String.format("%s:%s", "topic", resource);
        }
    }

    public static String getGroupFromRetryTopic(String retryTopic) {
        if (retryTopic == null) {
            return null;
        }
        return KeyBuilder.parseGroup(retryTopic);
    }

    public static String getRetryTopic(String group) {
        if (group == null) {
            return null;
        }
        return MixAll.getRetryTopic(group);
    }

    public void addResourceAndPerm(String resource, byte perm) {
        if (resource == null) {
            return;
        }
        if (resourcePermMap == null) {
            resourcePermMap = new HashMap<>();
        }
        resourcePermMap.put(resource, perm);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

}
