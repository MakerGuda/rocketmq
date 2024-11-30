package org.apache.rocketmq.acl.plain;

import org.apache.rocketmq.acl.AccessResource;
import org.apache.rocketmq.acl.PermissionChecker;
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.Permission;

import java.util.Map;

public class PlainPermissionChecker implements PermissionChecker {

    public void check(AccessResource checkedAccess, AccessResource ownedAccess) {
        PlainAccessResource checkedPlainAccess = (PlainAccessResource) checkedAccess;
        PlainAccessResource ownedPlainAccess = (PlainAccessResource) ownedAccess;
        if (ownedPlainAccess.isAdmin()) {
            return;
        }
        if (Permission.needAdminPerm(checkedPlainAccess.getRequestCode())) {
            throw new AclException(String.format("Need admin permission for request code=%d, but accessKey=%s is not", checkedPlainAccess.getRequestCode(), ownedPlainAccess.getAccessKey()));
        }
        Map<String, Byte> needCheckedPermMap = checkedPlainAccess.getResourcePermMap();
        Map<String, Byte> ownedPermMap = ownedPlainAccess.getResourcePermMap();
        if (needCheckedPermMap == null) {
            return;
        }
        for (Map.Entry<String, Byte> needCheckedEntry : needCheckedPermMap.entrySet()) {
            String resource = needCheckedEntry.getKey();
            Byte neededPerm = needCheckedEntry.getValue();
            boolean isGroup = PlainAccessResource.isRetryTopic(resource);
            if (ownedPermMap == null || !ownedPermMap.containsKey(resource)) {
                byte ownedPerm = isGroup ? ownedPlainAccess.getDefaultGroupPerm() : ownedPlainAccess.getDefaultTopicPerm();
                if (!Permission.checkPermission(neededPerm, ownedPerm)) {
                    throw new AclException(String.format("No default permission for %s", PlainAccessResource.printStr(resource, isGroup)));
                }
                continue;
            }
            if (!Permission.checkPermission(neededPerm, ownedPermMap.get(resource))) {
                throw new AclException(String.format("No permission for %s", PlainAccessResource.printStr(resource, isGroup)));
            }
        }
    }

}
