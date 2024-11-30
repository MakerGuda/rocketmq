package org.apache.rocketmq.acl.common;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.MixAll;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

@Getter
@Setter
public class SessionCredentials {

    public static final Charset CHARSET = StandardCharsets.UTF_8;

    public static final String ACCESS_KEY = "AccessKey";

    public static final String SECRET_KEY = "SecretKey";

    public static final String SIGNATURE = "Signature";

    public static final String SECURITY_TOKEN = "SecurityToken";

    public static final String KEY_FILE = System.getProperty("rocketmq.client.keyFile", System.getProperty("user.home") + File.separator + "key");

    private String accessKey;

    private String secretKey;

    private String securityToken;

    private String signature;

    public SessionCredentials() {
        String keyContent = null;
        try {
            keyContent = MixAll.file2String(KEY_FILE);
        } catch (IOException ignore) {
        }
        if (keyContent != null) {
            Properties prop = MixAll.string2Properties(keyContent);
            if (prop != null) {
                this.updateContent(prop);
            }
        }
    }

    public SessionCredentials(String accessKey, String secretKey) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
    }

    public SessionCredentials(String accessKey, String secretKey, String securityToken) {
        this(accessKey, secretKey);
        this.securityToken = securityToken;
    }

    public void updateContent(Properties prop) {
        {
            String value = prop.getProperty(ACCESS_KEY);
            if (value != null) {
                this.accessKey = value.trim();
            }
        }
        {
            String value = prop.getProperty(SECRET_KEY);
            if (value != null) {
                this.secretKey = value.trim();
            }
        }
        {
            String value = prop.getProperty(SECURITY_TOKEN);
            if (value != null) {
                this.securityToken = value.trim();
            }
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((accessKey == null) ? 0 : accessKey.hashCode());
        result = prime * result + ((secretKey == null) ? 0 : secretKey.hashCode());
        result = prime * result + ((signature == null) ? 0 : signature.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SessionCredentials other = (SessionCredentials) obj;
        if (accessKey == null) {
            if (other.accessKey != null)
                return false;
        } else if (!accessKey.equals(other.accessKey))
            return false;
        if (secretKey == null) {
            if (other.secretKey != null)
                return false;
        } else if (!secretKey.equals(other.secretKey))
            return false;
        if (signature == null) {
            return other.signature == null;
        } else return signature.equals(other.signature);
    }

}
