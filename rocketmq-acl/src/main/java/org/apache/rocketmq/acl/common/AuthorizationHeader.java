package org.apache.rocketmq.acl.common;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;

@Getter
@Setter
public class AuthorizationHeader {

    private static final String HEADER_SEPARATOR = " ";

    private static final String CREDENTIALS_SEPARATOR = "/";

    private static final int AUTH_HEADER_KV_LENGTH = 2;

    private static final String CREDENTIAL = "Credential";

    private static final String SIGNED_HEADERS = "SignedHeaders";

    private static final String SIGNATURE = "Signature";

    private String method;

    private String accessKey;

    private String[] signedHeaders;

    private String signature;

    public AuthorizationHeader(String header) throws DecoderException {
        String[] result = header.split(HEADER_SEPARATOR, 2);
        if (result.length != 2) {
            throw new DecoderException("authorization header is incorrect");
        }
        this.method = result[0];
        String[] keyValues = result[1].split(",");
        for (String keyValue : keyValues) {
            String[] kv = keyValue.trim().split("=", 2);
            int kvLength = kv.length;
            if (kv.length != AUTH_HEADER_KV_LENGTH) {
                throw new DecoderException("authorization keyValues length is incorrect, actual length=" + kvLength);
            }
            String authItem = kv[0];
            if (CREDENTIAL.equals(authItem)) {
                String[] credential = kv[1].split(CREDENTIALS_SEPARATOR);
                int credentialActualLength = credential.length;
                if (credentialActualLength == 0) {
                    throw new DecoderException("authorization credential length is incorrect, actual length=" + credentialActualLength);
                }
                this.accessKey = credential[0];
                continue;
            }
            if (SIGNED_HEADERS.equals(authItem)) {
                this.signedHeaders = kv[1].split(";");
                continue;
            }
            if (SIGNATURE.equals(authItem)) {
                this.signature = this.hexToBase64(kv[1]);
            }
        }
    }

    public String hexToBase64(String input) throws DecoderException {
        byte[] bytes = Hex.decodeHex(input);
        return Base64.encodeBase64String(bytes);
    }

}
