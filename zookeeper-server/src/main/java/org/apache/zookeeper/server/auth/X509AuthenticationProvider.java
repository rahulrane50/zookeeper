/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server.auth;

import java.security.cert.CertificateException;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.common.ClientX509Util;
import org.apache.zookeeper.common.X509Exception.KeyManagerException;
import org.apache.zookeeper.common.X509Exception.TrustManagerException;
import org.apache.zookeeper.common.X509Util;
import org.apache.zookeeper.common.ZKConfig;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.ServerCnxn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An AuthenticationProvider backed by an X509TrustManager and an X509KeyManager
 * to perform remote host certificate authentication. The default algorithm is
 * SunX509 and a JKS KeyStore. To specify the locations of the key store and
 * trust store, set the following system properties:
 * <br><code>zookeeper.ssl.keyStore.location</code>
 * <br><code>zookeeper.ssl.trustStore.location</code>
 * <br>To specify store passwords, set the following system properties:
 * <br><code>zookeeper.ssl.keyStore.password</code>
 * <br><code>zookeeper.ssl.trustStore.password</code>
 * <br>Alternatively, this can be plugged with any X509TrustManager and
 * X509KeyManager implementation.
 */
public class X509AuthenticationProvider implements AuthenticationProvider {

    static final String ZOOKEEPER_X509AUTHENTICATIONPROVIDER_SUPERUSER = "zookeeper.X509AuthenticationProvider.superUser";
    /**
     * The following System Property keys are used to extract clientId from the client cert.
     * @see #getClientId(X509Certificate)
     */
    public static final String ZOOKEEPER_X509AUTHENTICATIONPROVIDER_CLIENT_CERT_ID_TYPE = "zookeeper.X509AuthenticationProvider.clientCertIdType";
    public static final String ZOOKEEPER_X509AUTHENTICATIONPROVIDER_CLIENT_CERT_ID_SAN_MATCH_TYPE = "zookeeper.X509AuthenticationProvider.clientCertIdSanMatchType";
    // Match Regex is used to choose which entry to use
    public static final String ZOOKEEPER_X509AUTHENTICATIONPROVIDER_CLIENT_CERT_ID_SAN_MATCH_REGEX = "zookeeper.X509AuthenticationProvider.clientCertIdSanMatchRegex";
    // Extract Regex is used to construct a client ID (acl entity) to return
    public static final String ZOOKEEPER_X509AUTHENTICATIONPROVIDER_CLIENT_CERT_ID_SAN_EXTRACT_REGEX = "zookeeper.X509AuthenticationProvider.clientCertIdSanExtractRegex";
    // Specifies match group index for the extract regex (i in Matcher.group(i))
    public static final String ZOOKEEPER_X509AUTHENTICATIONPROVIDER_CLIENT_CERT_ID_SAN_EXTRACT_MATCHER_GROUP_INDEX = "zookeeper.X509AuthenticationProvider.clientCertIdSanExtractMatcherGroupIndex";

    static final Logger LOG = LoggerFactory.getLogger(X509AuthenticationProvider.class);
    private final X509TrustManager trustManager;
    private final X509KeyManager keyManager;

    /**
     * Initialize the X509AuthenticationProvider with a JKS KeyStore and JKS
     * TrustStore according to the following system properties:
     * <br><code>zookeeper.ssl.keyStore.location</code>
     * <br><code>zookeeper.ssl.trustStore.location</code>
     * <br><code>zookeeper.ssl.keyStore.password</code>
     * <br><code>zookeeper.ssl.trustStore.password</code>
     */
    public X509AuthenticationProvider() {
        ZKConfig config = new ZKConfig();
        try (X509Util x509Util = new ClientX509Util()) {
            String keyStoreLocation = config.getProperty(x509Util.getSslKeystoreLocationProperty(), "");
            String keyStorePassword = config.getProperty(x509Util.getSslKeystorePasswdProperty(), "");
            String keyStoreTypeProp = config.getProperty(x509Util.getSslKeystoreTypeProperty());

            boolean crlEnabled = Boolean.parseBoolean(config.getProperty(x509Util.getSslCrlEnabledProperty()));
            boolean ocspEnabled = Boolean.parseBoolean(config.getProperty(x509Util.getSslOcspEnabledProperty()));
            boolean hostnameVerificationEnabled = Boolean.parseBoolean(config.getProperty(x509Util.getSslHostnameVerificationEnabledProperty()));

            X509KeyManager km = null;
            X509TrustManager tm = null;
            if (keyStoreLocation.isEmpty()) {
                LOG.warn("keystore not specified for client connection");
            } else {
                try {
                    km = X509Util.createKeyManager(keyStoreLocation, keyStorePassword, keyStoreTypeProp);
                } catch (KeyManagerException e) {
                    LOG.error("Failed to create key manager", e);
                }
            }

            String trustStoreLocation = config.getProperty(x509Util.getSslTruststoreLocationProperty(), "");
            String trustStorePassword = config.getProperty(x509Util.getSslTruststorePasswdProperty(), "");
            String trustStoreTypeProp = config.getProperty(x509Util.getSslTruststoreTypeProperty());

            if (trustStoreLocation.isEmpty()) {
                LOG.warn("Truststore not specified for client connection");
            } else {
                try {
                    tm = X509Util.createTrustManager(
                        trustStoreLocation,
                        trustStorePassword,
                        trustStoreTypeProp,
                        crlEnabled,
                        ocspEnabled,
                        hostnameVerificationEnabled,
                        false);
                } catch (TrustManagerException e) {
                    LOG.error("Failed to create trust manager", e);
                }
            }
            this.keyManager = km;
            this.trustManager = tm;
        }
    }

    /**
     * Initialize the X509AuthenticationProvider with the provided
     * X509TrustManager and X509KeyManager.
     *
     * @param trustManager X509TrustManager implementation to use for remote
     *                     host authentication.
     * @param keyManager   X509KeyManager implementation to use for certificate
     *                     management.
     */
    public X509AuthenticationProvider(X509TrustManager trustManager, X509KeyManager keyManager) {
        this.trustManager = trustManager;
        this.keyManager = keyManager;
    }

    @Override
    public String getScheme() {
        return "x509";
    }

    @Override
    public KeeperException.Code handleAuthentication(ServerCnxn cnxn, byte[] authData) {
        X509Certificate[] certChain = (X509Certificate[]) cnxn.getClientCertificateChain();

        if (certChain == null || certChain.length == 0) {
            return KeeperException.Code.AUTHFAILED;
        }

        if (trustManager == null) {
            LOG.error("No trust manager available to authenticate session 0x{}", Long.toHexString(cnxn.getSessionId()));
            return KeeperException.Code.AUTHFAILED;
        }

        X509Certificate clientCert = certChain[0];

        try {
            // Authenticate client certificate
            trustManager.checkClientTrusted(certChain, clientCert.getPublicKey().getAlgorithm());
        } catch (CertificateException ce) {
            LOG.error("Failed to trust certificate for session 0x{}", Long.toHexString(cnxn.getSessionId()), ce);
            return KeeperException.Code.AUTHFAILED;
        }

        String clientId = getClientId(clientCert);

        if (clientId.equals(System.getProperty(ZOOKEEPER_X509AUTHENTICATIONPROVIDER_SUPERUSER))) {
            cnxn.addAuthInfo(new Id("super", clientId));
            LOG.info("Authenticated Id '{}' as super user", clientId);
        }

        Id authInfo = new Id(getScheme(), clientId);
        cnxn.addAuthInfo(authInfo);

        LOG.info("Authenticated Id '{}' for Scheme '{}'", authInfo.getId(), authInfo.getScheme());
        return KeeperException.Code.OK;
    }

    /**
     * Determine the string to be used as the remote host session Id for
     * authorization purposes. Associate this client identifier with a
     * ServerCnxn that has been authenticated over SSL, and any ACLs that refer
     * to the authenticated client.
     *
     * @param clientCert Authenticated X509Certificate associated with the
     *                   remote host.
     * @return Identifier string to be associated with the client.
     */
    protected String getClientId(X509Certificate clientCert) {
        String clientCertIdType =
            System.getProperty(ZOOKEEPER_X509AUTHENTICATIONPROVIDER_CLIENT_CERT_ID_TYPE);
        if (clientCertIdType != null && clientCertIdType.equalsIgnoreCase("SAN")) {
            try {
                return matchAndExtractSAN(clientCert);
            } catch (Exception ce) {
                LOG.warn("X509AuthenticationProvider::getClientId(): failed to match and extract a"
                    + " client ID from SAN! Using Subject DN instead...", ce);
            }
        }
        // return Subject DN by default
        return clientCert.getSubjectX500Principal().getName();
    }

    private String matchAndExtractSAN(X509Certificate clientCert)
        throws CertificateParsingException {
        Integer matchType =
            Integer.getInteger(ZOOKEEPER_X509AUTHENTICATIONPROVIDER_CLIENT_CERT_ID_SAN_MATCH_TYPE);
        String matchRegex =
            System.getProperty(ZOOKEEPER_X509AUTHENTICATIONPROVIDER_CLIENT_CERT_ID_SAN_MATCH_REGEX);
        String extractRegex =
            System.getProperty(
                ZOOKEEPER_X509AUTHENTICATIONPROVIDER_CLIENT_CERT_ID_SAN_EXTRACT_REGEX);
        Integer extractMatcherGroupIndex = Integer.getInteger(
            ZOOKEEPER_X509AUTHENTICATIONPROVIDER_CLIENT_CERT_ID_SAN_EXTRACT_MATCHER_GROUP_INDEX);
        LOG.info("X509AuthenticationProvider::matchAndExtractSAN(): Using SAN in the client cert "
                + "for client ID! matchType: {}, matchRegex: {}, extractRegex: {}, "
                + "extractMatcherGroupIndex: {}", matchType, matchRegex, extractRegex,
            extractMatcherGroupIndex);
        if (matchType == null || matchRegex == null || extractRegex == null || matchType < 0
            || matchType > 8) {
            // SAN extension must be in the range of [0, 8].
            // See GeneralName object defined in RFC 5280 (The ASN.1 definition of the
            // SubjectAltName extension)
            String errStr = "X509AuthenticationProvider::matchAndExtractSAN(): ClientCert ID type "
                + "SAN was provided but matchType or matchRegex given is invalid! matchType: "
                + matchType + " matchRegex: " + matchRegex;
            LOG.error(errStr);
            throw new IllegalArgumentException(errStr);
        }
        // filter by match type and match regex
        LOG.info("X509AuthenticationProvider::matchAndExtractSAN(): number of SAN entries found in"
            + " clientCert: " + clientCert.getSubjectAlternativeNames().size());
        Pattern matchPattern = Pattern.compile(matchRegex);
        Collection<List<?>> matched = clientCert.getSubjectAlternativeNames().stream().filter(
            list -> list.get(0).equals(matchType) && matchPattern
                .matcher((CharSequence) list.get(1)).find()).collect(Collectors.toList());

        LOG.info(
            "X509AuthenticationProvider::matchAndExtractSAN(): number of SAN entries matched: "
                + matched.size() + ". Printing all matches...");
        for (List<?> match : matched) {
            LOG.info("  Match: (" + match.get(0) + ", " + match.get(1) + ")");
        }

        // if there are more than one match or 0 matches, throw an error
        if (matched.size() != 1) {
            String errStr = "X509AuthenticationProvider::matchAndExtractSAN(): 0 or multiple "
                + "matches found in SAN! Please fix match type and regex so that exactly one match "
                + "is found.";
            LOG.error(errStr);
            throw new IllegalArgumentException(errStr);
        }

        // Extract a substring from the found match using extractRegex
        Pattern extractPattern = Pattern.compile(extractRegex);
        Matcher matcher = extractPattern.matcher(matched.iterator().next().get(1).toString());
        if (matcher.find()) {
            // If extractMatcherGroupIndex is not given, return the 1st index by default
            extractMatcherGroupIndex =
                extractMatcherGroupIndex == null ? 1 : extractMatcherGroupIndex;
            String result = matcher.group(extractMatcherGroupIndex);
            LOG.info("X509AuthenticationProvider::matchAndExtractSAN(): returning extracted "
                + "client ID: {} using Matcher group index: {}", result, extractMatcherGroupIndex);
            return result;
        }
        String errStr = "X509AuthenticationProvider::matchAndExtractSAN(): failed to find an "
            + "extract substring! Please review the extract regex!";
        LOG.error(errStr);
        throw new IllegalArgumentException(errStr);
    }

    @Override
    public boolean matches(String id, String aclExpr) {
        if (System.getProperty(ZOOKEEPER_X509AUTHENTICATIONPROVIDER_SUPERUSER) != null) {
            return id.equals(System.getProperty(ZOOKEEPER_X509AUTHENTICATIONPROVIDER_SUPERUSER))
                   || id.equals(aclExpr);
        }

        return id.equals(aclExpr);
    }

    @Override
    public boolean isAuthenticated() {
        return true;
    }

    @Override
    public boolean isValid(String id) {
        try {
            new X500Principal(id);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /**
     * Get the X509TrustManager implementation used for remote host
     * authentication.
     *
     * @return The X509TrustManager.
     * @throws TrustManagerException When there is no trust manager available.
     */
    public X509TrustManager getTrustManager() throws TrustManagerException {
        if (trustManager == null) {
            throw new TrustManagerException("No trust manager available");
        }
        return trustManager;
    }

    /**
     * Get the X509KeyManager implementation used for certificate management.
     *
     * @return The X509KeyManager.
     * @throws KeyManagerException When there is no key manager available.
     */
    public X509KeyManager getKeyManager() throws KeyManagerException {
        if (keyManager == null) {
            throw new KeyManagerException("No key manager available");
        }
        return keyManager;
    }

}
