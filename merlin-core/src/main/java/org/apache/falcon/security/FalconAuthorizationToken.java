package org.apache.falcon.security;

import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.authentication.client.PseudoAuthenticator;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;

public class FalconAuthorizationToken {
    private static final String AUTH_URL = "api/options";
    private static final KerberosAuthenticator AUTHENTICATOR = new KerberosAuthenticator();
    private static final FalconAuthorizationToken INSTANCE = new FalconAuthorizationToken();

    // Use a hashmap so that we can cache the tokens.
    private final ThreadLocal<HashMap<String, AuthenticatedURL.Token>> tokens =
            new ThreadLocal<HashMap<String,
                    AuthenticatedURL.Token>>();

    private FalconAuthorizationToken() {

    }

    public static void authenticate(String user, String protocol, String host,
                                    int port)
    throws IOException, AuthenticationException {
        URL url = new URL(String.format("%s://%s:%d/%s", protocol, host, port,
                AUTH_URL + "?" + PseudoAuthenticator.USER_NAME + "=" + user));

        AuthenticatedURL.Token currentToken = new AuthenticatedURL.Token();
        // using KerberosAuthenticator which falls back to PsuedoAuthenticator
        // instead of passing authentication type from the command line - bad factory
        new AuthenticatedURL(AUTHENTICATOR).openConnection(url, currentToken);
        String key = getKey(user, protocol, host, port);
        // initialize a hash map if its null.
        if (null == INSTANCE.tokens.get()) {
            INSTANCE.tokens.set(new HashMap<String, AuthenticatedURL.Token>());
        }
        INSTANCE.tokens.get().put(key, currentToken);
    }

    public static AuthenticatedURL.Token getToken(String user, String protocol, String host,
                                                  int port)
    throws IOException, AuthenticationException {
        String key = getKey(user, protocol, host, port);

        // if the tokens are null or if token is not found then we will go ahead and authenticate
        if ((null == INSTANCE.tokens.get()) || (!INSTANCE.tokens.get().containsKey(key))) {
            authenticate(user, protocol, host, port);
        }

        return INSTANCE.tokens.get().get(key);
    }

    // spnego token will be unique to the user and uri its being requested for.
    // Hence the key of the hash map is the combination of user, protocol, host and port.
    private static String getKey(String user, String protocol, String host, int port) {
        return String.format("%s-%s-%s-%d", user, protocol, host, port);
    }
}