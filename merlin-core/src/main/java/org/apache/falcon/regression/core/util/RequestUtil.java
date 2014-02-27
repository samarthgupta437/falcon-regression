package org.apache.falcon.regression.core.util;


import org.apache.falcon.security.FalconAuthorizationToken;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.PseudoAuthenticator;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicHeader;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class RequestUtil {
    public static final String AUTH_COOKIE = "hadoop.auth";
    private static final String AUTH_COOKIE_EQ = AUTH_COOKIE + "=";
    private static final String COOKIE = "Cookie";
    private static final String CURRENT_USER = System
            .getProperty("user.name");

    private static HttpResponse execute(String user, HttpUriRequest request)
    throws IOException, AuthenticationException, URISyntaxException {
        URI uri = request.getURI();
        URIBuilder uriBuilder = new URIBuilder(uri);
        // falcon now reads a user.name parameter in the request.
        // by default we will add it to every request.
        uriBuilder.addParameter(PseudoAuthenticator.USER_NAME, user);
        uri = uriBuilder.build();
        // get the token and add it to the header.
        // works in secure and un secure mode.
        AuthenticatedURL.Token token = FalconAuthorizationToken.getToken(user, uri.getScheme(),
                uri.getHost(), uri.getPort());
        request.addHeader(new BasicHeader(COOKIE, AUTH_COOKIE_EQ + token));
        DefaultHttpClient client = new DefaultHttpClient();
        return client.execute(request);
    }

    public static HttpResponse get(String url, String user)
    throws URISyntaxException, IOException, AuthenticationException {
        HttpGet get = new HttpGet(url);
        return execute(user, get);
    }

    public static HttpResponse get(String url)
    throws URISyntaxException, IOException, AuthenticationException {
        return get(url, CURRENT_USER);
    }

    public static HttpResponse delete(String url)
    throws URISyntaxException, IOException, AuthenticationException {
        return delete(url, CURRENT_USER);
    }

    public static HttpResponse delete(String url, String user)
    throws URISyntaxException, IOException, AuthenticationException {
        HttpDelete delete = new HttpDelete(url);
        return execute(user, delete);
    }

    public static HttpResponse post(String url)
    throws URISyntaxException, IOException, AuthenticationException {
        return post(url, null, CURRENT_USER);
    }

    public static HttpResponse post(String url, String user)
    throws URISyntaxException, IOException, AuthenticationException {
        return post(url, null, user);
    }

    public static HttpResponse post(String url, String data, String user)
    throws URISyntaxException, IOException, AuthenticationException {
        HttpPost post = new HttpPost(new URI(url));
        if (null != data) {
            post.setEntity(new StringEntity(data));
        }
        return execute(user, post);
    }
}