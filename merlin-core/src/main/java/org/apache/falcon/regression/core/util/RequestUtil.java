/**
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

package org.apache.falcon.regression.core.util;


import org.apache.falcon.security.FalconAuthorizationToken;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.PseudoAuthenticator;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class RequestUtil {
    public static final String AUTH_COOKIE = "hadoop.auth";
    private static final String AUTH_COOKIE_EQ = AUTH_COOKIE + "=";
    private static final String COOKIE = "Cookie";
    private static final String CURRENT_USER = System
            .getProperty("user.name");
    private static final String WWW_AUTHENTICATE = "WWW-Authenticate";
    private static final String NEGOTIATE = "Negotiate";

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
        request.addHeader(COOKIE, AUTH_COOKIE_EQ + token);
        DefaultHttpClient client = new DefaultHttpClient();
        HttpResponse response = client.execute(request);
        // incase the cookie is expired and we get a negotiate error back, generate the token again
        // and send the request
        if ((response.getStatusLine().getStatusCode() == HttpStatus.SC_UNAUTHORIZED)) {
            Header[] wwwAuthHeaders = response.getHeaders(WWW_AUTHENTICATE);
            if (wwwAuthHeaders != null && wwwAuthHeaders.length != 0 &&
                    wwwAuthHeaders[0].getValue().trim().startsWith(NEGOTIATE)) {
                token = FalconAuthorizationToken.getToken(user, uri.getScheme(),
                        uri.getHost(), uri.getPort(), true);

                request.removeHeaders(COOKIE);
                request.addHeader(COOKIE, AUTH_COOKIE_EQ + token);
                response = client.execute(request);
            }
        }
        return response;
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