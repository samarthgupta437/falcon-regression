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

package org.apache.falcon.request;

import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.security.FalconAuthorizationToken;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.PseudoAuthenticator;
import org.apache.http.Header;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class BaseRequest {

    private String method;
    private String url;
    private List<Header> headers;
    private String requestData;
    private String user;
    private URI uri;
    private HttpHost target;

    public BaseRequest(String url, String method) throws URISyntaxException {
        this(url, method, RequestKeys.CURRENT_USER, null);
    }

    public BaseRequest(String url, String method, String user) throws URISyntaxException {
        this(url, method, user, null);
    }

    public BaseRequest(String url, String method, String user, String data) throws URISyntaxException {
        this.method = method;
        this.url = url;
        this.requestData = null;
        this.user = (null == user) ? RequestKeys.CURRENT_USER : user;
        this.uri = new URI(url);
        target = new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme());
        this.headers = new ArrayList<Header>();
        this.requestData = data;
    }

    public void addHeader(String name, String value) {
        headers.add(new BasicHeader(name, value));
    }

    public HttpResponse run() throws URISyntaxException, IOException, AuthenticationException {
        // process the get
        if(this.method.equalsIgnoreCase("get")) {
            return execute(new HttpGet(this.url));
        } else if (this.method.equalsIgnoreCase("delete")) {
            return execute(new HttpDelete(this.url));
        }

        HttpEntityEnclosingRequest request = null;
        if (this.method.equalsIgnoreCase("post")) {
            request = new HttpPost(new URI(this.url));
        }else if (this.method.equalsIgnoreCase("put")) {
            request = new HttpPut(new URI(this.url));
        }

        if (this.requestData != null) {
            request.setEntity(new StringEntity(requestData));
        }

        return execute(request);
    }

    private static final Logger LOGGER = Logger.getLogger(BaseRequest.class);

    private HttpResponse execute(HttpRequest request)
    throws IOException, AuthenticationException, URISyntaxException {
        URIBuilder uriBuilder = new URIBuilder(this.url);

        // falcon now reads a user.name parameter in the request.
        // by default we will add it to every request.
        uriBuilder.addParameter(PseudoAuthenticator.USER_NAME, this.user);
        uri = uriBuilder.build();

        // add headers to the request
        if (null != headers && headers.size() > 0) {
            for (Header header: headers) {
                request.addHeader(header);
            }
        }
        request.addHeader("Remote-User","test");
        // get the token and add it to the header.
        // works in secure and un secure mode.
        AuthenticatedURL.Token token = FalconAuthorizationToken.getToken(user, uri.getScheme(),
                uri.getHost(), uri.getPort());
        request.addHeader(RequestKeys.COOKIE, RequestKeys.AUTH_COOKIE_EQ + token);
        DefaultHttpClient client = new DefaultHttpClient();
        LOGGER.info("Request Url: " + request.getRequestLine().getUri().toString());
        LOGGER.info("Request Method: " + request.getRequestLine().getMethod());

        for (Header header : request.getAllHeaders()) {
            LOGGER.info(String.format("Request Header: Name=%s Value=%s", header.getName(),
                    header.getValue()));
        }

        HttpResponse response = client.execute(target, request);
        // incase the cookie is expired and we get a negotiate error back, generate the token again
        // and send the request
        if ((response.getStatusLine().getStatusCode() == HttpStatus.SC_UNAUTHORIZED)) {
            Header[] wwwAuthHeaders = response.getHeaders(RequestKeys.WWW_AUTHENTICATE);
            if (wwwAuthHeaders != null && wwwAuthHeaders.length != 0 &&
                    wwwAuthHeaders[0].getValue().trim().startsWith(RequestKeys.NEGOTIATE)) {
                token = FalconAuthorizationToken.getToken(user, uri.getScheme(),
                        uri.getHost(), uri.getPort(), true);

                request.removeHeaders(RequestKeys.COOKIE);
                request.addHeader(RequestKeys.COOKIE, RequestKeys.AUTH_COOKIE_EQ + token);
                LOGGER.info("Request Url: " + request.getRequestLine().getUri().toString());
                LOGGER.info("Request Method: " + request.getRequestLine().getMethod());
                for (Header header : request.getAllHeaders()) {
                    LOGGER.info(
                            String.format("Request Header: Name=%s Value=%s", header.getName(),
                                    header.getValue())
                    );
                }
                response = client.execute(target, request);
            }
        }
        LOGGER.info("Response Status: " + response.getStatusLine());
        for (Header header : response.getAllHeaders()) {
            LOGGER.info(String.format("Response Header: Name=%s Value=%s", header.getName(),
                    header.getValue()));
        }
        return response;
    }
}