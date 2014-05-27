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

package org.apache.falcon.regression.core.helpers;

import com.google.gson.GsonBuilder;
import com.sun.tools.javac.util.Pair;
import junit.framework.Assert;
import org.apache.commons.lang.StringUtils;
import org.apache.falcon.regression.core.response.lineage.Direction;
import org.apache.falcon.regression.core.response.lineage.EdgesResult;
import org.apache.falcon.regression.core.response.lineage.Vertex;
import org.apache.falcon.regression.core.response.lineage.VerticesResult;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.request.BaseRequest;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.http.HttpResponse;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;

import javax.xml.bind.JAXBException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;

public class LineageHelper {
    private static Logger logger = Logger.getLogger(LineageHelper.class);
    private final String hostname;


    public static final String RESULTS = "results";
    public static final String TOTAL_SIZE = "totalSize";

    public enum URL {
        SERIALIZE("/api/graphs/lineage/serialize"),
        VERTICES("/api/graphs/lineage/vertices"),
        VERTICES_ALL("/api/graphs/lineage/vertices/all"),
        VERTICES_PROPERTIES("/api/graphs/lineage/vertices/properties"),
        EDGES("/api/graphs/lineage/edges"),
        EDGES_ALL("/api/graphs/lineage/edges/all");

        private final String url;

        URL(String url) {
            this.url = url;
        }

        public String getValue() {
            return this.url;
        }
    }

    public LineageHelper(String hostname) {
        this.hostname = hostname.trim().replaceAll("/$", "");
    }

    public LineageHelper(PrismHelper prismHelper) {
        this(prismHelper.getClusterHelper().getHostname());
    }

    private String getResponseString(HttpResponse response) throws IOException {
        BufferedReader reader = new BufferedReader(
            new InputStreamReader(response.getEntity().getContent(), "UTF-8"));
        StringBuilder sb = new StringBuilder();
        for (String line; (line = reader.readLine()) != null; ) {
            sb.append(line).append("\n");
        }
        return sb.toString();
    }

    private HttpResponse runGetRequest(String url)
        throws URISyntaxException, IOException, AuthenticationException {
        final BaseRequest request = new BaseRequest(url, "get", null);
        return request.run();
    }

    private String getUrl(final URL url, final String urlPath, final Pair<String, String>... paramPairs) {
        Assert.assertNotNull(hostname, "Hostname can't be null.");
        String hostAndPath = hostname + url.getValue();
        if(urlPath != null) {
            hostAndPath += "/" + urlPath;
        }
        if(paramPairs.length > 0) {
            String[] params = new String[paramPairs.length];
            for (int i = 0; i < paramPairs.length; ++i) {
                params[i] = StringUtils.join(new String[] {paramPairs[i].fst, paramPairs[i].snd}, "=");
            }
            return hostAndPath + "/?" + StringUtils.join(params, "&");
        }
        return hostAndPath;
    }

    private String getUrl(final URL url, final Pair<String, String>... paramPairs) {
        return getUrl(url, null, paramPairs);
    }

    public String getUrlPath(String... pathParts) {
        return StringUtils.join(pathParts, "/");
    }

    public String getUrlPath(int oneInt, String... pathParts) {
        return oneInt + "/" + getUrlPath(pathParts);
    }

    private VerticesResult getVerticesResult(String url)
        throws URISyntaxException, IOException, AuthenticationException {
        HttpResponse response = runGetRequest(url);
        String responseString = getResponseString(response);
        logger.info(Util.prettyPrintXmlOrJson(responseString));
        return new GsonBuilder().create().fromJson(responseString,
            VerticesResult.class);
    }

    public VerticesResult getAllVertices()
        throws AuthenticationException, IOException, URISyntaxException, JAXBException,
        JSONException {
        return getVerticesResult(getUrl(URL.VERTICES_ALL));
    }

    public VerticesResult getVertices(String key, String value)
        throws AuthenticationException, IOException, URISyntaxException, JAXBException,
        JSONException {
        return getVerticesResult(getUrl(URL.VERTICES,
            new Pair<String, String>("key", key),
            new Pair<String, String>("value", value)));
    }

    public VerticesResult getVerticesByType(Vertex.VERTEX_TYPE vertexType)
        throws AuthenticationException, IOException, URISyntaxException, JAXBException,
        JSONException {
        return getVertices("type", vertexType.getValue());
    }

    public VerticesResult getVerticesByName(String name)
        throws AuthenticationException, IOException, URISyntaxException, JAXBException,
        JSONException {
        return getVertices("name", name);
    }

    public VerticesResult getVerticesByDirection(int vertexId, Direction direction)
        throws AuthenticationException, IOException, URISyntaxException, JAXBException,
        JSONException {
        return getVerticesResult(getUrl(URL.VERTICES, getUrlPath(vertexId, direction.getValue())));
    }

    public EdgesResult getAllEdges()
        throws AuthenticationException, IOException, URISyntaxException, JAXBException,
        JSONException {
        HttpResponse response = runGetRequest(getUrl(URL.EDGES_ALL));
        String responseString = getResponseString(response);
        logger.info(Util.prettyPrintXmlOrJson(responseString));
        final EdgesResult edgesResult = new GsonBuilder().create().fromJson(responseString,
            EdgesResult.class);
        return edgesResult;
    }

}
