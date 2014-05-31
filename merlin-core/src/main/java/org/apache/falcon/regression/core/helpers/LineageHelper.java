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
import org.apache.commons.lang.StringUtils;
import org.apache.falcon.regression.core.response.lineage.Direction;
import org.apache.falcon.regression.core.response.lineage.EdgesResult;
import org.apache.falcon.regression.core.response.lineage.Vertex;
import org.apache.falcon.regression.core.response.lineage.VertexResult;
import org.apache.falcon.regression.core.response.lineage.VerticesResult;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.request.BaseRequest;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.http.HttpResponse;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.testng.Assert;

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

    /**
     * Lineage related REST endpoints
     */
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

    /**
     * Create a LineageHelper to use with a specified hostname
     * @param hostname hostname
     */
    public LineageHelper(String hostname) {
        this.hostname = hostname.trim().replaceAll("/$", "");
    }

    /**
     * Create a LineageHelper to use with a specified prismHelper
     * @param prismHelper prismHelper
     */
    public LineageHelper(PrismHelper prismHelper) {
        this(prismHelper.getClusterHelper().getHostname());
    }

    /**
     * Extract response string from the response object
     * @param response the response object
     * @return the response string
     * @throws IOException
     */
    public String getResponseString(HttpResponse response) throws IOException {
        BufferedReader reader = new BufferedReader(
            new InputStreamReader(response.getEntity().getContent(), "UTF-8"));
        StringBuilder sb = new StringBuilder();
        for (String line; (line = reader.readLine()) != null; ) {
            sb.append(line).append("\n");
        }
        return sb.toString();
    }

    /**
     * Run a get request on the specified url
     * @param url url
     * @return response of the request
     * @throws URISyntaxException
     * @throws IOException
     * @throws AuthenticationException
     */
    public HttpResponse runGetRequest(String url)
        throws URISyntaxException, IOException, AuthenticationException {
        final BaseRequest request = new BaseRequest(url, "get", null);
        return request.run();
    }

    /**
     * Successfully run a get request on the specified url
     * @param url url
     * @return string response of the request
     * @throws URISyntaxException
     * @throws IOException
     * @throws AuthenticationException
     */
    public String runGetRequestSuccessfully(String url)
        throws URISyntaxException, IOException, AuthenticationException {
        HttpResponse response = runGetRequest(url);
        String responseString = getResponseString(response);
        logger.info(Util.prettyPrintXmlOrJson(responseString));
        Assert.assertEquals(response.getStatusLine().getStatusCode(), 200,
            "The get request  was expected to be successfully");
        return responseString;
    }

    /**
     * Create a full url for the given lineage endpoint, urlPath and parameter
     * @param url lineage endpoint
     * @param urlPath url path to be added to lineage endpoint
     * @param paramPairs parameters to be passed
     * @return url string
     */
    public String getUrl(final URL url, final String urlPath, final Pair<String,
        String>... paramPairs) {
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

    /**
     * Create a full url for the given lineage endpoint and parameter
     * @param url lineage endpoint
     * @param paramPairs parameters to be passed
     * @return url string
     */
    public String getUrl(final URL url, final Pair<String, String>... paramPairs) {
        return getUrl(url, null, paramPairs);
    }

    /**
     * Create url path from parts
     * @param pathParts parts of the path
     * @return url path
     */
    public String getUrlPath(String... pathParts) {
        return StringUtils.join(pathParts, "/");
    }

    /**
     * Create url path from parts
     * @param oneInt part of the path
     * @param pathParts parts of the path
     * @return url path
     */
    public String getUrlPath(int oneInt, String... pathParts) {
        return oneInt + "/" + getUrlPath(pathParts);
    }

    /**
     * Get vertices result for the url
     * @param url url
     * @return result of the REST request
     * @throws URISyntaxException
     * @throws IOException
     * @throws AuthenticationException
     */
    public VerticesResult getVerticesResult(String url)
        throws URISyntaxException, IOException, AuthenticationException {
        String responseString = runGetRequestSuccessfully(url);
        return new GsonBuilder().create().fromJson(responseString,
            VerticesResult.class);
    }

    /**
     * Get vertex result for the url
     * @param url url
     * @return result of the REST request
     * @throws URISyntaxException
     * @throws IOException
     * @throws AuthenticationException
     */
    private VertexResult getVertexResult(String url)
        throws URISyntaxException, IOException, AuthenticationException {
        String responseString = runGetRequestSuccessfully(url);
        return new GsonBuilder().create().fromJson(responseString,
            VertexResult.class);
    }

    /**
     * Get all the vertices
     * @return all the vertices
     * @throws AuthenticationException
     * @throws IOException
     * @throws URISyntaxException
     * @throws JAXBException
     * @throws JSONException
     */
    public VerticesResult getAllVertices()
        throws AuthenticationException, IOException, URISyntaxException, JAXBException,
        JSONException {
        return getVerticesResult(getUrl(URL.VERTICES_ALL));
    }

    public VerticesResult getVertices(Vertex.FilterKey key, String value)
        throws AuthenticationException, IOException, URISyntaxException, JAXBException,
        JSONException {
        return getVerticesResult(getUrl(URL.VERTICES,
            new Pair<String, String>("key", key.toString()),
            new Pair<String, String>("value", value)));
    }

    public VertexResult getVertexById(int vertexId)
        throws AuthenticationException, IOException, URISyntaxException, JAXBException,
        JSONException {
        return getVertexResult(getUrl(URL.VERTICES, getUrlPath(vertexId)));
    }

    public VertexResult getVertexProperties(int vertexId)
        throws AuthenticationException, IOException, URISyntaxException, JAXBException,
        JSONException {
        return getVertexResult(getUrl(URL.VERTICES_PROPERTIES, getUrlPath(vertexId)));
    }

    public VerticesResult getVerticesByType(Vertex.VERTEX_TYPE vertexType)
        throws AuthenticationException, IOException, URISyntaxException, JAXBException,
        JSONException {
        return getVertices(Vertex.FilterKey.type, vertexType.getValue());
    }

    public VerticesResult getVerticesByName(String name)
        throws AuthenticationException, IOException, URISyntaxException, JAXBException,
        JSONException {
        return getVertices(Vertex.FilterKey.name, name);
    }

    public VerticesResult getVerticesByDirection(int vertexId, Direction direction)
        throws AuthenticationException, IOException, URISyntaxException, JAXBException,
        JSONException {
        return getVerticesResult(getUrl(URL.VERTICES, getUrlPath(vertexId, direction.getValue())));
    }

    public EdgesResult getAllEdges()
        throws AuthenticationException, IOException, URISyntaxException, JAXBException,
        JSONException {
        String responseString = runGetRequestSuccessfully(getUrl(URL.EDGES_ALL));
        final EdgesResult edgesResult = new GsonBuilder().create().fromJson(responseString,
            EdgesResult.class);
        return edgesResult;
    }

}
