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
import org.apache.falcon.regression.core.response.graph.EdgesResult;
import org.apache.falcon.regression.core.response.graph.VerticesResult;
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

public class GraphHelper {
    private static Logger logger = Logger.getLogger(GraphHelper.class);
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

    public GraphHelper(String hostname) {
        this.hostname = hostname.trim().replaceAll("/$", "");
    }

    public GraphHelper(PrismHelper prismHelper) {
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

    private String getUrl(URL url, Pair<String, String>... paramPairs) {
        Assert.assertNotNull(hostname, "Hostname can't be null.");
        if(paramPairs.length > 0) {
            String[] params = new String[paramPairs.length];
            for (int i = 0; i < paramPairs.length; ++i) {
                params[i] = StringUtils.join(new String[] {paramPairs[i].fst, paramPairs[i].snd}, "=");
            }
            return hostname + url.getValue() + "/?" + StringUtils.join(params, "&");
        }
        return hostname + url.getValue();
    }

    public VerticesResult getAllVertices()
        throws AuthenticationException, IOException, URISyntaxException, JAXBException,
        JSONException {
        HttpResponse response = runGetRequest(getUrl(URL.VERTICES_ALL));
        String responseString = getResponseString(response);
        logger.info(Util.prettyPrintXmlOrJson(responseString));
        final VerticesResult allVertices = new GsonBuilder().create().fromJson(responseString,
            VerticesResult.class);
        return allVertices;
    }

    public VerticesResult getVertices(String key, String value)
        throws AuthenticationException, IOException, URISyntaxException, JAXBException,
        JSONException {
        HttpResponse response = runGetRequest(getUrl(URL.VERTICES,
            new Pair<String, String>("key", key),
            new Pair<String, String>("value", value)));
        String responseString = getResponseString(response);
        logger.info(Util.prettyPrintXmlOrJson(responseString));
        final VerticesResult verticesResult = new GsonBuilder().create().fromJson(responseString,
            VerticesResult.class);
        return verticesResult;
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
