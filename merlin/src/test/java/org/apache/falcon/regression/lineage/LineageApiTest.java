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

package org.apache.falcon.regression.lineage;

import com.google.gson.GsonBuilder;
import org.apache.falcon.regression.Entities.ClusterMerlin;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.regression.core.enumsAndConstants.MerlinConstants;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.LineageHelper;
import org.apache.falcon.regression.core.response.lineage.Direction;
import org.apache.falcon.regression.core.response.lineage.Edge;
import org.apache.falcon.regression.core.response.lineage.EdgesResult;
import org.apache.falcon.regression.core.response.lineage.Vertex;
import org.apache.falcon.regression.core.response.lineage.VertexResult;
import org.apache.falcon.regression.core.response.lineage.VerticesResult;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.CleanupUtil;
import org.apache.falcon.regression.core.util.Generator;
import org.apache.falcon.regression.core.util.GraphAssert;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.http.HttpResponse;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Random;

@Test(groups = "embedded")
public class LineageApiTest extends BaseTestClass {
    private static final String datePattern = "${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    private static final Logger logger = Logger.getLogger(LineageApiTest.class);
    private static final String testTag =
        Edge.LEBEL_TYPE.TESTNAME.toString().toLowerCase() + "=LineageApiTest";
    LineageHelper lineageHelper;
    final ColoHelper cluster = servers.get(0);
    final String baseTestHDFSDir = baseHDFSDir + "/LineageApiTest";
    final String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    final String feedInputPath =
        baseTestHDFSDir + "/input";
    final String feedOutputPath =
        baseTestHDFSDir + "/output";
    // use 5 <= x < 10 input feeds
    final int numInputFeeds = 5 + new Random().nextInt(5);
    // use 5 <= x < 10 output feeds
    final int numOutputFeeds = 5 + new Random().nextInt(5);
    ClusterMerlin clusterMerlin;
    FeedMerlin[] inputFeeds;
    FeedMerlin[] outputFeeds;

    @BeforeClass(alwaysRun = true)
    public void init() throws Exception {
        lineageHelper = new LineageHelper(prism);
    }

    @BeforeMethod(alwaysRun = true, firstTimeOnly = true)
    public void setUp() throws Exception {
        CleanupUtil.cleanAllEntities(prism);
        Bundle bundle = BundleUtil.readELBundles()[0][0];
        bundle.generateUniqueBundle();
        bundles[0] = new Bundle(bundle, cluster);
        final List<String> clusterStrings = bundles[0].getClusters();
        Assert.assertEquals(clusterStrings.size(), 1, "Expecting only 1 clusterMerlin.");
        clusterMerlin = new ClusterMerlin(clusterStrings.get(0));
        clusterMerlin.setTags(testTag);
        AssertUtil.assertSucceeded(
            prism.getClusterHelper().submitEntity(Util.URLS.SUBMIT_URL, clusterMerlin.toString()));
        logger.info("numInputFeeds = " + numInputFeeds);
        logger.info("numOutputFeeds = " + numOutputFeeds);
        final FeedMerlin inputMerlin = new FeedMerlin(BundleUtil.getInputFeedFromBundle(bundles[0]));
        inputMerlin.setTags(testTag);
        inputFeeds = generateFeeds(numInputFeeds, inputMerlin,
            Generator.getNameGenerator("infeed", inputMerlin.getName()),
            Generator.getHadoopPathGenerator(feedInputPath, datePattern));
        for (FeedMerlin feed : inputFeeds) {
            AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(Util.URLS.SUBMIT_URL,
                feed.toString()));
        }

        FeedMerlin outputMerlin = new FeedMerlin(BundleUtil.getOutputFeedFromBundle(bundles[0]));
        outputMerlin.setTags(testTag);
        outputFeeds = generateFeeds(numOutputFeeds, outputMerlin,
            Generator.getNameGenerator("outfeed", outputMerlin.getName()),
            Generator.getHadoopPathGenerator(feedOutputPath, datePattern));
        for (FeedMerlin feed : outputFeeds) {
            AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(Util.URLS.SUBMIT_URL,
                feed.toString()));
        }
    }

    public static FeedMerlin[] generateFeeds(final int numInputFeeds,
                                             final FeedMerlin originalFeedMerlin,
                                             final Generator nameGenerator,
                                             final Generator pathGenerator)
        throws JAXBException, NoSuchMethodException, InvocationTargetException,
        IllegalAccessException, IOException, URISyntaxException, AuthenticationException {
        FeedMerlin[] inputFeeds = new FeedMerlin[numInputFeeds];
        //submit all input feeds
        for(int count = 0; count < numInputFeeds; ++count) {
            final FeedMerlin feed = new FeedMerlin(originalFeedMerlin.toString());
            feed.setName(nameGenerator.generate());
            feed.setLocation(LocationType.DATA, pathGenerator.generate());
            inputFeeds[count] = feed;
        }
        return inputFeeds;
    }

    @AfterMethod(alwaysRun = true, lastTimeOnly = true)
    public void tearDown() {
        for (FeedMerlin inputFeed : inputFeeds) {
            CleanupUtil.deleteQuietly(prism.getFeedHelper(), inputFeed.toString());
        }
        for (FeedMerlin outputFeed : outputFeeds) {
            CleanupUtil.deleteQuietly(prism.getFeedHelper(), outputFeed.toString());
        }
        removeBundles();
    }

    @Test
    public void testAllVertices() throws Exception {
        final VerticesResult verticesResult = lineageHelper.getAllVertices();
        logger.info(verticesResult);
        GraphAssert.assertVertexSanity(verticesResult);
        GraphAssert.assertUserVertexPresence(verticesResult);
        GraphAssert.assertVerticesPresenceMinOccur(verticesResult, Vertex.VERTEX_TYPE.COLO, 1);
        GraphAssert.assertVerticesPresenceMinOccur(verticesResult, Vertex.VERTEX_TYPE.TAGS, 1);
        GraphAssert.assertVerticesPresenceMinOccur(verticesResult, Vertex.VERTEX_TYPE.CLUSTER_ENTITY, 1);
        GraphAssert.assertVerticesPresenceMinOccur(verticesResult,
            Vertex.VERTEX_TYPE.FEED_ENTITY, numInputFeeds + numOutputFeeds);
    }

    @Test
    public void testVertexId() throws Exception {
        final VerticesResult userResult =
            lineageHelper.getVerticesByName(MerlinConstants.CURRENT_USER_NAME);
        GraphAssert.assertVertexSanity(userResult);
        final int vertexId = userResult.getResults().get(0).get_id();
        final VertexResult userVertex =
            lineageHelper.getVertexById(vertexId);
        Assert.assertEquals(userResult.getResults().get(0), userVertex.getResults(),
            "Same vertex should have been returned.");
    }

    @Test
    public void testVertexNoId() throws Exception {
        final VerticesResult userResult =
            lineageHelper.getVerticesByName(MerlinConstants.CURRENT_USER_NAME);
        GraphAssert.assertVertexSanity(userResult);
        final int vertexId = userResult.getResults().get(0).get_id();
        HttpResponse response = lineageHelper.runGetRequest(
            lineageHelper.getUrl(LineageHelper.URL.VERTICES, ""));
        String responseString = lineageHelper.getResponseString(response);
        logger.info("response: " + response);
        logger.info("responseString: " + responseString);
        Assert.assertNotEquals(response.getStatusLine().getStatusCode(), 500,
            "We should not get internal server error");
    }

    @Test
    public void testVertexInvalidId() throws Exception {
        final VerticesResult allVerticesResult =
            lineageHelper.getAllVertices();
        GraphAssert.assertVertexSanity(allVerticesResult);
        int invalidVertexId = -1;
        for (Vertex vertex : allVerticesResult.getResults()) {
            if(invalidVertexId <= vertex.get_id()) {
                invalidVertexId = vertex.get_id() + 1;
            }
        }

        HttpResponse response = lineageHelper.runGetRequest(
            lineageHelper.getUrl(LineageHelper.URL.VERTICES, "" + invalidVertexId));
        String responseString = lineageHelper.getResponseString(response);
        logger.info("response: " + response);
        logger.info("responseString: " + responseString);
        Assert.assertTrue(
            responseString.matches(String.format(".*Vertex.*%d.*not.*found.*\n?", invalidVertexId)),
            "Unexpected responseString: " + responseString);
        Assert.assertEquals(response.getStatusLine().getStatusCode(), 404,
            "We should get 404 Not Found error");
    }

    @Test
    public void testVerticesFilterByName() throws Exception {
        final String clusterName = clusterMerlin.getName();
        final VerticesResult clusterVertices = lineageHelper.getVerticesByName(clusterName);
        GraphAssert.assertVertexSanity(clusterVertices);
        GraphAssert.assertVerticesPresenceMinOccur(clusterVertices,
            Vertex.VERTEX_TYPE.CLUSTER_ENTITY, 1);
        GraphAssert.assertVertexPresence(clusterVertices, clusterName);
        for(int i = 0; i < numInputFeeds; ++i) {
            final String feedName = inputFeeds[i].getName();
            final VerticesResult feedVertices = lineageHelper.getVerticesByName(feedName);
            GraphAssert.assertVertexSanity(feedVertices);
            GraphAssert.assertVerticesPresenceMinOccur(feedVertices,
                Vertex.VERTEX_TYPE.FEED_ENTITY, 1);
            GraphAssert.assertVertexPresence(feedVertices, feedName);
        }
        for(int i = 0; i < numOutputFeeds; ++i) {
            final String feedName = outputFeeds[i].getName();
            final VerticesResult feedVertices = lineageHelper.getVerticesByName(feedName);
            GraphAssert.assertVertexSanity(feedVertices);
            GraphAssert.assertVerticesPresenceMinOccur(feedVertices,
                Vertex.VERTEX_TYPE.FEED_ENTITY, 1);
            GraphAssert.assertVertexPresence(feedVertices, feedName);
        }

    }

    @Test
    public void testVerticesFilterByType() throws Exception {
        final VerticesResult clusterVertices =
            lineageHelper.getVerticesByType(Vertex.VERTEX_TYPE.CLUSTER_ENTITY);
        GraphAssert.assertVertexSanity(clusterVertices);
        GraphAssert.assertVerticesPresenceMinOccur(clusterVertices,
            Vertex.VERTEX_TYPE.CLUSTER_ENTITY, 1);
        GraphAssert.assertVertexPresence(clusterVertices, clusterMerlin.getName());
        final VerticesResult feedVertices =
            lineageHelper.getVerticesByType(Vertex.VERTEX_TYPE.FEED_ENTITY);
        GraphAssert.assertVertexSanity(feedVertices);
        GraphAssert.assertVerticesPresenceMinOccur(feedVertices,
            Vertex.VERTEX_TYPE.FEED_ENTITY, 1);
        for (FeedMerlin oneFeed : inputFeeds) {
            GraphAssert.assertVertexPresence(feedVertices, oneFeed.getName());
        }
        for (FeedMerlin oneFeed : outputFeeds) {
            GraphAssert.assertVertexPresence(feedVertices, oneFeed.getName());
        }
    }

    @Test
    public void testAllEdges() throws Exception {
        final EdgesResult edgesResult = lineageHelper.getAllEdges();
        logger.info(edgesResult);
        Assert.assertTrue(edgesResult.getTotalSize() > 0, "Total number of edges should be" +
            " greater that zero but is: " + edgesResult.getTotalSize());
        GraphAssert.assertEdgeSanity(edgesResult);
        GraphAssert.assertEdgePresenceMinOccur(edgesResult, Edge.LEBEL_TYPE.CLUSTER_COLO, 1);
        GraphAssert.assertEdgePresenceMinOccur(edgesResult, Edge.LEBEL_TYPE.STORED_IN,
            numInputFeeds + numOutputFeeds);
        GraphAssert.assertEdgePresenceMinOccur(edgesResult, Edge.LEBEL_TYPE.OWNED_BY,
            1 + numInputFeeds + numOutputFeeds);
    }

    @Test
    public void testColoToEntityNode() throws Exception {
        final VerticesResult verticesResult = lineageHelper.getVerticesByType(Vertex.VERTEX_TYPE.COLO);
        GraphAssert.assertVertexSanity(verticesResult);
        Assert.assertTrue(verticesResult.getTotalSize() > 0, "Expected at least 1 colo node");
        Assert.assertTrue(verticesResult.getTotalSize() <= 3, "Expected at most 3 colo nodes");
        final List<Vertex> colo1Vertex = verticesResult.filterByName(clusterMerlin.getColo());
        AssertUtil.checkForListSize(colo1Vertex, 1);
        Vertex coloVertex = colo1Vertex.get(0);
        logger.info("coloVertex: " + coloVertex);
        final VerticesResult verticesByDirection =
            lineageHelper.getVerticesByDirection(coloVertex.get_id(), Direction.inComingVertices);
        AssertUtil.checkForListSize(
            verticesByDirection.filterByName(clusterMerlin.getName()), 1);
    }

    @Test
    public void testClusterNodeToFeedNode() throws Exception {
        final VerticesResult clusterResult = lineageHelper.getVerticesByName(
            clusterMerlin.getName());
        GraphAssert.assertVertexSanity(clusterResult);
        Vertex clusterVertex = clusterResult.getResults().get(0);
        final VerticesResult clusterIncoming =
            lineageHelper.getVerticesByDirection(clusterVertex.get_id(), Direction.inComingVertices);
        GraphAssert.assertVertexSanity(clusterIncoming);
        for(FeedMerlin feed : inputFeeds) {
            AssertUtil.checkForListSize(clusterIncoming.filterByName(feed.getName()), 1);
        }
        for(FeedMerlin feed : outputFeeds) {
            AssertUtil.checkForListSize(clusterIncoming.filterByName(feed.getName()), 1);
        }
    }

    @Test
    public void testUserToEntityNode() throws Exception {
        final VerticesResult userResult = lineageHelper.getVerticesByName(
            MerlinConstants.CURRENT_USER_NAME);
        GraphAssert.assertVertexSanity(userResult);
        Vertex clusterVertex = userResult.getResults().get(0);
        final VerticesResult userIncoming =
            lineageHelper.getVerticesByDirection(clusterVertex.get_id(), Direction.inComingVertices);
        GraphAssert.assertVertexSanity(userIncoming);
        AssertUtil.checkForListSize(userIncoming.filterByName(clusterMerlin.getName()), 1);
        for(FeedMerlin feed : inputFeeds) {
            AssertUtil.checkForListSize(userIncoming.filterByName(feed.getName()), 1);
        }
        for(FeedMerlin feed : outputFeeds) {
            AssertUtil.checkForListSize(userIncoming.filterByName(feed.getName()), 1);
        }
    }
}
