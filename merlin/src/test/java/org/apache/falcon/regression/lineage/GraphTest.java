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

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.enumsAndConstants.MerlinConstants;
import org.apache.falcon.regression.core.generated.feed.LocationType;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.interfaces.IEntityManagerHelper;
import org.apache.falcon.regression.core.response.graph.EdgesResult;
import org.apache.falcon.regression.core.response.graph.GraphResult;
import org.apache.falcon.regression.core.response.graph.VerticesResult;
import org.apache.falcon.regression.core.response.graph.Edge;
import org.apache.falcon.regression.core.response.graph.Vertex;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.helpers.GraphHelper;
import org.apache.falcon.regression.core.util.CleanupUtil;
import org.apache.falcon.regression.core.util.Generator;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
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
import java.util.Random;

@Test(groups = "embedded")
public class GraphTest extends BaseTestClass {
    private static final String datePattern = "${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    private final Logger logger = Logger.getLogger(GraphTest.class);
    GraphHelper graphHelper;
    final ColoHelper cluster = servers.get(0);
    final String baseTestHDFSDir = baseHDFSDir + "/GraphTest";
    final String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    final String feedInputPath =
        baseTestHDFSDir + "/input";
    final String feedOutputPath =
        baseTestHDFSDir + "/output";
    // use 5 <= x < 10 input feeds
    final int numInputFeeds = 5 + new Random().nextInt(5);
    // use 5 <= x < 10 output feeds
    final int numOutputFeeds = 5 + new Random().nextInt(5);
    FeedMerlin[] inputFeeds;
    FeedMerlin[] outputFeeds;

    @BeforeClass(alwaysRun = true)
    public void init() throws Exception {
        graphHelper = new GraphHelper(prism);
    }

    @BeforeMethod(alwaysRun = true, firstTimeOnly = true)
    public void setUp() throws Exception {
        CleanupUtil.cleanAllEntities(prism);
        Bundle bundle = BundleUtil.readELBundles()[0][0];
        bundle.generateUniqueBundle();
        bundles[0] = new Bundle(bundle, cluster);
        bundles[0].submitClusters(prism);
        logger.info("numInputFeeds = " + numInputFeeds);
        logger.info("numOutputFeeds = " + numOutputFeeds);
        final FeedMerlin inputMerlin = new FeedMerlin(BundleUtil.getInputFeedFromBundle(bundles[0]));
        inputFeeds = generateFeeds(numInputFeeds, inputMerlin,
            Generator.getNameGenerator("infeed", inputMerlin.getName()),
            Generator.getHadoopPathGenerator(feedInputPath, datePattern));
        for (FeedMerlin feed : inputFeeds) {
            AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(Util.URLS.SUBMIT_URL,
                feed.toString()));
        }

        FeedMerlin outputMerlin = new FeedMerlin(BundleUtil.getOutputFeedFromBundle(bundles[0]));
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
        removeBundles();
        for (FeedMerlin inputFeed : inputFeeds) {
            deleteQuietly(prism.getFeedHelper(), inputFeed.toString());
        }
        for (FeedMerlin outputFeed : outputFeeds) {
            deleteQuietly(prism.getFeedHelper(), outputFeed.toString());
        }
    }

    private void deleteQuietly(IEntityManagerHelper helper, String feed) {
        try {
            helper.delete(Util.URLS.DELETE_URL, feed);
        } catch (Exception e) {
            logger.info("Caught exception: " + ExceptionUtils.getStackTrace(e));
        }
    }

    public void testAllVertices() throws Exception {
        final VerticesResult verticesResult = graphHelper.getAllVertices();
        logger.info(verticesResult);
        assertVertexSanity(verticesResult);
        assertUserVertexPresence(verticesResult);
    }

    public void testAllEdges() throws Exception {
        final EdgesResult edgesResult = graphHelper.getAllEdges();
        logger.info(edgesResult);
        Assert.assertTrue(edgesResult.getTotalSize() > 0, "Total number of edges should be" +
            " greater that zero but is: " + edgesResult.getTotalSize());
        assertEdgeSanity(edgesResult);
    }

    private void assertUserVertexPresence(VerticesResult verticesResult) {
        checkVerticesPresence(verticesResult, 1);
        for(Vertex vertex : verticesResult.getResults()) {
            if(vertex.getType().equals(Vertex.VERTEX_TYPE.USER.getValue())) {
                if(vertex.getName().equals(MerlinConstants.CURRENT_USER_NAME)) {
                    return;
                }
            }
        }
        Assert.fail(String.format("Vertex corresponding to user: %s is not present.",
            MerlinConstants.CURRENT_USER_NAME));
    }

    private void checkVerticesPresence(GraphResult graphResult, int minNumOfVertices) {
        Assert.assertTrue(graphResult.getTotalSize() >= minNumOfVertices,
            "graphResult should have at least " + minNumOfVertices + " vertex");
    }

    private void assertVertexSanity(VerticesResult verticesResult) {
        Assert.assertEquals(verticesResult.getResults().length, verticesResult.getTotalSize(),
            "Size of vertices don't match");
        for (Vertex vertex : verticesResult.getResults()) {
            Assert.assertNotNull(vertex.get_id(),
                "id of the vertex should be non-null: " + vertex);
            Assert.assertNotNull(vertex.get_type(),
                "_type of the vertex should be non-null: " + vertex);
            Assert.assertNotNull(vertex.getName(),
                "name of the vertex should be non-null: " + vertex);
            Assert.assertNotNull(vertex.getType(),
                "id of the vertex should be non-null: " + vertex);
            Assert.assertNotNull(vertex.getTimestamp(),
                "id of the vertex should be non-null: " + vertex);
        }
    }

    public void assertEdgeSanity(EdgesResult edgesResult) {
        for (Edge edge : edgesResult.getResults()) {
            Assert.assertNotNull(edge.get_id(), "id of an edge can't be null: " + edge);
            Assert.assertNotNull(edge.get_type(), "_type of an edge can't be null: " + edge);
            Assert.assertNotNull(edge.get_label(), "_label of an edge can't be null: " + edge);
            Assert.assertNotNull(edge.get_inV(), "_inV of an edge can't be null: " + edge);
            Assert.assertNotNull(edge.get_outV(), "_outV of an edge can't be null: " + edge);
        }
    }
}
