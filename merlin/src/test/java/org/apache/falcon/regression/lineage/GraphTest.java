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
import org.apache.falcon.regression.core.generated.feed.LocationType;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.interfaces.IEntityManagerHelper;
import org.apache.falcon.regression.core.response.graph.AllEdges;
import org.apache.falcon.regression.core.response.graph.AllVertices;
import org.apache.falcon.regression.core.response.graph.Edge;
import org.apache.falcon.regression.core.response.graph.Vertex;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.helpers.GraphHelper;
import org.apache.falcon.regression.core.util.CleanupUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Random;

@Test(groups = "embedded")
public class GraphTest extends BaseTestClass {
    private final Logger logger = Logger.getLogger(GraphTest.class);
    GraphHelper graphHelper;
    final ColoHelper cluster = servers.get(0);
    final String baseTestHDFSDir = baseHDFSDir + "/GraphTest";
    final String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    final String feedInputPath =
        baseTestHDFSDir + "/input/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    final String feedOutputPath =
        baseTestHDFSDir + "/output/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    // use 5 <= x < 10 input feeds
    final int numInputFeeds = 5 + new Random().nextInt(5);
    // use 5 <= x < 10 output feeds
    final int numOutputFeeds = 5 + new Random().nextInt(5);
    FeedMerlin[] inputFeeds = new FeedMerlin[numInputFeeds];
    FeedMerlin[] outputFeeds = new FeedMerlin[numOutputFeeds];

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
        FeedMerlin inputFeedMerlin = new FeedMerlin(BundleUtil.getInputFeedFromBundle(bundles[0]));
        inputFeedMerlin.setLocation(LocationType.DATA, feedInputPath);
        final String inputFeedString = inputFeedMerlin.toString();
        //submit all input feeds
        for(int count = 0; count < numInputFeeds; ++count) {
            inputFeeds[count] = new FeedMerlin(inputFeedString);
            inputFeeds[count].setName("infeed-" + count + "-" + inputFeedMerlin.getName());
            AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(Util.URLS.SUBMIT_URL,
                inputFeeds[count].toString()));
        }
        FeedMerlin outputFeedMerlin = new FeedMerlin(BundleUtil.getOutputFeedFromBundle(bundles[0]));
        outputFeedMerlin.setLocation(LocationType.DATA, feedOutputPath);
        final String outputFeedString = outputFeedMerlin.toString();
        //submit all output feeds
        for(int count = 0; count < numOutputFeeds; ++count) {
            outputFeeds[count] = new FeedMerlin(outputFeedString);
            outputFeeds[count].setName("outfeed" + count + "-" + outputFeedMerlin.getName());
            AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(Util.URLS.SUBMIT_URL,
                outputFeeds[count].toString()));
        }
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
        final AllVertices allVertices = graphHelper.getAllVertices();
        logger.info(allVertices);
        Assert.assertTrue(allVertices.getTotalSize() > 0, "Total number of vertices should be" +
            " greater that zero but is: " + allVertices.getTotalSize());
        for (Vertex vertex : allVertices.getResults()) {
            assertVertexSanity(vertex);
        }
    }

    public void testAllEdges() throws Exception {
        final AllEdges allEdges = graphHelper.getAllEdges();
        logger.info(allEdges);
        Assert.assertTrue(allEdges.getTotalSize() > 0, "Total number of edges should be" +
            " greater that zero but is: " + allEdges.getTotalSize());
        for (Edge edge : allEdges.getResults()) {
            assertEdgeSanity(edge);
        }
    }

    private void assertVertexSanity(Vertex vertex) {
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

    private void assertEdgeSanity(Edge edge) {
        Assert.assertNotNull(edge.get_id(), "id of an edge can't be null: " + edge);
        Assert.assertNotNull(edge.get_type(), "_type of an edge can't be null: " + edge);
        Assert.assertNotNull(edge.get_label(), "_label of an edge can't be null: " + edge);
        Assert.assertNotNull(edge.get_inV(), "_inV of an edge can't be null: " + edge);
        Assert.assertNotNull(edge.get_outV(), "_outV of an edge can't be null: " + edge);
    }
}
