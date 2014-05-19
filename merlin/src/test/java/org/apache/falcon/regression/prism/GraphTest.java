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

package org.apache.falcon.regression.prism;

import org.apache.falcon.regression.core.response.graph.AllEdges;
import org.apache.falcon.regression.core.response.graph.AllVertices;
import org.apache.falcon.regression.core.response.graph.Edge;
import org.apache.falcon.regression.core.response.graph.Vertex;
import org.apache.falcon.regression.core.util.GraphUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "embedded")
public class GraphTest extends BaseTestClass {
    private final Logger logger = Logger.getLogger(GraphTest.class);
    GraphUtil graphUtil;

    @BeforeClass(alwaysRun = true)
    public void setup() {
        graphUtil = new GraphUtil(prism);
    }

    public void testAllVertices() throws Exception {
        final AllVertices allVertices = graphUtil.getAllVertices();
        logger.info(allVertices);
        Assert.assertTrue(allVertices.getTotalSize() > 0, "Total number of vertices should be" +
            " greater that zero but is: " + allVertices.getTotalSize());
        for (Vertex vertex : allVertices.getResults()) {
            assertVertexSanity(vertex);
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

    public void testAllEdges() throws Exception {
        final AllEdges allEdges = graphUtil.getAllEdges();
        logger.info(allEdges);
        Assert.assertTrue(allEdges.getTotalSize() > 0, "Total number of edges should be" +
            " greater that zero but is: " + allEdges.getTotalSize());
        for (Edge edge : allEdges.getResults()) {
            logger.info(edge.get_label());
        }
    }
}
