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

import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.generated.feed.ClusterType;
import org.apache.falcon.regression.core.generated.feed.Feed;
import org.apache.falcon.regression.core.generated.feed.Retention;
import org.apache.falcon.regression.core.generated.feed.Validity;
import org.apache.log4j.Logger;
import org.testng.Assert;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

public class PrismUtil {

    String cluster2Data = null;
    private static Logger logger = Logger.getLogger(PrismUtil.class);

/*
    public static void verifyClusterSubmission(ServiceResponse r, String clusterData, String env,
                                               String expectedStatus,
                                               List<String> beforeSubmit, List<String> afterSubmit)
     {

        if (expectedStatus.equals("SUCCEEDED")) {
            Assert.assertEquals(r.getMessage().contains("SUCCEEDED"), true,
                    "submit cluster status was not succeeded");
            compareDataStoreStates(beforeSubmit, afterSubmit, Util.readClusterName(clusterData));
            Assert.assertEquals(StringUtils.countMatches(r.getMessage(), "(cluster)"), 1,
                    "exactly one colo cluster submission should have been there");
        }
    }
*/

    /*public static void compareDataStoreStates(List<String> initialState, List<String> finalState,
                                              String filename)
     {
        finalState.removeAll(initialState);

        Assert.assertEquals(finalState.size(), 1);
        Assert.assertTrue(finalState.get(0).contains(filename));

    }*/

    public static void compareDataStoreStates(List<String> initialState,
                                              List<String> finalState, String filename,
                                              int expectedDiff) {

        if (expectedDiff > -1) {
            finalState.removeAll(initialState);
            Assert.assertEquals(finalState.size(), expectedDiff);
            if (expectedDiff != 0)
                Assert.assertTrue(finalState.get(0).contains(filename));
        } else {
            expectedDiff = expectedDiff * -1;
            initialState.removeAll(finalState);
            Assert.assertEquals(initialState.size(), expectedDiff);
            if (expectedDiff != 0)
                Assert.assertTrue(initialState.get(0).contains(filename));
        }


    }

    public static void compareDataStoreStates(List<String> initialState,
                                              List<String> finalState, int expectedDiff) {

        if (expectedDiff > -1) {
            finalState.removeAll(initialState);
            Assert.assertEquals(finalState.size(), expectedDiff);

        } else {
            expectedDiff = expectedDiff * -1;
            initialState.removeAll(finalState);
            Assert.assertEquals(initialState.size(), expectedDiff);

        }


    }

    private org.apache.falcon.regression.core.generated.cluster.Cluster getClusterElement(
            Bundle bundle) throws JAXBException {
        JAXBContext jc = JAXBContext
                .newInstance(org.apache.falcon.regression.core.generated.cluster.Cluster.class);
        Unmarshaller u = jc.createUnmarshaller();
        return (org.apache.falcon.regression.core.generated.cluster.Cluster) u
                .unmarshal((new StringReader(bundle.getClusterData())));
    }


    public Bundle setFeedCluster(Validity v1, Retention r1, String n1, ClusterType t1, Validity v2,
                                 Retention r2,
                                 String n2, ClusterType t2) throws IOException, JAXBException {
        Bundle bundle = BundleUtil.readELBundles()[0][0];
        bundle.generateUniqueBundle();
        org.apache.falcon.regression.core.generated.feed.Cluster c1 =
                new org.apache.falcon.regression.core.generated.feed.Cluster();
        c1.setName(n1);
        c1.setRetention(r1);
        c1.setType(t1);
        c1.setValidity(v1);

        org.apache.falcon.regression.core.generated.feed.Cluster c2 =
                new org.apache.falcon.regression.core.generated.feed.Cluster();
        c2.setName(n2);
        c2.setRetention(r2);
        c2.setType(t2);
        c2.setValidity(v2);

        if (v1.getStart().toString().equals("allNull"))
            c1 = c2 = null;

        org.apache.falcon.regression.core.generated.cluster.Cluster clusterElement =
                getClusterElement(bundle);
        clusterElement.setName(n1);
        writeClusterElement(bundle, clusterElement);

        Feed feedElement = getFeedElement(bundle);
        feedElement.getClusters().getCluster().set(0, c1);
        feedElement.getClusters().getCluster().add(c2);
        writeFeedElement(bundle, feedElement);


        clusterElement.setName(n2);
        JAXBContext jc = JAXBContext
                .newInstance(org.apache.falcon.regression.core.generated.cluster.Cluster.class);
        java.io.StringWriter sw = new StringWriter();
        Marshaller marshaller = jc.createMarshaller();
        marshaller.marshal(clusterElement, sw);
        logger.info("modified cluster 2 is: " + sw);
        cluster2Data = sw.toString();
        return bundle;
    }

    private void writeClusterElement(Bundle bundle,
                                     org.apache.falcon.regression.core.generated.cluster.Cluster
                                             clusterElement) throws JAXBException {
        JAXBContext jc = JAXBContext
                .newInstance(org.apache.falcon.regression.core.generated.cluster.Cluster.class);
        java.io.StringWriter sw = new StringWriter();
        Marshaller marshaller = jc.createMarshaller();
        marshaller.marshal(clusterElement, sw);
        logger.info("modified cluster is: " + sw);
        bundle.setClusterData(sw.toString());
    }

    public Feed getFeedElement(Bundle bundle) throws JAXBException {
        JAXBContext jc = JAXBContext.newInstance(Feed.class);
        Unmarshaller u = jc.createUnmarshaller();
        return (Feed) u.unmarshal((new StringReader(bundle.getDataSets().get(0))));
    }

    public void writeFeedElement(Bundle bundle, Feed feedElement) throws JAXBException {
        JAXBContext jc = JAXBContext.newInstance(Feed.class);
        java.io.StringWriter sw = new StringWriter();
        Marshaller marshaller = jc.createMarshaller();
        marshaller.marshal(feedElement, sw);
        List<String> bundleData = new ArrayList<String>();
        logger.info("modified dataset is: " + sw);
        bundleData.add(sw.toString());
        bundleData.add(bundle.getDataSets().get(1));
        bundle.setDataSets(bundleData);
    }

}
