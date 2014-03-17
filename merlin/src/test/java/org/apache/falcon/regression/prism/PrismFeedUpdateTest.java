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

import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.generated.feed.ActionType;
import org.apache.falcon.regression.core.generated.feed.ClusterType;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;


@Test(groups = "distributed")
public class PrismFeedUpdateTest extends BaseTestClass {

    ColoHelper cluster1 = servers.get(0);
    ColoHelper cluster2 = servers.get(1);
    FileSystem server1FS = serverFS.get(0);
    String baseTestDir = baseHDFSDir + "/PrismFeedUpdateTest";
    public final String cluster1colo = cluster1.getClusterHelper().getColo().split("=")[0];
    public final String cluster2colo = cluster2.getClusterHelper().getColo().split("=")[0];


    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        bundles[0] = Util.readELBundles()[0][0];
        bundles[0].generateUniqueBundle();

        bundles[1] = Util.readELBundles()[0][0];
        bundles[1].generateUniqueBundle();

        //generate bundles according to config files
        bundles[0] = new Bundle(bundles[0], cluster1.getEnvFileName(), cluster1.getPrefix());
        bundles[1] = new Bundle(bundles[1], cluster2.getEnvFileName(), cluster2.getPrefix());
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        removeBundles();
    }

    @Test(enabled = true, timeOut = 1200000)
    public void updateFeedQueue_dependentMultipleProcess_oneProcessZeroInput() throws Exception {
        //cluster1colo and cluster2colo are source. feed01 on cluster1colo target cluster2colo, feed02 on cluster2colo target cluster1colo

        //get 3 unique bundles
        //set cluster colos
        bundles[0].setCLusterColo(cluster1colo);
        Util.print("cluster bundles[0]: " + bundles[0].getClusters().get(0));

        bundles[1].setCLusterColo(cluster2colo);
        Util.print("cluster bundles[1]: " + bundles[1].getClusters().get(0));

        //submit 3 clusters

        //get 2 unique feeds
        String feed01 = Util.getInputFeedFromBundle(bundles[0]);
        String outputFeed = Util.getOutputFeedFromBundle(bundles[0]);

        //set source and target for the 2 feeds

        //set clusters to null;
        feed01 = InstanceUtil
                .setFeedCluster(feed01,
                        XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                        XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
                        ClusterType.SOURCE, null, null);
        outputFeed = InstanceUtil
                .setFeedCluster(outputFeed,
                        XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                        XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
                        ClusterType.SOURCE, null, null);


        //set new feed input data
        feed01 = Util.setFeedPathValue(feed01,
                baseTestDir + "/feed01/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/");


        //generate data in both the colos cluster1colo and cluster2colo
        String prefix = InstanceUtil.getFeedPrefix(feed01);
        HadoopUtil.deleteDirIfExists(prefix.substring(1), server1FS);
        Util.lateDataReplenish(cluster1, 70, 1, prefix, null);

        String startTime = InstanceUtil.getTimeWrtSystemTime(-50);

        //set clusters for feed01
        feed01 = InstanceUtil
                .setFeedCluster(feed01, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                        XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                        Util.readClusterName(bundles[0].getClusters().get(0)), ClusterType.SOURCE,
                        null, null);
        feed01 = InstanceUtil
                .setFeedCluster(feed01, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                        XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                        Util.readClusterName(bundles[1].getClusters().get(0)), ClusterType.TARGET,
                        null, null);

        //set clusters for output feed
        outputFeed = InstanceUtil.setFeedCluster(outputFeed,
                XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(bundles[0].getClusters().get(0)), ClusterType.SOURCE, null, null);
        outputFeed = InstanceUtil.setFeedCluster(outputFeed,
                XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(bundles[1].getClusters().get(0)), ClusterType.TARGET, null, null);


        //submit and schedule feeds
        Util.print("feed01: " + feed01);
        Util.print("outputFeed: " + outputFeed);

        //create 2 process with 2 clusters

        //get first process
        String process01 = bundles[0].getProcessData();

        //add clusters to process

        String processStartTime = InstanceUtil.getTimeWrtSystemTime(-11);
        String processEndTime = InstanceUtil.getTimeWrtSystemTime(70);


        process01 = InstanceUtil
                .setProcessCluster(process01, null,
                        XmlUtil.createProcessValidity(startTime, "2099-01-01T00:00Z"));
        process01 = InstanceUtil
                .setProcessCluster(process01, Util.readClusterName(bundles[0].getClusters().get(0)),
                        XmlUtil.createProcessValidity(processStartTime, processEndTime));
        process01 = InstanceUtil
                .setProcessCluster(process01, Util.readClusterName(bundles[1].getClusters().get(0)),
                        XmlUtil.createProcessValidity(processStartTime, processEndTime));

        //get 2nd process :
        String process02 = process01;
        process02 = InstanceUtil
                .setProcessName(process02, "zeroInputProcess" + new Random().nextInt());
        List<String> feed = new ArrayList<String>();
        feed.add(outputFeed);
        process02 = bundles[0].setProcessFeeds(process02, feed, 0, 0, 1);


        //submit and schedule both process
        Util.print("process: " + process01);
        Util.print("process: " + process02);


        Util.print("Wait till process goes into running ");

			        //change feed location path
        outputFeed = Util.setFeedProperty(outputFeed, "queueName", "myQueue");

        Util.print("updated feed: " + outputFeed);

        //update feed first time
        prism.getFeedHelper().update(outputFeed, outputFeed);
    }

}
