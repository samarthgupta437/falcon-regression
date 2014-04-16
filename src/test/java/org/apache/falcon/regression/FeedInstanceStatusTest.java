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

package org.apache.falcon.regression;

import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.generated.feed.ActionType;
import org.apache.falcon.regression.core.generated.feed.ClusterType;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ProcessInstancesResult;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;


/**
 * Feed instance status tests.
 */
@Test(groups = {"distributed", "embedded"} )
public class FeedInstanceStatusTest extends BaseTestClass {

    private String baseTestDir = baseHDFSDir + "/FeedInstanceStatusTest";
    private String feedInputPath = baseTestDir + "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/";
    private String aggregateWorkflowDir = baseTestDir + "/aggregator";

    ColoHelper cluster2 = servers.get(1);
    ColoHelper cluster3 = servers.get(2);
    FileSystem cluster2FS = serverFS.get(1);
    FileSystem cluster3FS = serverFS.get(2);
    private static final Logger logger = Logger.getLogger(FeedInstanceStatusTest.class);

    public void uploadWorkflow() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void testName(Method method) throws Exception {
        logger.info("test name: " + method.getName());
        Bundle bundle = Util.readELBundles()[0][0];
        for (int i = 0; i < 3; i++) {
            bundles[i] = new Bundle(bundle, servers.get(i));
            bundles[i].generateUniqueBundle();
            bundles[i].setProcessWorkflow(aggregateWorkflowDir);
        }
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        removeBundles();
    }

    @Test(groups = {"multiCluster"})
    public void feedInstanceStatus_running() throws Exception {
        bundles[0].setInputFeedDataPath(feedInputPath);

        bundles[0].setCLusterColo("ua1");
        logger.info("cluster bundle1: " + bundles[0].getClusters().get(0));

        ServiceResponse r = prism.getClusterHelper()
                .submitEntity(URLS.SUBMIT_URL, bundles[0].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));


        bundles[1].setCLusterColo("ua2");
        logger.info("cluster bundle2: " + bundles[1].getClusters().get(0));
        r = prism.getClusterHelper()
                .submitEntity(URLS.SUBMIT_URL, bundles[1].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));


        bundles[2].setCLusterColo("ua3");
        logger.info("cluster bundle3: " + bundles[2].getClusters().get(0));
        r = prism.getClusterHelper()
                .submitEntity(URLS.SUBMIT_URL, bundles[2].getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));


        String feed = bundles[0].getDataSets().get(0);
        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
                ClusterType.SOURCE, null);
        String startTime = InstanceUtil.getTimeWrtSystemTime(-50);


        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTime,
                InstanceUtil.addMinsToTime(startTime, 65)),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(bundles[1].getClusters().get(0)), ClusterType.SOURCE,
                "US/${cluster.colo}");
        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 20),
                        InstanceUtil.addMinsToTime(startTime, 85)),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(bundles[0].getClusters().get(0)), ClusterType.TARGET, null);
        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 40),
                        InstanceUtil.addMinsToTime(startTime, 110)),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(bundles[2].getClusters().get(0)), ClusterType.SOURCE,
                "UK/${cluster.colo}");


        logger.info("feed: " + feed);

        //status before submit
        prism.getFeedHelper()
                .getProcessInstanceStatus(Util.readDatasetName(feed), "?start=" + InstanceUtil
                        .addMinsToTime(startTime, 100) + "&end=" +
                        InstanceUtil.addMinsToTime(startTime, 120));

        r = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
        Thread.sleep(10000);
        prism.getFeedHelper()
                .getProcessInstanceStatus(Util.readDatasetName(feed),
                        "?start=" + startTime + "&end=" + InstanceUtil
                                .addMinsToTime(startTime, 100));

        r = prism.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
        Thread.sleep(15000);

        // both replication instances
        prism.getFeedHelper()
                .getProcessInstanceStatus(Util.readDatasetName(feed),
                        "?start=" + startTime + "&end=" + InstanceUtil
                                .addMinsToTime(startTime, 100));


        // single instance at -30
        prism.getFeedHelper().getProcessInstanceStatus(Util.readDatasetName(feed),
                "?start=" + InstanceUtil
                        .addMinsToTime(startTime, 20));

        //single at -10
        prism.getFeedHelper()
                .getProcessInstanceStatus(Util.readDatasetName(feed), "?start=" + InstanceUtil
                        .addMinsToTime(startTime, 40));


        //single at 10
        prism.getFeedHelper()
                .getProcessInstanceStatus(Util.readDatasetName(feed), "?start=" + InstanceUtil
                        .addMinsToTime(startTime, 40));


        //single at 30
        prism.getFeedHelper()
                .getProcessInstanceStatus(Util.readDatasetName(feed), "?start=" + InstanceUtil
                        .addMinsToTime(startTime, 40));


        String postFix = "/US/ua2";
        String prefix = bundles[0].getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), cluster2FS);
        Util.lateDataReplenish(cluster2, 80, 1, prefix, postFix);


        postFix = "/UK/ua3";
        prefix = bundles[0].getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), cluster3FS);
        Util.lateDataReplenish(cluster3, 80, 1, prefix, postFix);


        // both replication instances
        prism.getFeedHelper()
                .getProcessInstanceStatus(Util.readDatasetName(feed),
                        "?start=" + startTime + "&end=" + InstanceUtil
                                .addMinsToTime(startTime, 100));

        // single instance at -30
        prism.getFeedHelper()
                .getProcessInstanceStatus(Util.readDatasetName(feed), "?start=" + InstanceUtil
                        .addMinsToTime(startTime, 20));

        //single at -10
        prism.getFeedHelper()
                .getProcessInstanceStatus(Util.readDatasetName(feed), "?start=" + InstanceUtil
                        .addMinsToTime(startTime, 40));


        //single at 10
        prism.getFeedHelper()
                .getProcessInstanceStatus(Util.readDatasetName(feed), "?start=" + InstanceUtil
                        .addMinsToTime(startTime, 40));


        //single at 30
        prism.getFeedHelper()
                .getProcessInstanceStatus(Util.readDatasetName(feed), "?start=" + InstanceUtil
                        .addMinsToTime(startTime, 40));


        logger.info("Wait till feed goes into running ");


        //suspend instances -10
        prism.getFeedHelper()
                .getProcessInstanceSuspend(Util.readDatasetName(feed), "?start=" + InstanceUtil
                        .addMinsToTime(startTime, 40));
        prism.getFeedHelper()
                .getProcessInstanceStatus(Util.readDatasetName(feed), "?start=" + InstanceUtil
                        .addMinsToTime(startTime, 20) + "&end=" +
                        InstanceUtil.addMinsToTime(startTime, 40));

        //resuspend -10 and suspend -30 source specific
        prism.getFeedHelper()
                .getProcessInstanceSuspend(Util.readDatasetName(feed),
                        "?start=" + InstanceUtil
                                .addMinsToTime(startTime, 20) + "&end=" +
                                InstanceUtil.addMinsToTime(startTime, 40));
        prism.getFeedHelper()
                .getProcessInstanceStatus(Util.readDatasetName(feed), "?start=" + InstanceUtil
                        .addMinsToTime(startTime, 20) + "&end=" +
                        InstanceUtil.addMinsToTime(startTime, 40));


        //resume -10 and -30
        prism.getFeedHelper()
                .getProcessInstanceResume(Util.readDatasetName(feed), "?start=" + InstanceUtil
                        .addMinsToTime(startTime, 20) + "&end=" +
                        InstanceUtil.addMinsToTime(startTime, 40));
        prism.getFeedHelper()
                .getProcessInstanceStatus(Util.readDatasetName(feed), "?start=" + InstanceUtil
                        .addMinsToTime(startTime, 20) + "&end=" +
                        InstanceUtil.addMinsToTime(startTime, 40));

        //get running instances
        prism.getFeedHelper().getRunningInstance(URLS.INSTANCE_RUNNING, Util.readDatasetName(feed));

        //rerun succeeded instance
        prism.getFeedHelper()
                .getProcessInstanceRerun(Util.readDatasetName(feed), "?start=" + startTime);
        prism.getFeedHelper()
                .getProcessInstanceStatus(Util.readDatasetName(feed),
                        "?start=" + startTime + "&end=" + InstanceUtil
                                .addMinsToTime(startTime, 20));

        //kill instance
        prism.getFeedHelper()
                .getProcessInstanceKill(Util.readDatasetName(feed), "?start=" + InstanceUtil
                        .addMinsToTime(startTime, 44));
        prism.getFeedHelper()
                .getProcessInstanceKill(Util.readDatasetName(feed), "?start=" + startTime);

        //end time should be less than end of validity i.e startTime + 110
        prism.getFeedHelper()
                .getProcessInstanceStatus(Util.readDatasetName(feed),
                        "?start=" + startTime + "&end=" + InstanceUtil
                                .addMinsToTime(startTime, 110));


        //rerun killed instance
        prism.getFeedHelper()
                .getProcessInstanceRerun(Util.readDatasetName(feed), "?start=" + startTime);
        prism.getFeedHelper()
                .getProcessInstanceStatus(Util.readDatasetName(feed),
                        "?start=" + startTime + "&end=" + InstanceUtil
                                .addMinsToTime(startTime, 110));


        //kill feed
        prism.getFeedHelper().delete(URLS.DELETE_URL, feed);
        ProcessInstancesResult responseInstance = prism.getFeedHelper()
                .getProcessInstanceStatus(Util.readDatasetName(feed),
                        "?start=" + startTime + "&end=" + InstanceUtil
                                .addMinsToTime(startTime, 110));

        logger.info(responseInstance.getMessage());
    }
}
