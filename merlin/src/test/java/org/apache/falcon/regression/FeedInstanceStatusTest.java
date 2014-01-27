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
import org.apache.falcon.regression.core.response.ProcessInstancesResult;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.apache.falcon.regression.testHelper.BaseMultiClusterTests;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;


/**
 * Feed instance status tests.
 */
public class FeedInstanceStatusTest extends BaseMultiClusterTests {

    private String baseTestDir = baseHDFSDir + "/FeedInstanceStatusTest";
    private String feedInputPath = baseTestDir + "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/";
    private Bundle b1 = new Bundle();
    private Bundle b2 = new Bundle();
    private Bundle b3 = new Bundle();

    @BeforeMethod(alwaysRun = true)
    public void testName(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        Util.restartService(server1.getClusterHelper());
        Util.restartService(server2.getClusterHelper());

        b1 = (Bundle) Util.readELBundles()[0][0];
        b1.generateUniqueBundle();
        b2 = (Bundle) Util.readELBundles()[0][0];
        b2.generateUniqueBundle();
        b3 = (Bundle) Util.readELBundles()[0][0];
        b3.generateUniqueBundle();
        b1 = new Bundle(b1, server1.getEnvFileName(), server1.getPrefix());
        b2 = new Bundle(b2, server2.getEnvFileName(), server2.getPrefix());
        b3 = new Bundle(b3, server3.getEnvFileName(), server3.getPrefix());
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        b1.deleteBundle(prism);
        b2.deleteBundle(prism);
        b3.deleteBundle(prism);
    }

    @Test(groups = {"multiCluster"})
    public void feedInstanceStatus_running() throws Exception {
        b1.setInputFeedDataPath(feedInputPath);

        b1.setCLusterColo("ua1");
        Util.print("cluster b1: " + b1.getClusters().get(0));

        ServiceResponse r = prism.getClusterHelper()
                .submitEntity(URLS.SUBMIT_URL, b1.getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));


        b2.setCLusterColo("ua2");
        Util.print("cluster b2: " + b2.getClusters().get(0));
        r = prism.getClusterHelper()
                .submitEntity(URLS.SUBMIT_URL, b2.getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));


        b3.setCLusterColo("ua3");
        Util.print("cluster b3: " + b3.getClusters().get(0));
        r = prism.getClusterHelper()
                .submitEntity(URLS.SUBMIT_URL, b3.getClusters().get(0));
        Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));


        String feed = b1.getDataSets().get(0);
        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
                ClusterType.SOURCE, null, null);
        String startTime = InstanceUtil.getTimeWrtSystemTime(-50);


        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTime,
                InstanceUtil.addMinsToTime(startTime, 65)),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(b2.getClusters().get(0)), ClusterType.SOURCE,
                "US/${cluster.colo}", null);
        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 20),
                        InstanceUtil.addMinsToTime(startTime, 85)),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(b1.getClusters().get(0)), ClusterType.TARGET, null, null);
        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 40),
                        InstanceUtil.addMinsToTime(startTime, 110)),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(b3.getClusters().get(0)), ClusterType.SOURCE,
                "UK/${cluster.colo}", null);


        Util.print("feed: " + feed);

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
        String prefix = b1.getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), server2FS);
        Util.lateDataReplenish(server2, 80, 1, prefix, postFix);


        postFix = "/UK/ua3";
        prefix = b1.getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), server3FS);
        Util.lateDataReplenish(server3, 80, 1, prefix, postFix);


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


        Util.print("Wait till feed goes into running ");


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

        prism.getFeedHelper()
                .getProcessInstanceStatus(Util.readDatasetName(feed),
                        "?start=" + startTime + "&end=" + InstanceUtil
                                .addMinsToTime(startTime, 120));


        //rerun killed instance
        prism.getFeedHelper()
                .getProcessInstanceRerun(Util.readDatasetName(feed), "?start=" + startTime);
        prism.getFeedHelper()
                .getProcessInstanceStatus(Util.readDatasetName(feed),
                        "?start=" + startTime + "&end=" + InstanceUtil
                                .addMinsToTime(startTime, 120));


        //kill feed
        prism.getFeedHelper().delete(URLS.DELETE_URL, feed);
        ProcessInstancesResult responseInstance = prism.getFeedHelper()
                .getProcessInstanceStatus(Util.readDatasetName(feed),
                        "?start=" + startTime + "&end=" + InstanceUtil
                                .addMinsToTime(startTime, 120));

        Util.print(responseInstance.getMessage());
    }
}