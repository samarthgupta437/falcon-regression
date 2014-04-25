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
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@Test(groups = "embedded")
public class RescheduleKilledProcessTest extends BaseTestClass {

    ColoHelper cluster = servers.get(0);
    FileSystem clusterFS = serverFS.get(0);
    String aggregateWorkflowDir = baseHDFSDir + "/RescheduleKilledProcessTest/aggregator";
    private static final Logger logger = Logger.getLogger(RescheduleKilledProcessTest.class);

    @BeforeClass
    public void uploadWorkflow() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        logger.info("test name: " + method.getName());
        bundles[0] = BundleUtil.readELBundles()[0][0];
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        removeBundles();
    }

    @Test(enabled = false, timeOut = 1200000)
    public void recheduleKilledProcess() throws Exception {
        // submit and schedule a process with error in workflow .
        //it will get killed
        //generate bundles according to config files
        String processStartTime = TimeUtil.getTimeWrtSystemTime(-11);
        String processEndTime = TimeUtil.getTimeWrtSystemTime(06);
        String process = bundles[0].getProcessData();
        process = InstanceUtil.setProcessName(process, "zeroInputProcess" + new Random().nextInt());
        List<String> feed = new ArrayList<String>();
        feed.add(BundleUtil.getOutputFeedFromBundle(bundles[0]));
        process = bundles[0].setProcessFeeds(process, feed, 0, 0, 1);

        process = InstanceUtil.setProcessCluster(process, null,
                XmlUtil.createProcessValidity(processStartTime, "2099-01-01T00:00Z"));
        process = InstanceUtil
                .setProcessCluster(process, Util.readClusterName(bundles[0].getClusters().get(0)),
                        XmlUtil.createProcessValidity(processStartTime, processEndTime));
        bundles[0].setProcessData(process);

        bundles[0].submitAndScheduleBundle(prism);

        prism.getProcessHelper().delete(URLS.DELETE_URL, bundles[0].getProcessData());
        prism.getProcessHelper().submitEntity(URLS.SUBMIT_URL, bundles[0].getProcessData());
        prism.getProcessHelper().schedule(URLS.SCHEDULE_URL, bundles[0].getProcessData());

    }


    @Test(enabled = true, timeOut = 1200000)
    public void recheduleKilledProcess02() throws Exception {
        // submit and schedule a process with error in workflow .
        //it will get killed
        bundles[0].setProcessValidity(TimeUtil.getTimeWrtSystemTime(-11),
                TimeUtil.getTimeWrtSystemTime(06));

        bundles[0].setInputFeedDataPath(
                baseHDFSDir + "/rawLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");


        String prefix = InstanceUtil.getFeedPrefix(BundleUtil.getInputFeedFromBundle(bundles[0]));
        HadoopUtil.deleteDirIfExists(prefix.substring(1), clusterFS);
        Util.lateDataReplenish(cluster, 40, 1, prefix, null);

        logger.info("process: " + bundles[0].getProcessData());

        bundles[0].submitAndScheduleBundle(prism);

        prism.getProcessHelper().delete(URLS.DELETE_URL, bundles[0].getProcessData());
        prism.getProcessHelper().submitEntity(URLS.SUBMIT_URL, bundles[0].getProcessData());
        prism.getProcessHelper().schedule(URLS.SCHEDULE_URL, bundles[0].getProcessData());
        prism.getProcessHelper().delete(URLS.DELETE_URL, bundles[0].getProcessData());
        prism.getProcessHelper().submitEntity(URLS.SUBMIT_URL, bundles[0].getProcessData());
        prism.getProcessHelper().schedule(URLS.SCHEDULE_URL, bundles[0].getProcessData());

    }
}
