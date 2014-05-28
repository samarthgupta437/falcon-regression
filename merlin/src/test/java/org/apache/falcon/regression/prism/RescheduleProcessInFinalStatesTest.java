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
import org.apache.falcon.entity.v0.Frequency.TimeUnit;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.oozie.client.Job.Status;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;


@Test(groups = "embedded")
public class RescheduleProcessInFinalStatesTest extends BaseTestClass {

    ColoHelper cluster = servers.get(0);
    FileSystem clusterFS = serverFS.get(0);
    String baseTestDir = baseHDFSDir + "/RescheduleProcessInFinalStates";
    String aggregateWorkflowDir = baseTestDir + "/aggregator";
    private static final Logger logger = Logger.getLogger(RescheduleProcessInFinalStatesTest.class);

    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {

        logger.info("in @BeforeClass");
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);

        Bundle b = BundleUtil.readELBundles()[0][0];
        b.generateUniqueBundle();
        b = new Bundle(b, cluster);
        b.setProcessWorkflow(aggregateWorkflowDir);

        String startDate = "2010-01-01T20:00Z";
        String endDate = "2010-01-03T01:04Z";

        b.setInputFeedDataPath(baseTestDir + "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
        String prefix = b.getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), clusterFS);

        DateTime startDateJoda = new DateTime(TimeUtil.oozieDateToDate(startDate));
        DateTime endDateJoda = new DateTime(TimeUtil.oozieDateToDate(endDate));

        List<String> dataDates =
            TimeUtil.getMinuteDatesOnEitherSide(startDateJoda, endDateJoda, 20);

        for (int i = 0; i < dataDates.size(); i++)
            dataDates.set(i, prefix + dataDates.get(i));

        ArrayList<String> dataFolder = new ArrayList<String>();

        for (String dataDate : dataDates)
            dataFolder.add(dataDate);

        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.NORMAL_INPUT, dataFolder);
    }


    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        logger.info("test name: " + method.getName());
        bundles[0] = BundleUtil.readELBundles()[0][0];
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].setInputFeedDataPath(baseTestDir + "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:15Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(
            baseTestDir + "/output-data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
        bundles[0].setProcessConcurrency(6);
        bundles[0].submitAndScheduleBundle(prism);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeBundles();
    }


    // DWE mean Done With Error In Oozie
    @Test(enabled = false)
    public void rescheduleSucceeded() throws Exception {
        InstanceUtil
            .waitForBundleToReachState(cluster, bundles[0].getProcessName(), Status.SUCCEEDED, 20);
        Thread.sleep(20000);

        //delete the process
        prism.getProcessHelper().delete(URLS.DELETE_URL, bundles[0].getProcessData());

        //check ... get definition should return process not found
        ServiceResponse r = prism.getProcessHelper()
            .getEntityDefinition(URLS.GET_ENTITY_DEFINITION, bundles[0].getProcessData());
        Assert.assertTrue(r.getMessage().contains("(process) not found"));
        AssertUtil.assertFailed(r);

        //submit and schedule process again
        r = prism.getProcessHelper()
            .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[0].getProcessData());
        AssertUtil.assertSucceeded(r);
        Thread.sleep(20000);
        InstanceUtil
            .waitForBundleToReachState(cluster, bundles[0].getProcessName(), Status.SUCCEEDED, 20);

    }

    @Test(enabled = false)
    public void rescheduleFailed() throws Exception {
        InstanceUtil
            .waitForBundleToReachState(cluster, bundles[0].getProcessName(), Status.SUCCEEDED, 20);
        Thread.sleep(20000);

        //delete the process
        prism.getProcessHelper().delete(URLS.DELETE_URL, bundles[0].getProcessData());

        //check ... get definition should return process not found
        ServiceResponse r = prism.getProcessHelper()
            .getEntityDefinition(URLS.GET_ENTITY_DEFINITION, bundles[0].getProcessData());
        Assert.assertTrue(r.getMessage().contains("(process) not found"));
        AssertUtil.assertFailed(r);

        //submit and schedule process again
        r = prism.getProcessHelper()
            .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[0].getProcessData());
        AssertUtil.assertSucceeded(r);
        Thread.sleep(20000);
        InstanceUtil
            .waitForBundleToReachState(cluster, bundles[0].getProcessName(), Status.SUCCEEDED, 20);
    }

    @Test(enabled = false)
    public void rescheduleDWE() throws Exception {
        Thread.sleep(20000);

        prism.getProcessHelper()
            .getProcessInstanceKill(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:05Z");

        InstanceUtil
            .waitForBundleToReachState(cluster, bundles[0].getProcessName(), Status.DONEWITHERROR,
                20);

        Thread.sleep(20000);

        //delete the process
        prism.getProcessHelper().delete(URLS.DELETE_URL, bundles[0].getProcessData());

        //check ... get definition should return process not found
        ServiceResponse r = prism.getProcessHelper()
            .getEntityDefinition(URLS.GET_ENTITY_DEFINITION, bundles[0].getProcessData());
        Assert.assertTrue(r.getMessage().contains("(process) not found"));
        AssertUtil.assertFailed(r);

        //submit and schedule process again
        r = prism.getProcessHelper()
            .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[0].getProcessData());
        AssertUtil.assertSucceeded(r);
        Thread.sleep(20000);
        InstanceUtil
            .waitForBundleToReachState(cluster, bundles[0].getProcessName(), Status.SUCCEEDED, 20);

    }

    @Test(enabled = false)
    public void rescheduleKilled() throws Exception {
        Thread.sleep(15000);

        prism.getProcessHelper().delete(URLS.DELETE_URL, bundles[0].getProcessData());


        InstanceUtil
            .waitForBundleToReachState(cluster, bundles[0].getProcessName(), Status.KILLED, 20);

        Thread.sleep(20000);

        //check ... get definition should return process not found
        ServiceResponse r = prism.getProcessHelper()
            .getEntityDefinition(URLS.GET_ENTITY_DEFINITION, bundles[0].getProcessData());
        Assert.assertTrue(r.getMessage().contains("(process) not found"));
        AssertUtil.assertFailed(r);

        //submit and schedule process again
        r = prism.getProcessHelper()
            .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[0].getProcessData());
        AssertUtil.assertSucceeded(r);
        Thread.sleep(20000);
        InstanceUtil
            .waitForBundleToReachState(cluster, bundles[0].getProcessName(), Status.SUCCEEDED, 20);
    }
}
