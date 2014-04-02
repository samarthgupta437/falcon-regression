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
import org.apache.falcon.regression.core.generated.dependencies.Frequency.TimeUnit;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ProcessInstancesResult;
import org.apache.falcon.regression.core.response.ProcessInstancesResult.WorkflowStatus;
import org.apache.falcon.regression.core.response.ResponseKeys;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.oozie.client.CoordinatorAction.Status;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Process instance status tests.
 */
@Test(groups = "embedded")
public class ProcessInstanceStatusTest extends BaseTestClass {

    ColoHelper cluster = servers.get(0);
    FileSystem clusterFS = serverFS.get(0);
    String baseTestHDFSDir = baseHDFSDir + "/ProcessInstanceStatusTest";
    String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    String feedInputPath = baseTestHDFSDir + "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    String feedOutputPath = baseTestHDFSDir + "/output-data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    String feedInputTimedOutPath = baseTestHDFSDir + "/timedoutStatus/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    String feedOutputTimedOutPath = baseTestHDFSDir + "/output-data/timedoutStatus/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";

    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {

        Util.print("in @BeforeClass");

        HadoopUtil.uploadDir(clusterFS, aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);

        Bundle bundle = Util.readELBundles()[0][0];
        bundle.generateUniqueBundle();
        bundle = new Bundle(bundle, cluster);

        String startDate = "2010-01-01T20:00Z";
        String endDate = "2010-01-03T01:04Z";

        bundle.setInputFeedDataPath(feedInputPath);
        String prefix = bundle.getFeedDataPathPrefix();

        HadoopUtil.deleteDirIfExists(prefix.substring(1), clusterFS);

        DateTime startDateJoda = new DateTime(InstanceUtil.oozieDateToDate(startDate));
        DateTime endDateJoda = new DateTime(InstanceUtil.oozieDateToDate(endDate));

        List<String> dataDates = Util.getMinuteDatesOnEitherSide(startDateJoda, endDateJoda, 20);

        for (int i = 0; i < dataDates.size(); i++)
            dataDates.set(i, prefix + dataDates.get(i));

        ArrayList<String> dataFolder = new ArrayList<String>();

        for (String dataDate : dataDates) {
            dataFolder.add(dataDate);
        }

        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.NORMAL_INPUT, dataFolder);
    }


    @BeforeMethod(alwaysRun = true)
    public void setup(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        bundles[0] = Util.readELBundles()[0][0];
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].setInputFeedDataPath(feedInputPath);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        removeBundles();
    }

    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatus_StartAndEnd_checkNoInstanceAfterEndDate()
            throws Exception {
        //time out is set as 3 minutes .... getStatus is for a large range in past.
        //6 instance should be materialized and one in running and other in waiting
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-03T10:22Z");
        bundles[0].setProcessTimeOut(3, TimeUnit.minutes);
        bundles[0].setProcessPeriodicity(1, TimeUnit.minutes);
        bundles[0].setProcessConcurrency(1);
        bundles[0].submitAndScheduleBundle(prism);
        Thread.sleep(5000);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getProcessInstanceStatus(Util.readEntityName(bundles[0].getProcessData()),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T10:20Z");
        InstanceUtil.validateSuccess(r, bundles[0], WorkflowStatus.RUNNING);
        InstanceUtil.validateResponse(r, 6, 1, 0, 5, 0);
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatus_onlyStartAfterMat() throws Exception {
        //time out is set as 3 minutes .... getStatus is for a large range in past.
        //6 instance should be materialized and one in running and other in waiting
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-03T10:22Z");
        bundles[0].setProcessTimeOut(3, TimeUnit.minutes);
        bundles[0].setProcessPeriodicity(1, TimeUnit.minutes);
        bundles[0].setProcessConcurrency(1);
        bundles[0].submitAndScheduleBundle(prism);
        Thread.sleep(5000);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getProcessInstanceStatus(Util.readEntityName(bundles[0].getProcessData()),
                        "?start=2010-01-02T05:00Z");
        Util.assertSucceeded(r);
        Assert.assertEquals(r.getInstances(), null);
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatus_EndOutOfRange() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:22Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].submitAndScheduleBundle(prism);
        Thread.sleep(15000);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getProcessInstanceStatus(Util.readEntityName(bundles[0].getProcessData()),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:30Z");
        InstanceUtil.validateSuccessWithStatusCode(r, 400);
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatus_dateEmpty() {
        try {
            bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T02:30Z");
            bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
            bundles[0].submitAndScheduleBundle(prism);
            ProcessInstancesResult r = prism.getProcessHelper()
                    .getProcessInstanceStatus(Util.readEntityName(bundles[0].getProcessData()), "");
            InstanceUtil.validateSuccessWithStatusCode(r, ResponseKeys.UNPARSEABLE_DATE);
        } catch (Exception e) {
            if (!e.getMessage().contains("Expected BEGIN_OBJECT but was STRING at line 1 column"))
                Assert.assertTrue(false);
        }
    }

    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatus_StartAndEnd() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:22Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].submitAndScheduleBundle(prism);
        Thread.sleep(5000);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getProcessInstanceStatus(Util.readEntityName(bundles[0].getProcessData()),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:20Z");
        InstanceUtil.validateSuccess(r, bundles[0], WorkflowStatus.RUNNING);
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatus_StartOutOfRange() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:22Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].submitAndScheduleBundle(prism);
        Thread.sleep(5000);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getProcessInstanceStatus(Util.readEntityName(bundles[0].getProcessData()),
                        "?start=2010-01-02T00:00Z&end=2010-01-02T01:20Z");
        InstanceUtil.validateSuccessWithStatusCode(r, 400);
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatus_killed() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:22Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].submitAndScheduleBundle(prism);
        Thread.sleep(15000);
        prism.getProcessHelper().delete(URLS.DELETE_URL, bundles[0].getProcessData());
        Thread.sleep(15000);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getProcessInstanceStatus(Util.readEntityName(bundles[0].getProcessData()),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:20Z");
        if ((r.getStatusCode() != ResponseKeys.PROCESS_NOT_FOUND))
            Assert.assertTrue(false);
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatus_onlyStartSuspended() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:22Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].submitAndScheduleBundle(prism);
        Thread.sleep(15000);
        prism.getProcessHelper().suspend(URLS.SUSPEND_URL, bundles[0].getProcessData());
        Thread.sleep(15000);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getProcessInstanceStatus(Util.readEntityName(bundles[0].getProcessData()),
                        "?start=2010-01-02T01:00Z");
        InstanceUtil.validateSuccessOnlyStart(r, WorkflowStatus.SUSPENDED);
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatus_reverseDateRange() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:22Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].submitAndScheduleBundle(prism);
        Thread.sleep(15000);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getProcessInstanceStatus(Util.readEntityName(bundles[0].getProcessData()),
                        "?start=2010-01-02T01:20Z&end=2010-01-02T01:07Z");
        InstanceUtil.validateSuccessWithStatusCode(r, 400);
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatus_StartEndOutOfRange() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:22Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(
                feedOutputPath);
        bundles[0].setProcessConcurrency(2);
        bundles[0].submitAndScheduleBundle(prism);
        Thread.sleep(15000);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getProcessInstanceStatus(Util.readEntityName(bundles[0].getProcessData()),
                        "?start=2010-01-02T00:00Z&end=2010-01-02T01:30Z");
        InstanceUtil.validateSuccessWithStatusCode(r, 400);
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatus_resumed() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:22Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(2);
        bundles[0].submitAndScheduleBundle(prism);
        Thread.sleep(15000);
        prism.getProcessHelper().suspend(URLS.SUSPEND_URL, bundles[0].getProcessData());
        Thread.sleep(15000);
        prism.getProcessHelper().resume(URLS.RESUME_URL, bundles[0].getProcessData());
        Thread.sleep(15000);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getProcessInstanceStatus(Util.readEntityName(bundles[0].getProcessData()),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:22Z");
        InstanceUtil.validateSuccess(r, bundles[0], WorkflowStatus.RUNNING);
    }

    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatus_onlyStart() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:22Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].submitAndScheduleBundle(prism);
        Thread.sleep(15000);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getProcessInstanceStatus(Util.readEntityName(bundles[0].getProcessData()),
                        "?start=2010-01-02T01:00Z");
        InstanceUtil.validateSuccessOnlyStart(r, WorkflowStatus.RUNNING);
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatus_invalidName() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T02:30Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].submitAndScheduleBundle(prism);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getProcessInstanceStatus("invalidProcess", "?start=2010-01-01T01:00Z");
        if (!(r.getStatusCode() == ResponseKeys.PROCESS_NOT_FOUND))
            Assert.assertTrue(false);
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatus_suspended() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:22Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        for (int i = 0; i < bundles[0].getClusters().size(); i++)
            Util.print("cluster to be submitted: " + i + "  " + bundles[0].getClusters().get(i));
        bundles[0].submitAndScheduleBundle(prism);
        Thread.sleep(5000);
        prism.getProcessHelper().suspend(URLS.SUSPEND_URL, bundles[0].getProcessData());
        Thread.sleep(5000);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getProcessInstanceStatus(Util.readEntityName(bundles[0].getProcessData()),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:20Z");
        InstanceUtil.validateSuccess(r, bundles[0], WorkflowStatus.SUSPENDED);
    }

    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatus_woParams() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T02:30Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].submitAndScheduleBundle(prism);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getProcessInstanceStatus(Util.readEntityName(bundles[0].getProcessData()), null);
        InstanceUtil.validateSuccessWithStatusCode(r, ResponseKeys.UNPARSEABLE_DATE);
    }

    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatus_timedOut() throws Exception {
        //submit
        bundles[0].setInputFeedDataPath(feedInputTimedOutPath);
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:11Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setProcessTimeOut(2, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputTimedOutPath);
        bundles[0].setProcessConcurrency(3);
        bundles[0].submitAndScheduleBundle(prism);
        Status status = null;
        while (status != Status.TIMEDOUT) {
            status = InstanceUtil
                    .getInstanceStatus(cluster, Util.readEntityName(bundles[0].getProcessData()), 0, 0);
            Thread.sleep(15000);
        }
        ProcessInstancesResult r = prism.getProcessHelper()
                .getProcessInstanceStatus(Util.readEntityName(bundles[0].getProcessData()),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:11Z");
        InstanceUtil.validateFailedInstances(r, 3);
    }
}
