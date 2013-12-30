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
import org.apache.falcon.regression.core.response.ProcessInstancesResult;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.TestClassHelper;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.WorkflowAction.Status;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class ProcessInstanceRerunTest extends TestClassHelper {

    String feedInputPath = "/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";//baseTestHDFSDir + datePattern;
    String feedOutputPath = "/examples/samarth/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";

    Bundle b = new Bundle();

    public ProcessInstanceRerunTest() throws IOException {
        super();
    }


    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {

        Util.print("in @BeforeClass");

        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");

        Bundle b = (Bundle) Util.readELBundles()[0][0];
        b = new Bundle(b, server2.getEnvFileName());
        b = new Bundle(b, server2.getEnvFileName());

        String startDate = "2010-01-01T20:00Z";
        String endDate = "2010-01-03T01:04Z";

        b.setInputFeedDataPath(feedInputPath);
        String prefix = b.getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), server2FS);

        DateTime startDateJoda = new DateTime(InstanceUtil.oozieDateToDate(startDate));
        DateTime endDateJoda = new DateTime(InstanceUtil.oozieDateToDate(endDate));

        List<String> dataDates = Util.getMinuteDatesOnEitherSide(startDateJoda, endDateJoda, 20);

        for (int i = 0; i < dataDates.size(); i++)
            dataDates.set(i, prefix + dataDates.get(i));

        ArrayList<String> dataFolder = new ArrayList<String>();

        for (String dataDate : dataDates) {
            dataFolder.add(dataDate);
        }
        HadoopUtil.flattenAndPutDataInFolder(server2FS, "src/test/resources/OozieExampleInputData/normalInput", dataFolder);
    }


    @BeforeMethod(alwaysRun = true)
    public void setup(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        b = (Bundle) Util.readELBundles()[0][0];
        b = new Bundle(b, server2.getEnvFileName());
        b.setInputFeedDataPath(feedInputPath);
    }


    @AfterMethod(alwaysRun = true)
    public void tearDown(Method method) throws Exception {
        Util.print("tearDown " + method.getName());
        b.deleteBundle(prism);
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceRerun_someKilled02() throws Exception {
        b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:26Z");
        b.setProcessPeriodicity(5, TimeUnit.minutes);
        b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
        b.setOutputFeedLocationData(feedOutputPath);
        b.setProcessConcurrency(5);
        b.submitAndScheduleBundle(prism);
        Thread.sleep(15000);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getProcessInstanceKill(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:16Z");
        InstanceUtil.validateResponse(r, 4, 0, 0, 0, 4);
        prism.getProcessHelper()
                .getProcessInstanceRerun(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:11Z");
        Thread.sleep(15000);
        InstanceUtil
                .areWorkflowsRunning(server2, Util.readEntityName(b.getProcessData()), 6, 5, 1,
                        0);
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceRerun_multipleSucceededDeleted() throws Exception {
        try {
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:11Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedLocationData(feedOutputPath);
            b.setProcessConcurrency(3);
            b.submitAndScheduleBundle(prism);
            prism.getProcessHelper()
                    .getProcessInstanceRerun(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z&end=2010-01-02T01:11Z");
            Thread.sleep(15000);
        } finally {
            b.deleteBundle(prism);
            prism.getProcessHelper()
                    .getProcessInstanceRerun(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z&end=2010-01-02T01:11Z");
            prism.getProcessHelper()
                    .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z&end=2010-01-02T01:11Z");
        }
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceRerun_multipleKilled() throws Exception {
        b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:11Z");
        b.setProcessPeriodicity(5, TimeUnit.minutes);
        b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
        b.setOutputFeedLocationData(feedOutputPath);
        b.setProcessConcurrency(5);

        Util.print("process: " + b.getProcessData());

        b.submitAndScheduleBundle(prism);
        Thread.sleep(15000);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getProcessInstanceKill(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:11Z");
        InstanceUtil.validateResponse(r, 3, 0, 0, 0, 3);
        prism.getProcessHelper()
                .getProcessInstanceRerun(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:11Z");
        Thread.sleep(5000);
        InstanceUtil
                .areWorkflowsRunning(server2, Util.readEntityName(b.getProcessData()), 3, 3, 0,
                        0);
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceRerun_someKilled01() throws Exception {
        b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:26Z");
        b.setProcessPeriodicity(5, TimeUnit.minutes);
        b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
        b.setOutputFeedLocationData(feedOutputPath);
        b.setProcessConcurrency(5);
        b.submitAndScheduleBundle(prism);
        Thread.sleep(15000);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getProcessInstanceKill(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:11Z");
        InstanceUtil.validateResponse(r, 3, 0, 0, 0, 3);
        prism.getProcessHelper()
                .getProcessInstanceRerun(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:11Z");
        Thread.sleep(5000);
        InstanceUtil
                .areWorkflowsRunning(server2, Util.readEntityName(b.getProcessData()), 6, 6, 0,
                        0);
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceRerun_deleted() throws Exception {
        b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:04Z");
        b.setProcessPeriodicity(5, TimeUnit.minutes);
        b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
        b.setOutputFeedLocationData(feedOutputPath);
        b.setProcessConcurrency(1);
        b.submitAndScheduleBundle(prism);
        Thread.sleep(15000);
        prism.getProcessHelper()
                .getProcessInstanceKill(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:00Z");
        Thread.sleep(15000);
        prism.getProcessHelper()
                .getProcessInstanceRerun(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:00Z");
        Thread.sleep(15000);
        Assert.assertTrue(InstanceUtil.isWorkflowRunning(
                InstanceUtil.getWorkflows(server2, Util.getProcessName(b.getProcessData()),
                        Status.RUNNING)
                        .get(0)));
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceRerun_singleKilled() throws Exception {
        b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:04Z");
        b.setProcessPeriodicity(5, TimeUnit.minutes);
        b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
        b.setOutputFeedLocationData(feedOutputPath);
        b.setProcessConcurrency(1);
        b.submitAndScheduleBundle(prism);
        Thread.sleep(25000);
        prism.getProcessHelper()
                .getProcessInstanceKill(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:00Z");
        Thread.sleep(25000);
        prism.getProcessHelper()
                .getProcessInstanceRerun(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:00Z");
        Thread.sleep(25000);
        Assert.assertTrue(InstanceUtil.isWorkflowRunning(
                InstanceUtil.getWorkflows(server2, Util.getProcessName(b.getProcessData()),
                        Status.RUNNING)
                        .get(0)));
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceRerun_singleSucceeded() throws Exception {
        b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:04Z");
        b.setProcessPeriodicity(5, TimeUnit.minutes);
        b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
        b.setOutputFeedLocationData(feedOutputPath);
        b.setProcessConcurrency(6);
        b.submitAndScheduleBundle(prism);
        Thread.sleep(180000);
        prism.getProcessHelper()
                .getProcessInstanceRerun(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:00Z");
        Thread.sleep(15000);
        Assert.assertTrue(InstanceUtil.isWorkflowRunning(
                InstanceUtil.getWorkflows(server2, Util.getProcessName(b.getProcessData()),
                        Status.RUNNING)
                        .get(0)));
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceRerun_singleSuspended() throws Exception {
        b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:06Z");
        b.setProcessPeriodicity(5, TimeUnit.minutes);
        b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
        b.setOutputFeedLocationData(feedOutputPath);
        b.setProcessConcurrency(2);
        b.submitAndScheduleBundle(prism);
        Thread.sleep(15000);
        prism.getProcessHelper()
                .getProcessInstanceSuspend(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:06Z");
        Thread.sleep(15000);
        prism.getProcessHelper()
                .getProcessInstanceRerun(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:06Z");
        Thread.sleep(15000);
        Assert.assertEquals(InstanceUtil
                .getInstanceStatus(server2, Util.getProcessName(b.getProcessData()), 0, 1),
                CoordinatorAction.Status.SUSPENDED);
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceRerun_multipleSucceeded() throws Exception {
        b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:11Z");
        b.setProcessPeriodicity(5, TimeUnit.minutes);
        b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
        b.setOutputFeedLocationData(feedOutputPath);
        b.setProcessConcurrency(3);
        b.submitAndScheduleBundle(prism);
        prism.getProcessHelper()
                .getProcessInstanceRerun(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:11Z");
        Thread.sleep(15000);
        InstanceUtil
                .areWorkflowsRunning(server2, Util.readEntityName(b.getProcessData()), 3, 3, 0,
                        0);
    }

    @Test(groups = {"singleCluster"})
    public void testProcessInstanceRerun_timedOut() throws Exception {
        b.setInputFeedDataPath("/samarthData/timedout/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
        b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:11Z");
        b.setProcessPeriodicity(5, TimeUnit.minutes);
        b.setProcessTimeOut(2, TimeUnit.minutes);
        b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
        b.setOutputFeedLocationData(feedOutputPath);
        b.setProcessConcurrency(3);
        b.submitAndScheduleBundle(prism);
        org.apache.oozie.client.CoordinatorAction.Status s = null;
        while (!org.apache.oozie.client.CoordinatorAction.Status.TIMEDOUT.equals(s)) {
            s = InstanceUtil
                    .getInstanceStatus(server2, Util.readEntityName(b.getProcessData()), 0, 0);
            Thread.sleep(15000);
        }
        prism.getProcessHelper()
                .getProcessInstanceRerun(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:11Z");
        Thread.sleep(15000);
        s = InstanceUtil
                .getInstanceStatus(server2, Util.readEntityName(b.getProcessData()), 0, 0);
        Assert.assertTrue(org.apache.oozie.client.CoordinatorAction.Status.WAITING.equals(s),
                "instance should have been in WAITING state");
    }


    @AfterClass(alwaysRun = true)
    public void deleteData() throws Exception {
        Util.print("in @AfterClass");
        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");
        Bundle b = (Bundle) Util.readELBundles()[0][0];
        b = new Bundle(b, server2.getEnvFileName());
        b.setInputFeedDataPath(feedInputPath);
        String prefix = b.getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), server2FS);
    }
}
