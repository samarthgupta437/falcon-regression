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
import org.apache.falcon.regression.core.response.ProcessInstancesResult.WorkflowStatus;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseSingleClusterTests;
import org.apache.oozie.client.Job;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.*;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class ProcessInstanceRunningTest extends BaseSingleClusterTests {

    String baseTestHDFSDir = baseHDFSDir + "/ProcessInstanceRunningTest";
    String feedInputPath = baseTestHDFSDir + "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    String feedOutputPath = baseTestHDFSDir + "/output-data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";

    private Bundle b = new Bundle();

    public ProcessInstanceRunningTest() throws IOException {
        super();
    }

    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {
        Util.print("in @BeforeClass");
        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");

        Bundle bundle = (Bundle) Util.readELBundles()[0][0];
        bundle.generateUniqueBundle();
        bundle = new Bundle(bundle, server1.getEnvFileName());

        String startDate = "2010-01-01T20:00Z";
        String endDate = "2010-01-03T01:04Z";

        bundle.setInputFeedDataPath(feedInputPath);
        String prefix = bundle.getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), server1FS);
        DateTime startDateJoda = new DateTime(InstanceUtil.oozieDateToDate(startDate));
        DateTime endDateJoda = new DateTime(InstanceUtil.oozieDateToDate(endDate));

        List<String> dataDates = Util.getMinuteDatesOnEitherSide(startDateJoda, endDateJoda, 20);
        for (int i = 0; i < dataDates.size(); i++)
            dataDates.set(i, prefix + dataDates.get(i));

        ArrayList<String> dataFolder = new ArrayList<String>();

        for (String dataDate : dataDates) {
            dataFolder.add(dataDate);
        }
        HadoopUtil.flattenAndPutDataInFolder(server1FS, "src/test/resources/OozieExampleInputData/normalInput", dataFolder);
    }


    @BeforeMethod(alwaysRun = true)
    public void setup(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        b = (Bundle) Util.readELBundles()[0][0];
        b = new Bundle(b, server1.getEnvFileName());
        b.setInputFeedDataPath(feedInputPath);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        b.deleteBundle(prism);
    }

    @Test(groups = {"singleCluster"})
    public void getResumedProcessInstance() throws Exception {
        b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T02:30Z");
        b.setProcessPeriodicity(5, TimeUnit.minutes);
        b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
        b.setOutputFeedLocationData(feedOutputPath);
        b.setProcessConcurrency(3);
        b.submitAndScheduleBundle(prism);
        Thread.sleep(15000);
        prism.getProcessHelper().suspend(URLS.SUSPEND_URL, b.getProcessData());
        Thread.sleep(15000);
        prism.getProcessHelper().resume(URLS.RESUME_URL, b.getProcessData());
        Thread.sleep(15000);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getRunningInstance(URLS.INSTANCE_RUNNING,
                        Util.readEntityName(b.getProcessData()));
        InstanceUtil.validateSuccess(r, b, WorkflowStatus.RUNNING);
    }


    @Test(groups = {"singleCluster"})
    public void getSuspendedProcessInstance() throws Exception {
        b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T02:30Z");
        b.setProcessPeriodicity(5, TimeUnit.minutes);
        b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
        b.setOutputFeedLocationData(feedOutputPath);
        b.setProcessConcurrency(3);
        b.submitAndScheduleBundle(prism);
        prism.getProcessHelper().suspend(URLS.SUSPEND_URL, b.getProcessData());
        Thread.sleep(5000);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getRunningInstance(URLS.INSTANCE_RUNNING,
                        Util.readEntityName(b.getProcessData()));
        InstanceUtil.validateSuccessWOInstances(r);
    }


    @Test(groups = {"singleCluster"})
    public void getRunningProcessInstance() throws Exception {
        b = new Bundle(b, server1.getEnvFileName());
        b.setCLusterColo("ua2");
        b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T02:30Z");
        b.setProcessPeriodicity(5, TimeUnit.minutes);
        b.submitAndScheduleBundle(prism);
        Thread.sleep(5000);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getRunningInstance(URLS.INSTANCE_RUNNING,
                        Util.readEntityName(b.getProcessData()));
        InstanceUtil.validateSuccess(r, b, WorkflowStatus.RUNNING);
    }

    @Test(groups = {"singleCluster"})
    public void getNonExistenceProcessInstance() throws Exception {
        ProcessInstancesResult r =
                prism.getProcessHelper()
                        .getRunningInstance(URLS.INSTANCE_RUNNING, "invalidName");
        if (!(r.getStatusCode() == 777))
            AssertJUnit.assertTrue(false);
    }


    @Test(groups = {"singleCluster"})
    public void getKilledProcessInstance() throws Exception {
        b.submitAndScheduleBundle(prism);
        prism.getProcessHelper().delete(URLS.DELETE_URL, b.getProcessData());
        Thread.sleep(5000);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getRunningInstance(URLS.INSTANCE_RUNNING,
                        Util.readEntityName(b.getProcessData()));
        if (!(r.getStatusCode() == 777))
            AssertJUnit.assertTrue(false);
    }


    @Test(groups = {"singleCluster"})
    public void getSucceededProcessInstance() throws Exception {
        b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:11Z");
        b.submitAndScheduleBundle(prism);
        Job.Status status = null;
        for (int i = 0; i < 45; i++) {
            status = InstanceUtil.getDefaultCoordinatorStatus(server1,
                    Util.getProcessName(b.getProcessData()), 0);
            if (status.equals(Job.Status.SUCCEEDED))
                break;
            Thread.sleep(45000);
        }
        Assert.assertNotNull(status);
        Assert.assertEquals(status, Job.Status.SUCCEEDED,
                "The job did not succeeded even in long time");

        ProcessInstancesResult result = prism.getProcessHelper()
                .getRunningInstance(URLS.INSTANCE_RUNNING,
                        Util.readEntityName(b.getProcessData()));
        InstanceUtil.validateSuccessWOInstances(result);
    }


    @AfterClass(alwaysRun = true)
    public void deleteData() throws Exception {
        Util.print("in @AfterClass");
        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");
        Bundle b = (Bundle) Util.readELBundles()[0][0];
        b = new Bundle(b, server1.getEnvFileName());
        b.setInputFeedDataPath(feedInputPath);
        String prefix = b.getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), server1FS);
    }
}
