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
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.oozie.client.CoordinatorAction;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.*;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Process instance kill tests.
 */
@Test(groups = "embedded")
public class ProcessInstanceKillsTest extends BaseTestClass {

    ColoHelper cluster = servers.get(0);
    FileSystem clusterFS = serverFS.get(0);
    String aggregateWorkflowDir = baseWorkflowDir + "/aggregator";
    String testDir = "/ProcessInstanceKillsTest";
    String baseTestHDFSDir = baseHDFSDir + testDir;
    String feedInputPath = baseTestHDFSDir + "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    String feedOutputPath = baseTestHDFSDir + "/output-data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";

    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {

        Util.print("in @BeforeClass");
        HadoopUtil.uploadDir(clusterFS, aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);

        Bundle b = Util.readELBundles()[0][0];
        b.generateUniqueBundle();
        b = new Bundle(b, cluster.getEnvFileName(), cluster.getPrefix());

        String startDate = "2010-01-01T20:00Z";
        String endDate = "2010-01-03T01:04Z";

        b.setInputFeedDataPath(feedInputPath);
        String prefix = b.getFeedDataPathPrefix();
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
        bundles[0] = new Bundle(bundles[0], cluster.getEnvFileName(), cluster.getPrefix());
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[0].setInputFeedDataPath(feedInputPath);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown(Method method) throws Exception {
        Util.print("tearDown " + method.getName());
        removeBundles();
    }

    @Test(groups = {"singleCluster"})
    public void testProcessInstanceKill_single() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:04Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(1);
        bundles[0].submitAndScheduleBundle(prism);
        Thread.sleep(15000);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getProcessInstanceKill(Util.readEntityName(bundles[0].getProcessData()),
                        "?start=2010-01-02T01:00Z");
        InstanceUtil.validateSuccess(r, bundles[0], WorkflowStatus.KILLED);
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceKill_startAndEndSame() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T00:00Z", "2010-01-02T04:00Z");
        bundles[0].setProcessConcurrency(2);
        bundles[0].setProcessTimeOut(3, TimeUnit.minutes);
        bundles[0].setProcessPeriodicity(1, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(10);
        bundles[0].submitAndScheduleBundle(prism);
        Thread.sleep(15000);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getProcessInstanceKill(Util.readEntityName(bundles[0].getProcessData()),
                        "?start=2010-01-02T00:03Z&end=2010-01-02T00:03Z");
        InstanceUtil.validateResponse(r, 1, 0, 0, 0, 1);
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceKill_killNonMatrelized() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T00:00Z", "2010-01-02T04:00Z");
        bundles[0].setProcessConcurrency(2);
        bundles[0].setProcessTimeOut(3, TimeUnit.minutes);
        bundles[0].setProcessPeriodicity(1, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(10);
        bundles[0].submitAndScheduleBundle(prism);
        Thread.sleep(15000);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getProcessInstanceKill(Util.readEntityName(bundles[0].getProcessData()),
                        "?start=2010-01-02T00:03Z&end=2010-01-02T00:30Z");
        InstanceUtil.validateResponse(r, 3, 0, 0, 0, 3);
        Thread.sleep(15000);
        Util.print(r.toString());
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceKill_bothStartAndEndInFuture01() throws Exception {
        /*
        both start and end r in future with respect to process start end
         */

        String startTime = InstanceUtil.getTimeWrtSystemTime(-20);
        String endTime = InstanceUtil.getTimeWrtSystemTime(400);
        String startTimeData = InstanceUtil.getTimeWrtSystemTime(-150);
        String endTimeData = InstanceUtil.getTimeWrtSystemTime(50);
        InstanceUtil.createDataWithinDatesAndPrefix(cluster,
                InstanceUtil.oozieDateToDate(startTimeData),
                InstanceUtil.oozieDateToDate(endTimeData), baseTestHDFSDir + "/", 1);
        bundles[0].setProcessValidity(startTime, endTime);
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(6);
        bundles[0].submitAndScheduleBundle(prism);
        Thread.sleep(60000);
        String startTimeRequest = InstanceUtil.getTimeWrtSystemTime(-17);
        String endTimeRequest = InstanceUtil.getTimeWrtSystemTime(23);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getProcessInstanceKill(Util.readEntityName(bundles[0].getProcessData()),
                        "?start=" + startTimeRequest + "&end=" + endTimeRequest);
        Util.print(r.toString());
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceKill_bothStartAndEndInFuture() throws Exception {

        /*
         both start and end r in future with respect to current time
          */

        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2099-01-02T01:21Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(6);
        bundles[0].submitAndScheduleBundle(prism);
        Thread.sleep(15000);
        String startTime = InstanceUtil.getTimeWrtSystemTime(1);
        String endTime = InstanceUtil.getTimeWrtSystemTime(40);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getProcessInstanceKill(Util.readEntityName(bundles[0].getProcessData()),
                        "?start=" + startTime + "&end=" + endTime);
        Thread.sleep(15000);
        Util.print(r.getMessage());
        Assert.assertEquals(r.getInstances(), null);
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceKill_multipleInstance() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:21Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(6);
        bundles[0].submitAndScheduleBundle(prism);
        Thread.sleep(15000);
        prism.getProcessHelper()
                .getProcessInstanceKill(Util.readEntityName(bundles[0].getProcessData()),
                        "?start=2010-01-02T01:05Z&end=2010-01-02T01:15Z");
        Thread.sleep(15000);
        ProcessInstancesResult result = prism.getProcessHelper()
                .getProcessInstanceStatus(Util.readEntityName(bundles[0].getProcessData()),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:20Z");
        InstanceUtil.validateResponse(result, 5, 2, 0, 0, 3);
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceKill_lastInstance() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:21Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(6);
        bundles[0].submitAndScheduleBundle(prism);
        Thread.sleep(15000);
        prism.getProcessHelper()
                .getProcessInstanceKill(Util.readEntityName(bundles[0].getProcessData()),
                        "?start=2010-01-02T01:20Z");
        Thread.sleep(15000);
        ProcessInstancesResult result = prism.getProcessHelper()
                .getProcessInstanceStatus(Util.readEntityName(bundles[0].getProcessData()),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:20Z");
        InstanceUtil.validateResponse(result, 5, 4, 0, 0, 1);
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceKill_suspended() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:04Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(1);
        bundles[0].submitAndScheduleBundle(prism);
        Thread.sleep(15000);
        prism.getProcessHelper()
                .getProcessInstanceSuspend(Util.readEntityName(bundles[0].getProcessData()),
                        "?start=2010-01-02T01:00Z");
        Thread.sleep(15000);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getProcessInstanceKill(Util.readEntityName(bundles[0].getProcessData()),
                        "?start=2010-01-02T01:00Z");
        InstanceUtil.validateSuccess(r, bundles[0], WorkflowStatus.KILLED);
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceKill_succeeded() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:04Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(1);
        bundles[0].submitAndScheduleBundle(prism);
        for (int i = 0; i < 30; i++) {
            if (InstanceUtil
                    .getInstanceStatus(cluster, Util.readEntityName(bundles[0].getProcessData()), 0, 0)
                    .equals(CoordinatorAction.Status.SUCCEEDED))
                break;
            Thread.sleep(30000);
        }
        ProcessInstancesResult r = prism.getProcessHelper()
                .getProcessInstanceKill(Util.readEntityName(bundles[0].getProcessData()),
                        "?start=2010-01-02T01:00Z");
        InstanceUtil.validateSuccess(r, bundles[0], WorkflowStatus.SUCCEEDED);
    }


    @AfterClass(alwaysRun = true)
    public void deleteData() throws Exception {
        Util.print("in @AfterClass");

        Bundle b = Util.readELBundles()[0][0];
        b = new Bundle(b, cluster.getEnvFileName(), cluster.getPrefix());

        b.setInputFeedDataPath(feedInputPath);
        String prefix = b.getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), clusterFS);
    }

}
