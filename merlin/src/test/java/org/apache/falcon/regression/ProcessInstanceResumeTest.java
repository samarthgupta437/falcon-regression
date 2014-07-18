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
import org.apache.falcon.entity.v0.Frequency.TimeUnit;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.InstancesResult;
import org.apache.falcon.regression.core.response.InstancesResult.WorkflowStatus;
import org.apache.falcon.regression.core.response.ResponseKeys;
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
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.List;

/**
 * Process instance resume tests.
 */
@Test(groups = "embedded")
public class ProcessInstanceResumeTest extends BaseTestClass {

    ColoHelper cluster = servers.get(0);
    FileSystem clusterFS = serverFS.get(0);
    String baseTestHDFSDir = baseHDFSDir + "/ProcessInstanceResumeTest";
    String feedInputPath = baseTestHDFSDir + "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    String feedOutputPath =
        baseTestHDFSDir + "/output-data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    private static final Logger logger = Logger.getLogger(ProcessInstanceResumeTest.class);

    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {
        logger.info("in @BeforeClass");
        HadoopUtil.uploadDir(clusterFS, aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
        Bundle b = BundleUtil.readELBundle();
        b = new Bundle(b, cluster);
        b = new Bundle(b, cluster);
        String startDate = "2010-01-01T23:20Z";
        String endDate = "2010-01-02T01:40Z";
        b.setInputFeedDataPath(feedInputPath);
        String prefix = b.getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), clusterFS);

        List<String> dataDates = TimeUtil.getMinuteDatesOnEitherSide(startDate, endDate, 20);
        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.NORMAL_INPUT, prefix, dataDates);
    }

    @BeforeMethod(alwaysRun = true)
    public void setup(Method method) throws Exception {
        logger.info("setup " + method.getName());
        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle();
        bundles[0].setInputFeedDataPath(feedInputPath);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown(Method method) {
        logger.info("tearDown " + method.getName());
        removeBundles();
    }

    /**
     * Schedule process. Suspend some instances. Attempt to -resume instance using single -end
     * parameter results in failure.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceResume_onlyEnd() throws Exception {
        bundles[0].setProcessConcurrency(6);
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:26Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].submitAndScheduleBundle(prism);
        prism.getProcessHelper()
            .getProcessInstanceSuspend(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:05Z&end=2010-01-02T01:21Z");
        InstancesResult result = prism.getProcessHelper()
            .getProcessInstanceStatus(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:00Z&end=2010-01-02T01:26Z");
        InstanceUtil.validateResponse(result, 6, 2, 4, 0, 0);

        result = prism.getProcessHelper()
            .getProcessInstanceResume(Util.readEntityName(bundles[0].getProcessData()),
                "?end=2010-01-02T01:15Z");
        InstanceUtil.validateSuccessWithStatusCode(result, ResponseKeys.UNPARSEABLE_DATE);
    }

    /**
     * Schedule process. Suspend some instances. Try to perform -resume using time range which
     * effects only on one instance. Check that this instance was resumed es expected.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceResume_resumeSome() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:26Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setProcessConcurrency(6);
        bundles[0].submitAndScheduleBundle(prism);
        TimeUtil.sleepSeconds(15);
        prism.getProcessHelper()
            .getProcessInstanceSuspend(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:05Z&end=2010-01-02T01:21Z");
        TimeUtil.sleepSeconds(10);
        InstancesResult result = prism.getProcessHelper()
            .getProcessInstanceStatus(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:00Z&end=2010-01-02T01:26Z");
        InstanceUtil.validateResponse(result, 6, 2, 4, 0, 0);

        prism.getProcessHelper()
            .getProcessInstanceResume(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:05Z&end=2010-01-02T01:16Z");
        result = prism.getProcessHelper()
            .getProcessInstanceStatus(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:00Z&end=2010-01-02T01:26Z");
        InstanceUtil.validateResponse(result, 6, 5, 1, 0, 0);
    }

    /**
     * Schedule process. Suspend some instances. Try to perform -resume using time range which
     * effects on all instances. Check that there are no suspended instances.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceResume_resumeMany() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:26Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setProcessConcurrency(6);
        bundles[0].submitAndScheduleBundle(prism);
        prism.getProcessHelper()
            .getProcessInstanceSuspend(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:05Z&end=2010-01-02T01:20Z");
        InstancesResult result = prism.getProcessHelper()
            .getProcessInstanceStatus(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:00Z&end=2010-01-02T01:26Z");
        InstanceUtil.validateResponse(result, 6, 2, 4, 0, 0);

        prism.getProcessHelper()
            .getProcessInstanceResume(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:05Z&end=2010-01-02T01:20Z");
        result = prism.getProcessHelper()
            .getProcessInstanceStatus(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:00Z&end=2010-01-02T01:26Z");
        InstanceUtil.validateResponse(result, 6, 6, 0, 0, 0);
    }

    /**
     * Schedule process. Suspend first instance. Resume that instance using only -start parameter.
     * Check that mentioned instance was resumed.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceResume_single() throws Exception {
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setProcessConcurrency(1);
        bundles[0].submitAndScheduleBundle(prism);
        prism.getProcessHelper()
            .getProcessInstanceSuspend(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:00Z");
        prism.getProcessHelper()
            .getProcessInstanceResume(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:00Z");
        InstancesResult r = prism.getProcessHelper()
            .getProcessInstanceStatus(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:00Z");
        InstanceUtil.validateSuccessOnlyStart(r, WorkflowStatus.RUNNING);
    }

    /**
     * Attempt to resume instances of non-existent process should fail with an appropriate
     * status code.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceResume_nonExistent() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:26Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setProcessConcurrency(6);
        bundles[0].submitAndScheduleBundle(prism);
        InstancesResult r =
            prism.getProcessHelper()
                .getProcessInstanceResume("invalidName",
                    "?start=2010-01-02T01:00Z&end=2010-01-02T01:15Z");
        InstanceUtil.validateSuccessWithStatusCode(r, ResponseKeys.PROCESS_NOT_FOUND);
    }

    /**
     * Attempt to perform -resume action without time range parameters should fail with an
     + appropriate status code or message.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceResume_noParams() throws Exception {
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setProcessConcurrency(6);
        bundles[0].submitAndScheduleBundle(prism);
        InstancesResult r =
                prism.getProcessHelper().getProcessInstanceResume(
                        Util.readEntityName(bundles[0].getProcessData()), null);
        InstanceUtil.validateSuccessWithStatusCode(r, ResponseKeys.UNPARSEABLE_DATE);
    }

    /**
     * Schedule process, remove it. Try to -resume it's instance. Attempt should fail with
     * an appropriate status code.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceResume_deleted() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:26Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setProcessConcurrency(6);
        bundles[0].submitAndScheduleBundle(prism);
        prism.getProcessHelper().delete(URLS.DELETE_URL, bundles[0].getProcessData());
        InstancesResult r = prism.getProcessHelper()
            .getProcessInstanceResume(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:05Z");
        InstanceUtil.validateSuccessWithStatusCode(r, ResponseKeys.PROCESS_NOT_FOUND);
    }

    /**
     * Schedule process. Try to resume entity which wasn't suspended.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceResume_nonSuspended() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:26Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setProcessConcurrency(6);
        bundles[0].submitAndScheduleBundle(prism);
        prism.getProcessHelper()
            .getProcessInstanceResume(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:05Z");
        InstancesResult result = prism.getProcessHelper()
            .getProcessInstanceStatus(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:05Z");
        InstanceUtil.validateResponse(result, 1, 1, 0, 0, 0);
        result = prism.getProcessHelper()
            .getProcessInstanceResume(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:05Z");
        InstanceUtil.validateResponse(result, 1, 1, 0, 0, 0);
    }

    /**
     * Schedule process. Suspend last instance. Resume it using parameter which points to
     * expected materialization time of last instance. Check that there are no suspended
     * instances among all which belong to current process.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceResume_lastInstance() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:26Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setProcessConcurrency(6);
        bundles[0].submitAndScheduleBundle(prism);
        prism.getProcessHelper()
            .getProcessInstanceSuspend(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:25Z");
        InstancesResult result = prism.getProcessHelper()
            .getProcessInstanceStatus(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:00Z&end=2010-01-02T01:25Z");
        InstanceUtil.validateResponse(result, 6, 5, 1, 0, 0);
        prism.getProcessHelper()
            .getProcessInstanceResume(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:25Z");
        result = prism.getProcessHelper()
            .getProcessInstanceStatus(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:00Z&end=2010-01-02T01:25Z");
        InstanceUtil.validateResponse(result, 6, 6, 0, 0, 0);
    }

    /**
     * Schedule process. Suspend all instances except the first and the last using appropriate
     * -start/-end parameters. Resume that instances. Check that there are no suspended ones.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceResume_withinRange() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:26Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setProcessConcurrency(6);
        bundles[0].submitAndScheduleBundle(prism);
        prism.getProcessHelper()
            .getProcessInstanceSuspend(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:05Z&end=2010-01-02T01:20Z");
        InstancesResult result = prism.getProcessHelper()
            .getProcessInstanceStatus(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:00Z&end=2010-01-02T01:26Z");
        InstanceUtil.validateResponse(result, 6, 2, 4, 0, 0);

        prism.getProcessHelper()
            .getProcessInstanceResume(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:05Z&end=2010-01-02T01:20Z");
        result = prism.getProcessHelper()
            .getProcessInstanceStatus(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:00Z&end=2010-01-02T01:26Z");
        InstanceUtil.validateResponse(result, 6, 6, 0, 0, 0);
    }


    @AfterClass(alwaysRun = true)
    public void deleteData() throws Exception {
        logger.info("in @AfterClass");

        Bundle b = BundleUtil.readELBundle();
        b = new Bundle(b, cluster);
        b.setInputFeedDataPath(feedInputPath);
        String prefix = b.getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), clusterFS);
    }
}
