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

import org.apache.commons.httpclient.HttpStatus;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.enumsAndConstants.ENTITY_TYPE;
import org.apache.falcon.regression.core.enumsAndConstants.MerlinConstants;
import org.apache.falcon.regression.core.generated.dependencies.Frequency;
import org.apache.falcon.regression.core.generated.process.Input;
import org.apache.falcon.regression.core.generated.process.Inputs;
import org.apache.falcon.regression.core.generated.process.Process;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ProcessInstancesResult;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.KerberosHelper;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.apache.log4j.Logger;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.List;

@Test(groups = "embedded")
public class AuthorizationTest extends BaseTestClass {
    private static final Logger logger = Logger.getLogger(AuthorizationTest.class);

    ColoHelper cluster = servers.get(0);
    FileSystem clusterFS = serverFS.get(0);
    OozieClient clusterOC = serverOC.get(0);
    String aggregateWorkflowDir = baseWorkflowDir + "/aggregator";
    String baseTestDir = baseHDFSDir + "/AuthorizationTest";
    String datePattern = "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    String feedInputPath = baseTestDir + datePattern;

    @BeforeClass
    public void uploadWorkflow() throws Exception {
        HadoopUtil.uploadDir(clusterFS, aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setup(Method method) throws Exception {
        logger.info("test name: " + method.getName());
        Bundle bundle = Util.readELBundles()[0][0];
        bundles[0] = new Bundle(bundle, cluster.getEnvFileName(), cluster.getPrefix());
        bundles[0].generateUniqueBundle();
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
    }

    /**
     * U2Delete test cases
     */
    @Test
    public void U1SubmitU2DeleteCluster() throws Exception {
        bundles[0].submitClusters(prism);
        KerberosHelper.loginFromKeytab(MerlinConstants.USER2_NAME);
        final ServiceResponse serviceResponse = prism.getClusterHelper().delete(
                Util.URLS.DELETE_URL, bundles[0].getClusters().get(0), MerlinConstants.USER2_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
                "Entity submitted by first user should not be deletable by second user");
    }

    @Test
    public void U1SubmitU2DeleteProcess() throws Exception {
        bundles[0].submitClusters(prism);
        bundles[0].submitProcess(true);
        KerberosHelper.loginFromKeytab(MerlinConstants.USER2_NAME);
        final ServiceResponse serviceResponse = prism.getProcessHelper().delete(
                Util.URLS.DELETE_URL, bundles[0].getProcessData(), MerlinConstants.USER2_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
                "Entity submitted by first user should not be deletable by second user");
    }

    @Test
    public void U1SubmitU2DeleteFeed() throws Exception {
        bundles[0].submitClusters(prism);
        bundles[0].submitFeed();
        KerberosHelper.loginFromKeytab(MerlinConstants.USER2_NAME);
        final ServiceResponse serviceResponse = prism.getFeedHelper().delete(
                Util.URLS.DELETE_URL, bundles[0].getDataSets().get(0), MerlinConstants.USER2_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
                "Entity submitted by first user should not be deletable by second user");
    }

    @Test
    public void U1ScheduleU2DeleteProcess()
    throws Exception {
        //submit, schedule process by U1
        bundles[0].submitAndScheduleBundle(prism);
        AssertUtil.checkStatus(clusterOC, ENTITY_TYPE.PROCESS, bundles[0].getProcessData(),
                Job.Status.RUNNING);
        //try to delete process by U2
        KerberosHelper.loginFromKeytab(MerlinConstants.USER2_NAME);
        final ServiceResponse serviceResponse = prism.getProcessHelper().delete(Util.URLS
                .DELETE_URL, bundles[0].getProcessData(), MerlinConstants.USER2_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
                "Process scheduled by first user should not be deleted by second user");
    }

    @Test
    public void U1ScheduleU2DeleteFeed() throws Exception {
        String feed = Util.getInputFeedFromBundle(bundles[0]);
        //submit, schedule feed by U1
        bundles[0].submitClusters(prism);
        Util.assertSucceeded(prism.getFeedHelper().submitAndSchedule(
                Util.URLS.SUBMIT_AND_SCHEDULE_URL, feed));
        AssertUtil.checkStatus(clusterOC, ENTITY_TYPE.FEED, feed, Job.Status.RUNNING);
        //delete feed by U2
        KerberosHelper.loginFromKeytab(MerlinConstants.USER2_NAME);
        final ServiceResponse serviceResponse = prism.getFeedHelper().delete(Util.URLS
                .DELETE_URL, feed, MerlinConstants.USER2_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
                "Feed scheduled by first user should not be deleted by second user");
    }

    @Test
    public void U1SuspendU2DeleteProcess() throws Exception {
        //submit, schedule, suspend process by U1
        bundles[0].submitAndScheduleBundle(prism);
        AssertUtil.checkStatus(clusterOC, ENTITY_TYPE.PROCESS, bundles[0].getProcessData(),
                Job.Status.RUNNING);
        Util.assertSucceeded(prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL,
                bundles[0].getProcessData()));
        AssertUtil.checkStatus(clusterOC, ENTITY_TYPE.PROCESS, bundles[0].getProcessData(),
                Job.Status.SUSPENDED);
        //try to delete process by U2
        KerberosHelper.loginFromKeytab(MerlinConstants.USER2_NAME);
        final ServiceResponse serviceResponse = prism.getProcessHelper().delete(Util.URLS
                .DELETE_URL, bundles[0].getProcessData(), MerlinConstants.USER2_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
                "Process suspended by first user should not be deleted by second user");
    }

    @Test
    public void U1SuspendU2DeleteFeed() throws Exception {
        String feed = Util.getInputFeedFromBundle(bundles[0]);
        //submit, schedule, suspend feed by U1
        bundles[0].submitClusters(prism);
        Util.assertSucceeded(prism.getFeedHelper().submitAndSchedule(
                Util.URLS.SUBMIT_AND_SCHEDULE_URL, feed));
        Util.assertSucceeded(prism.getFeedHelper().suspend(Util.URLS.SUSPEND_URL, feed));
        AssertUtil.checkStatus(clusterOC, ENTITY_TYPE.FEED, feed, Job.Status.SUSPENDED);
        //delete feed by U2
        KerberosHelper.loginFromKeytab(MerlinConstants.USER2_NAME);
        final ServiceResponse serviceResponse = prism.getFeedHelper().delete(Util.URLS
                .DELETE_URL, feed, MerlinConstants.USER2_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
                "Feed scheduled by first user should not be deleted by second user");
    }

    /**
     * U2Suspend test cases
     */
    @Test
    public void U1ScheduleU2SuspendFeed() throws Exception {
        String feed = Util.getInputFeedFromBundle(bundles[0]);
        //submit, schedule by U1
        bundles[0].submitClusters(prism);
        Util.assertSucceeded(prism.getFeedHelper().submitAndSchedule(
                Util.URLS.SUBMIT_AND_SCHEDULE_URL, feed));
        AssertUtil.checkStatus(clusterOC, ENTITY_TYPE.FEED, feed, Job.Status.RUNNING);
        //try to suspend by U2
        KerberosHelper.loginFromKeytab(MerlinConstants.USER2_NAME);
        final ServiceResponse serviceResponse = prism.getFeedHelper().suspend(Util.URLS
                .SUSPEND_URL, feed, MerlinConstants.USER2_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
                "Feed scheduled by first user should not be suspended by second user");
    }

    @Test
    public void U1ScheduleU2SuspendProcess() throws Exception {
        bundles[0].submitAndScheduleBundle(prism);
        AssertUtil.checkStatus(clusterOC, ENTITY_TYPE.PROCESS, bundles[0].getProcessData(),
                Job.Status.RUNNING);
        //try to suspend process by U2
        KerberosHelper.loginFromKeytab(MerlinConstants.USER2_NAME);
        final ServiceResponse serviceResponse = prism.getProcessHelper().suspend(Util.URLS
                .SUSPEND_URL, bundles[0].getProcessData(), MerlinConstants.USER2_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
                "Process scheduled by first user should not be suspended by second user");
    }

    /**
     * U2Resume test cases
     */
    @Test
    public void U1SuspendU2ResumeFeed() throws Exception {
        String feed = Util.getInputFeedFromBundle(bundles[0]);
        //submit, schedule and then suspend feed by User1
        bundles[0].submitClusters(prism);
        Util.assertSucceeded(prism.getFeedHelper().submitAndSchedule(
                Util.URLS.SUBMIT_AND_SCHEDULE_URL, feed));
        Util.assertSucceeded(prism.getFeedHelper().suspend(Util.URLS.SUSPEND_URL, feed));
        AssertUtil.checkStatus(clusterOC, ENTITY_TYPE.FEED, feed, Job.Status.SUSPENDED);
        //try to resume feed by User2
        KerberosHelper.loginFromKeytab(MerlinConstants.USER2_NAME);
        final ServiceResponse serviceResponse = prism.getFeedHelper().resume(Util.URLS
                .RESUME_URL, feed, MerlinConstants.USER2_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
                "Feed suspended by first user should not be resumed by second user");
    }

    @Test
    public void U1SuspendU2ResumeProcess() throws Exception {
        //submit, schedule, suspend process by U1
        bundles[0].submitAndScheduleBundle(prism);
        Util.assertSucceeded(prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL,
                bundles[0].getProcessData()));
        AssertUtil.checkStatus(clusterOC, ENTITY_TYPE.PROCESS, bundles[0].getProcessData(),
                Job.Status.SUSPENDED);
        //try to resume process by U2
        KerberosHelper.loginFromKeytab(MerlinConstants.USER2_NAME);
        final ServiceResponse serviceResponse = prism.getProcessHelper().resume(Util.URLS
                .RESUME_URL, bundles[0].getProcessData(), MerlinConstants.USER2_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
                "Process suspended by first user should not be resumed by second user");
    }

    @Test
    public void U1SuspendU2ResumeProcessInstances() throws Exception {
        String startTime = InstanceUtil.getTimeWrtSystemTime(0);
        String endTime = InstanceUtil.addMinsToTime(startTime, 5);
        String midTime = InstanceUtil.addMinsToTime(startTime, 2);
        Util.print("Start time: " + startTime + "\tEnd time: " + endTime);

        //prepare process definition
        bundles[0].setProcessValidity(startTime, endTime);
        bundles[0].setProcessPeriodicity(1, Frequency.TimeUnit.minutes);
        bundles[0].setProcessConcurrency(5);
        bundles[0].setInputFeedPeriodicity(1, Frequency.TimeUnit.minutes);
        bundles[0].setInputFeedDataPath(feedInputPath);
        bundles[0].setProcessData(setProcessInput(bundles[0], "now(0,0)", "now(0,4)"));

        //provide necessary data for first 3 instances to run
        Util.print("Creating necessary data...");
        String prefix = bundles[0].getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), clusterFS);
        DateTime startDate = new DateTime(InstanceUtil.oozieDateToDate(InstanceUtil.addMinsToTime
                (startTime, -2)));
        DateTime endDate = new DateTime(InstanceUtil.oozieDateToDate(endTime));
        List<String> dataDates = Util.getMinuteDatesOnEitherSide(startDate, endDate, 0);
        Util.print("Creating data in folders: \n" + dataDates);
        for (int i = 0; i < dataDates.size(); i++)
            dataDates.set(i, prefix + dataDates.get(i));
        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.NORMAL_INPUT, dataDates);

        //submit, schedule process by U1
        Util.print("Process data: " + bundles[0].getProcessData());
        bundles[0].submitAndScheduleBundle(prism);

        //check that there are 3 running instances
        InstanceUtil.waitTillInstanceReachState(clusterOC, Util.readEntityName(bundles[0]
                .getProcessData()), 3, CoordinatorAction.Status.RUNNING, 1, ENTITY_TYPE.PROCESS);

        //check that there are 2 waiting instances
        InstanceUtil.waitTillInstanceReachState(clusterOC, Util.readEntityName(bundles[0]
                .getProcessData()), 2, CoordinatorAction.Status.WAITING, 1, ENTITY_TYPE.PROCESS);

        //3 instances should be running , other 2 should be waiting
        ProcessInstancesResult r = prism.getProcessHelper().getProcessInstanceStatus(Util
                .readEntityName(bundles[0].getProcessData()),
                "?start=" + startTime + "&end=" + endTime);
        InstanceUtil.validateResponse(r, 5, 3, 0, 2, 0);

        //suspend 3 running instances
        r = prism.getProcessHelper().getProcessInstanceSuspend(Util
                .readEntityName(bundles[0].getProcessData()),
                "?start=" + startTime + "&end=" + midTime);
        InstanceUtil.validateResponse(r, 3, 0, 3, 0, 0);

        //try to resume suspended instances by U2
        KerberosHelper.loginFromKeytab(MerlinConstants.USER2_NAME);
        r = prism.getProcessHelper().getProcessInstanceResume(Util.readEntityName(bundles[0]
                .getProcessData()), "?start=" + startTime + "&end=" + midTime,
                MerlinConstants.USER2_NAME);

        //the state of above 3 instances should still be suspended
        InstanceUtil.validateResponse(r, 3, 0, 3, 0, 0);

        //check the status of all instances
        r = prism.getProcessHelper().getProcessInstanceStatus(Util
                .readEntityName(bundles[0].getProcessData()),
                "?start=" + startTime + "&end=" + endTime);
        InstanceUtil.validateResponse(r, 5, 0, 3, 2, 0);
    }

    /**
     * U2Kill test cases
     */
    @Test
    public void U1ScheduleU2KillProcessInstances() throws Exception {
        String startTime = InstanceUtil.getTimeWrtSystemTime(0);
        String endTime = InstanceUtil.addMinsToTime(startTime, 5);
        Util.print("Start time: " + startTime + "\tEnd time: " + endTime);

        //prepare process definition
        bundles[0].setProcessValidity(startTime, endTime);
        bundles[0].setProcessPeriodicity(1, Frequency.TimeUnit.minutes);
        bundles[0].setProcessConcurrency(5);
        bundles[0].setInputFeedPeriodicity(1, Frequency.TimeUnit.minutes);
        bundles[0].setInputFeedDataPath(feedInputPath);
        bundles[0].setProcessData(setProcessInput(bundles[0], "now(0,0)", "now(0,4)"));

        //provide necessary data for first 3 instances to run
        Util.print("Creating necessary data...");
        String prefix = bundles[0].getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), clusterFS);
        DateTime startDate = new DateTime(InstanceUtil.oozieDateToDate(InstanceUtil.addMinsToTime
                (startTime, -2)));
        DateTime endDate = new DateTime(InstanceUtil.oozieDateToDate(endTime));
        List<String> dataDates = Util.getMinuteDatesOnEitherSide(startDate, endDate, 0);
        Util.print("Creating data in folders: \n" + dataDates);
        for (int i = 0; i < dataDates.size(); i++)
            dataDates.set(i, prefix + dataDates.get(i));
        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.NORMAL_INPUT, dataDates);

        //submit, schedule process by U1
        Util.print("Process data: " + bundles[0].getProcessData());
        bundles[0].submitAndScheduleBundle(prism);

        //check that there are 3 running instances
        InstanceUtil.waitTillInstanceReachState(clusterOC, Util.readEntityName(bundles[0]
                .getProcessData()), 3, CoordinatorAction.Status.RUNNING, 1, ENTITY_TYPE.PROCESS);

        //3 instances should be running , other 2 should be waiting
        ProcessInstancesResult r = prism.getProcessHelper().getProcessInstanceStatus(Util
                .readEntityName(bundles[0].getProcessData()),
                "?start=" + startTime + "&end=" + endTime);
        InstanceUtil.validateResponse(r, 5, 3, 0, 2, 0);

        //try to kill all instances by U2
        KerberosHelper.loginFromKeytab(MerlinConstants.USER2_NAME);
        r = prism.getProcessHelper().getProcessInstanceKill(Util
                .readEntityName(bundles[0].getProcessData()),
                "?start=" + startTime + "&end=" + endTime, MerlinConstants.USER2_NAME);

        //number of instances should be the same as before
        InstanceUtil.validateResponse(r, 5, 3, 0, 2, 0);
    }

    @Test
    public void U1SuspendU2KillProcessInstances() throws Exception {
        String startTime = InstanceUtil.getTimeWrtSystemTime(0);
        String endTime = InstanceUtil.addMinsToTime(startTime, 5);
        String midTime = InstanceUtil.addMinsToTime(startTime, 2);
        Util.print("Start time: " + startTime + "\tEnd time: " + endTime);

        //prepare process definition
        bundles[0].setProcessValidity(startTime, endTime);
        bundles[0].setProcessPeriodicity(1, Frequency.TimeUnit.minutes);
        bundles[0].setProcessConcurrency(5);
        bundles[0].setInputFeedPeriodicity(1, Frequency.TimeUnit.minutes);
        bundles[0].setInputFeedDataPath(feedInputPath);
        bundles[0].setProcessData(setProcessInput(bundles[0], "now(0,0)", "now(0,4)"));

        //provide necessary data for first 3 instances to run
        Util.print("Creating necessary data...");
        String prefix = bundles[0].getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), clusterFS);
        DateTime startDate = new DateTime(InstanceUtil.oozieDateToDate(InstanceUtil.addMinsToTime
                (startTime, -2)));
        DateTime endDate = new DateTime(InstanceUtil.oozieDateToDate(endTime));
        List<String> dataDates = Util.getMinuteDatesOnEitherSide(startDate, endDate, 0);
        Util.print("Creating data in folders: \n" + dataDates);
        for (int i = 0; i < dataDates.size(); i++)
            dataDates.set(i, prefix + dataDates.get(i));
        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.NORMAL_INPUT, dataDates);

        //submit, schedule process by U1
        Util.print("Process data: " + bundles[0].getProcessData());
        bundles[0].submitAndScheduleBundle(prism);

        //check that there are 3 running instances
        InstanceUtil.waitTillInstanceReachState(clusterOC, Util.readEntityName(bundles[0]
                .getProcessData()), 3, CoordinatorAction.Status.RUNNING, 1, ENTITY_TYPE.PROCESS);

        //check that there are 2 waiting instances
        InstanceUtil.waitTillInstanceReachState(clusterOC, Util.readEntityName(bundles[0]
                .getProcessData()), 2, CoordinatorAction.Status.WAITING, 1, ENTITY_TYPE.PROCESS);

        //3 instances should be running , other 2 should be waiting
        ProcessInstancesResult r = prism.getProcessHelper().getProcessInstanceStatus(Util
                .readEntityName(bundles[0].getProcessData()),
                "?start=" + startTime + "&end=" + endTime);
        InstanceUtil.validateResponse(r, 5, 3, 0, 2, 0);

        //suspend 3 running instances
        r = prism.getProcessHelper().getProcessInstanceSuspend(Util
                .readEntityName(bundles[0].getProcessData()),
                "?start=" + startTime + "&end=" + midTime);
        InstanceUtil.validateResponse(r, 3, 0, 3, 0, 0);

        //try to kill all instances by U2
        KerberosHelper.loginFromKeytab(MerlinConstants.USER2_NAME);
        r = prism.getProcessHelper().getProcessInstanceKill(Util
                .readEntityName(bundles[0].getProcessData()),
                "?start=" + startTime + "&end=" + endTime, MerlinConstants.USER2_NAME);

        //3 should still be suspended, 2 should be waiting
        InstanceUtil.validateResponse(r, 5, 0, 3, 2, 0);
    }

    /**
     * U2Rerun test cases
     */
    @Test
    public void U1KillSomeU2RerunAllProcessInstances()
            throws ParseException, IOException, JAXBException, InterruptedException,
            AuthenticationException, URISyntaxException, OozieClientException {
        String startTime = InstanceUtil.getTimeWrtSystemTime(0);
        String endTime = InstanceUtil.addMinsToTime(startTime, 5);
        String midTime = InstanceUtil.addMinsToTime(startTime, 2);
        Util.print("Start time: " + startTime + "\tEnd time: " + endTime);

        //prepare process definition
        bundles[0].setProcessValidity(startTime, endTime);
        bundles[0].setProcessPeriodicity(1, Frequency.TimeUnit.minutes);
        bundles[0].setProcessConcurrency(5);
        bundles[0].setInputFeedPeriodicity(1, Frequency.TimeUnit.minutes);
        bundles[0].setInputFeedDataPath(feedInputPath);
        bundles[0].setProcessData(setProcessInput(bundles[0], "now(0,0)", "now(0,3)"));

        //provide necessary data for first 4 instances to run
        Util.print("Creating necessary data...");
        String prefix = bundles[0].getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), clusterFS);
        DateTime startDate = new DateTime(InstanceUtil.oozieDateToDate(InstanceUtil.addMinsToTime
                (startTime, -2)));
        DateTime endDate = new DateTime(InstanceUtil.oozieDateToDate(endTime));
        List<String> dataDates = Util.getMinuteDatesOnEitherSide(startDate, endDate, 0);
        Util.print("Creating data in folders: \n" + dataDates);
        for (int i = 0; i < dataDates.size(); i++)
            dataDates.set(i, prefix + dataDates.get(i));
        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.NORMAL_INPUT, dataDates);

        //submit, schedule process by U1
        Util.print("Process data: " + bundles[0].getProcessData());
        bundles[0].submitAndScheduleBundle(prism);

        //check that there are 4 running instances
        InstanceUtil.waitTillInstanceReachState(clusterOC, Util.readEntityName(bundles[0]
                .getProcessData()), 4, CoordinatorAction.Status.RUNNING, 1, ENTITY_TYPE.PROCESS);

        //4 instances should be running , 1 should be waiting
        ProcessInstancesResult r = prism.getProcessHelper().getProcessInstanceStatus(Util
                .readEntityName(bundles[0].getProcessData()),
                "?start=" + startTime + "&end=" + endTime);
        InstanceUtil.validateResponse(r, 5, 4, 0, 1, 0);

        //kill 3 running instances
        r = prism.getProcessHelper().getProcessInstanceKill(Util
                .readEntityName(bundles[0].getProcessData()), "?start=" + startTime + "&end=" +
                midTime);
        InstanceUtil.validateResponse(r, 3, 0, 0, 0, 3);

        //generally 3 instances should be killed, 1 is running and 1 is waiting

        //try to rerun instances by U2
        KerberosHelper.loginFromKeytab(MerlinConstants.USER2_NAME);
        r = prism.getProcessHelper().getProcessInstanceRerun(Util
                .readEntityName(bundles[0].getProcessData()), "?start=" + startTime + "&end=" +
                midTime, MerlinConstants.USER2_NAME);

        //instances should still be killed
        InstanceUtil.validateResponse(r, 3, 0, 0, 0, 3);
    }

    /**
     * U2Update test cases
     */
    @Test
    public void U1SubmitU2UpdateFeed()
            throws URISyntaxException, IOException, AuthenticationException, JAXBException {
        String feed = Util.getInputFeedFromBundle(bundles[0]);
        //submit feed
        bundles[0].submitClusters(prism);
        Util.assertSucceeded(prism.getFeedHelper().submitEntity(Util.URLS.SUBMIT_URL, feed));
        String definition = prism.getFeedHelper()
                .getEntityDefinition(Util.URLS.GET_ENTITY_DEFINITION,
                        feed).getMessage();
        Assert.assertTrue(definition.contains(Util
                .getFeedName(feed)) && !definition.contains("(feed) not found"),
                "Feed should be already submitted");
        //update feed definition
        String newFeed = Util.setFeedPathValue(feed,
                baseHDFSDir + "/randomPath/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/");
        //try to update feed by U2
        KerberosHelper.loginFromKeytab(MerlinConstants.USER2_NAME);
        final ServiceResponse serviceResponse = prism.getFeedHelper().update(feed, newFeed,
                InstanceUtil.getTimeWrtSystemTime(0),
                MerlinConstants.USER2_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
                "Feed submitted by first user should not be updated by second user");
    }

    @Test
    public void U1ScheduleU2UpdateFeed() throws Exception {
        String feed = Util.getInputFeedFromBundle(bundles[0]);
        //submit and schedule feed
        bundles[0].submitClusters(prism);
        Util.assertSucceeded(prism.getFeedHelper().submitAndSchedule(
                Util.URLS.SUBMIT_AND_SCHEDULE_URL, feed));
        AssertUtil.checkStatus(clusterOC, ENTITY_TYPE.FEED, feed, Job.Status.RUNNING);
        //update feed definition
        String newFeed = Util.setFeedPathValue(feed,
                baseHDFSDir + "/randomPath/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/");
        //try to update feed by U2
        KerberosHelper.loginFromKeytab(MerlinConstants.USER2_NAME);
        final ServiceResponse serviceResponse = prism.getFeedHelper().update(feed, newFeed,
                InstanceUtil.getTimeWrtSystemTime(0),
                MerlinConstants.USER2_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
                "Feed scheduled by first user should not be updated by second user");
    }

    @Test
    public void U1SubmitU2UpdateProcess() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:04Z");
        String processName = bundles[0].getProcessName();
        //submit process
        bundles[0].submitBundle(prism);
        String definition = prism.getProcessHelper()
                .getEntityDefinition(Util.URLS.GET_ENTITY_DEFINITION,
                        bundles[0].getProcessData()).getMessage();
        Assert.assertTrue(definition.contains(processName) &&
                !definition.contains("(process) not found"), "Process should be already submitted");
        //update process definition
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2020-01-02T01:04Z");
        //try to update process by U2
        KerberosHelper.loginFromKeytab(MerlinConstants.USER2_NAME);
        final ServiceResponse serviceResponse = prism.getProcessHelper().update(bundles[0]
                .getProcessData(), bundles[0].getProcessData(),
                InstanceUtil.getTimeWrtSystemTime(0),
                MerlinConstants.USER2_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
                "Process submitted by first user should not be updated by second user");
    }

    @Test
    public void U1ScheduleU2UpdateProcess() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:04Z");
        //submit, schedule process by U1
        bundles[0].submitAndScheduleBundle(prism);
        AssertUtil.checkStatus(clusterOC, ENTITY_TYPE.PROCESS, bundles[0].getProcessData(),
                Job.Status.RUNNING);
        //update process definition
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2020-01-02T01:04Z");
        //try to update process by U2
        KerberosHelper.loginFromKeytab(MerlinConstants.USER2_NAME);
        final ServiceResponse serviceResponse = prism.getProcessHelper().update(bundles[0]
                .getProcessData(), bundles[0].getProcessData(),
                InstanceUtil.getTimeWrtSystemTime(0),
                MerlinConstants.USER2_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
                "Process scheduled by first user should not be updated by second user");
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        KerberosHelper.loginFromKeytab(MerlinConstants.CURRENT_USER_NAME);
        removeBundles();
    }

    public String setProcessInput(Bundle bundle, String startEl,
                                  String endEl) throws JAXBException {
        Process process = InstanceUtil.getProcessElement(bundle);
        Inputs inputs = new Inputs();
        Input input = new Input();
        input.setFeed(Util.readEntityName(Util.getInputFeedFromBundle(bundle)));
        input.setStart(startEl);
        input.setEnd(endEl);
        input.setName("inputData");
        inputs.getInput().add(input);
        process.setInputs(inputs);
        return InstanceUtil.processToString(process);
    }

}
