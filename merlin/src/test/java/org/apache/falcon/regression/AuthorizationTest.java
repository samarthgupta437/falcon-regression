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
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.MerlinConstants;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.supportClasses.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.KerberosHelper;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.apache.log4j.Logger;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;

@Test(groups = "embedded")
public class AuthorizationTest extends BaseTestClass {
    private static final Logger logger = Logger.getLogger(AuthorizationTest.class);

    ColoHelper cluster = servers.get(0);
    FileSystem clusterFS = serverFS.get(0);
    OozieClient clusterOC = serverOC.get(0);
    String aggregateWorkflowDir = baseWorkflowDir + "/aggregator";

    @BeforeClass
    public void uploadWorkflow() throws Exception {
        HadoopUtil.uploadDir(clusterFS, aggregateWorkflowDir, "src/test/resources/oozie");
    }

    @BeforeMethod(alwaysRun = true)
    public void setup(Method method) throws Exception {
        logger.info("test name: " + method.getName());
        bundles[0] = Util.readELBundles()[0][0];
        bundles[0] = new Bundle(bundles[0], cluster.getEnvFileName(), cluster.getPrefix());
        bundles[0].generateUniqueBundle();
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
    }

    @Test
    public void U1SubmitU2DeleteCluster() throws Exception {
        bundles[0].submitClusters(prism);
        KerberosHelper.loginFromKeytab(MerlinConstants.USER2_NAME);
        final ServiceResponse serviceResponse = cluster.getClusterHelper().delete(
                Util.URLS.DELETE_URL, bundles[0].getClusters().get(0), MerlinConstants.USER2_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
                "Entity submitted by first user should not be deletable by second user");
    }

    @Test
    public void U1SubmitU2DeleteProcess() throws Exception {
        bundles[0].submitClusters(prism);
        bundles[0].submitProcess();
        KerberosHelper.loginFromKeytab(MerlinConstants.USER2_NAME);
        final ServiceResponse serviceResponse = cluster.getProcessHelper().delete(
                Util.URLS.DELETE_URL, bundles[0].getProcessData(), MerlinConstants.USER2_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
                "Entity submitted by first user should not be deletable by second user");
    }

    @Test
    public void U1SubmitU2DeleteFeed() throws Exception {
        bundles[0].submitClusters(prism);
        bundles[0].submitFeed();
        KerberosHelper.loginFromKeytab(MerlinConstants.USER2_NAME);
        final ServiceResponse serviceResponse = cluster.getFeedHelper().delete(
                Util.URLS.DELETE_URL, bundles[0].getDataSets().get(0), MerlinConstants.USER2_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
                "Entity submitted by first user should not be deletable by second user");
    }

    @Test
    public void U1SubmitScheduleU2SuspendFeed()
    throws URISyntaxException, IOException, AuthenticationException, JAXBException {
        String feed = Util.getInputFeedFromBundle(bundles[0]);
        //submit, schedule by U1
        bundles[0].submitClusters(prism);
        Util.assertSucceeded(cluster.getFeedHelper().submitAndSchedule(
                Util.URLS.SUBMIT_AND_SCHEDULE_URL, feed));
        //try to suspend by U2
        KerberosHelper.loginFromKeytab(MerlinConstants.USER2_NAME);
        final ServiceResponse serviceResponse = cluster.getFeedHelper().suspend(Util.URLS
                .SUSPEND_URL, feed, MerlinConstants.USER2_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
                "Entity scheduled by first user should not be suspended by second user");
    }

    @Test
    public void U1SubmitScheduleSuspendU2ResumeFeed() throws Exception {
        String feed = Util.getInputFeedFromBundle(bundles[0]);
        //submit, schedule and then suspend feed by User1
        bundles[0].submitClusters(prism);
        Util.assertSucceeded(cluster.getFeedHelper().submitAndSchedule(
                Util.URLS.SUBMIT_AND_SCHEDULE_URL, feed));
        Util.assertSucceeded(cluster.getFeedHelper().suspend(Util.URLS.SUSPEND_URL, feed));
        AssertUtil.checkStatus(clusterOC, ENTITY_TYPE.FEED, feed, Job.Status.SUSPENDED);
        //try to resume feed by User2
        KerberosHelper.loginFromKeytab(MerlinConstants.USER2_NAME);
        final ServiceResponse serviceResponse = cluster.getFeedHelper().resume(Util.URLS
                .RESUME_URL, feed, MerlinConstants.USER2_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
                "Entity suspended by first user should not be resumed by second user");
    }

    @Test
    public void U1SubmitScheduleU2SuspendProcess()
    throws URISyntaxException, IOException, AuthenticationException, JAXBException,
    InterruptedException {
        bundles[0].submitAndScheduleBundle(prism);
        //try to suspend process by U2
        KerberosHelper.loginFromKeytab(MerlinConstants.USER2_NAME);
        final ServiceResponse serviceResponse = cluster.getProcessHelper().suspend(Util.URLS
                .SUSPEND_URL, bundles[0].getProcessData(), MerlinConstants.USER2_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
                "Entity scheduled by first user should not be suspended by second user");
    }

    @Test
    public void U1SubmitScheduleSuspendU2ResumeProcess()
    throws Exception {
        //submit, schedule, suspend process by U1
        bundles[0].submitAndScheduleBundle(prism);
        Util.assertSucceeded(cluster.getProcessHelper().suspend(Util.URLS.SUSPEND_URL,
                bundles[0].getProcessData()));
        AssertUtil.checkStatus(clusterOC, ENTITY_TYPE.PROCESS, bundles[0].getProcessData(),
                Job.Status.SUSPENDED);
        //try to resume process by U2
        KerberosHelper.loginFromKeytab(MerlinConstants.USER2_NAME);
        final ServiceResponse serviceResponse = cluster.getProcessHelper().resume(Util.URLS
                .RESUME_URL, bundles[0].getProcessData(), MerlinConstants.USER2_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
                "Entity suspended by first user should not be resumed by second user");
    }


    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        KerberosHelper.loginFromKeytab(MerlinConstants.CURRENT_USER_NAME);
        removeBundles();
    }

}
