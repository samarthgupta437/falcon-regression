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
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.supportClasses.Brother;
import org.apache.falcon.regression.core.enumsAndConstants.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;


@Test(groups = "embedded")
public class PrismConcurrentRequestTest extends BaseTestClass {

    ColoHelper cluster = servers.get(0);
    private ThreadGroup brotherGrimm = null;
    private Brother brothers[] = null;
    String aggregateWorkflowDir = baseHDFSDir + "/PrismConcurrentRequest/aggregator";
    private static final Logger logger = Logger.getLogger(PrismConcurrentRequestTest.class);

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setup(Method method) throws Exception {
        logger.info("test name: " + method.getName());
        bundles[0] = BundleUtil.readELBundles()[0][0];
        bundles[0].generateUniqueBundle();
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        brotherGrimm = new ThreadGroup("mixed");
        brothers = new Brother[10];
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        removeBundles();
    }

    @Test(groups = {"multiCluster"})
    public void submitSameFeedParallel() throws Exception {
        prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundles[0].getClusters().get(0));
        for (int i = 1; i <= brothers.length; i++) {
            brothers[i - 1] =
                    new Brother("brother" + i, "submit", ENTITY_TYPE.DATA, brotherGrimm, bundles[0],
                            prism,
                            URLS.SUBMIT_URL);
        }
        for (Brother brother : brothers) {
            brother.start();
        }
        for (Brother brother : brothers) {
            brother.join();
        }
        for (Brother brother : brothers) {
            logger.info(brother.getName() + " output: \n" + Util.prettyPrintXml(brother.getOutput().getMessage()));
            AssertUtil.assertSucceeded(brother.getOutput());
        }
    }

    @Test(groups = {"multiCluster"})
    public void submitSameProcessParallel() throws Exception {
        prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundles[0].getClusters().get(0));
        prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, bundles[0].getDataSets().get(0));
        prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, bundles[0].getDataSets().get(1));
        for (int i = 1; i <= brothers.length; i++) {
            brothers[i - 1] =
                    new Brother("brother" + i, "submit", ENTITY_TYPE.PROCESS, brotherGrimm, bundles[0],
                            prism,
                            URLS.SUBMIT_URL);
        }
        for (Brother brother : brothers) {
            brother.start();
        }
        for (Brother brother : brothers) {
            brother.join();
        }
        for (Brother brother : brothers) {
            logger.info(brother.getName() + " output: \n" + Util.prettyPrintXml(brother.getOutput().getMessage()));
            AssertUtil.assertSucceeded(brother.getOutput());
        }
    }


    @Test(groups = {"multiCluster"})
    public void deleteSameProcessParallel() throws Exception {
        bundles[0].submitBundle(prism);
        for (int i = 1; i <= brothers.length; i++) {
            brothers[i - 1] =
                    new Brother("brother" + i, "delete", ENTITY_TYPE.PROCESS, brotherGrimm, bundles[0],
                            prism,
                            URLS.DELETE_URL);
        }
        for (Brother brother : brothers) {
            brother.start();
        }
        for (Brother brother : brothers) {
            brother.join();
        }
        for (Brother brother : brothers) {
            logger.info(brother.getName() + " output: \n" + Util.prettyPrintXml(brother.getOutput().getMessage()));
            AssertUtil.assertSucceeded(brother.getOutput());
        }
    }


    @Test(groups = {"multiCluster"})
    public void schedulePrismParallel() throws Exception {
        bundles[0].submitBundle(prism);
        for (int i = 1; i <= brothers.length; i++) {
            brothers[i - 1] =
                    new Brother("brother" + i, "schedule", ENTITY_TYPE.PROCESS, brotherGrimm, bundles[0],
                            prism,
                            URLS.SCHEDULE_URL);
        }
        for (Brother brother : brothers) {
            brother.start();
        }
        for (Brother brother : brothers) {
            brother.join();
        }
        for (Brother brother : brothers) {
            logger.info(brother.getName() + " output: \n" + Util.prettyPrintXml(brother.getOutput().getMessage()));
            AssertUtil.assertSucceeded(brother.getOutput());
        }
    }


    @Test(groups = {"multiCluster"})
    public void resumeAnsSuspendParallel() throws Exception {
        brothers = new Brother[4];
        prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundles[0].getClusters().get(0));
        prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, bundles[0].getDataSets().get(0));
        AssertUtil.assertSucceeded(prism.getFeedHelper().schedule(URLS.SCHEDULE_URL, bundles[0].getDataSets().get(0)));
        Thread.sleep(15000);
        prism.getFeedHelper().suspend(URLS.SUSPEND_URL, bundles[0].getDataSets().get(0));
        Thread.sleep(15000);
        for (int i = 1; i <= 2; i++) {
            brothers[i - 1] =
                    new Brother("brother" + i, "resume", ENTITY_TYPE.DATA, brotherGrimm, bundles[0],
                            prism,
                            URLS.RESUME_URL);
        }
        for (int i = 3; i <= 4; i++) {
            brothers[i - 1] =
                    new Brother("brother" + i, "suspend", ENTITY_TYPE.DATA, brotherGrimm, bundles[0],
                            prism,
                            URLS.SUSPEND_URL);
        }
        for (Brother brother : brothers) {
            brother.start();
        }
        for (Brother brother : brothers) {
            brother.join();
        }
        for (Brother brother : brothers) {
            logger.info(brother.getName() + " output: \n" + Util.prettyPrintXml(brother.getOutput().getMessage()));
            AssertUtil.assertSucceeded(brother.getOutput());
        }
    }

    @Test(groups = {"multiCluster"})
    public void resumeParallel() throws Exception {
        prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundles[0].getClusters().get(0));
        prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, bundles[0].getDataSets().get(0));
        AssertUtil.assertSucceeded(prism.getFeedHelper().schedule(URLS.SCHEDULE_URL, bundles[0].getDataSets().get(0)));
        Thread.sleep(15000);
        prism.getFeedHelper().resume(URLS.RESUME_URL, bundles[0].getDataSets().get(0));
        Thread.sleep(5000);
        prism.getFeedHelper().suspend(URLS.SUSPEND_URL, bundles[0].getDataSets().get(0));
        Thread.sleep(15000);
        for (int i = 1; i <= brothers.length; i++) {
            brothers[i - 1] =
                    new Brother("brother" + i, "resume", ENTITY_TYPE.DATA, brotherGrimm, bundles[0],
                            prism,
                            URLS.RESUME_URL);
        }
        for (Brother brother : brothers) {
            brother.start();
        }
        for (Brother brother : brothers) {
            brother.join();
        }
        for (Brother brother : brothers) {
            logger.info(brother.getName() + " output: \n" + Util.prettyPrintXml(brother.getOutput().getMessage()));
            AssertUtil.assertSucceeded(brother.getOutput());
        }
    }


    @Test(groups = {"multiCluster"})
    public void submitSameClusterParallel() throws Exception {
        for (int i = 1; i <= brothers.length; i++) {
            brothers[i - 1] =
                    new Brother("brother" + i, "submit", ENTITY_TYPE.CLUSTER, brotherGrimm, bundles[0],
                            prism,
                            URLS.SUBMIT_URL);
        }
        for (Brother brother : brothers) {
            brother.start();
        }
        for (Brother brother : brothers) {
            brother.join();
        }
        for (Brother brother : brothers) {
            logger.info(brother.getName() + " output: \n" + Util.prettyPrintXml(brother.getOutput().getMessage()));
            AssertUtil.assertSucceeded(brother.getOutput());
        }
    }

}
