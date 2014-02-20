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
import org.apache.falcon.regression.core.supportClasses.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;


@Test(groups = "embedded")
public class PrismConcurrentRequest extends BaseTestClass {

    ColoHelper cluster;
    private Bundle b = new Bundle();
    private ThreadGroup brotherGrimm = null;
    private Brother brothers[] = null;
    private int failedResponse = 0;
    private int succeedeResponse = 0;

    public PrismConcurrentRequest(){
        super();
        cluster = servers.get(1);
    }

    @BeforeMethod(alwaysRun = true)
    public void setup(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        b = (Bundle) Util.readELBundles()[0][0];
        b.generateUniqueBundle();
        b = new Bundle(b, cluster.getEnvFileName(), cluster.getPrefix());
        brotherGrimm = new ThreadGroup("mixed");
        brothers = new Brother[10];
        failedResponse = 0;
        succeedeResponse = 0;
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        Thread.sleep(60000);
    }

    @Test(groups = {"multiCluster"})
    public void submitSameFeedParallel() throws Exception {
        prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, b.getClusters().get(0));
        for (int i = 1; i <= brothers.length; i++) {
            brothers[i - 1] =
                    new Brother("brother" + i, "submit", ENTITY_TYPE.DATA, brotherGrimm, b,
                            prism,
                            URLS.SUBMIT_URL);
        }
        for (Brother brother : brothers) {
            brother.start();
        }
        Thread.sleep(60000);
        for (Brother brother : brothers) {
            if (brother.getOutput().getMessage().contains("SUCCEEDED"))
                succeedeResponse++;
            else if (brother.getOutput().getMessage().contains("FAILED"))
                failedResponse++;
        }
        Assert.assertEquals(succeedeResponse, 1);
        Assert.assertEquals(failedResponse, 9);
    }

    @Test(groups = {"multiCluster"})
    public void submitSameProcessParallel() throws Exception {
        prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, b.getClusters().get(0));
        prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, b.getDataSets().get(0));
        prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, b.getDataSets().get(1));
        for (int i = 1; i <= brothers.length; i++) {
            brothers[i - 1] =
                    new Brother("brother" + i, "submit", ENTITY_TYPE.PROCESS, brotherGrimm, b,
                            prism,
                            URLS.SUBMIT_URL);
        }
        for (Brother brother : brothers) {
            brother.start();
        }
        Thread.sleep(60000);
        for (Brother brother : brothers) {
            if (brother.getOutput().getMessage().contains("SUCCEEDED"))
                succeedeResponse++;
            else if (brother.getOutput().getMessage().contains("FAILED"))
                failedResponse++;
        }
        Assert.assertEquals(succeedeResponse, 1);
        Assert.assertEquals(failedResponse, 9);
    }


    @Test(groups = {"multiCluster"})
    public void deleteSameProcessParallel() throws Exception {
        try {
            b.submitBundle(prism);
            for (int i = 1; i <= brothers.length; i++) {
                brothers[i - 1] =
                        new Brother("brother" + i, "delete", ENTITY_TYPE.PROCESS, brotherGrimm, b,
                                prism,
                                URLS.DELETE_URL);
            }
            for (Brother brother : brothers) {
                brother.start();
            }
            Thread.sleep(60000);
            for (Brother brother : brothers) {
                if (brother.getOutput().getMessage().contains("SUCCEEDED"))
                    succeedeResponse++;
                else if (brother.getOutput().getMessage().contains("FAILED"))
                    failedResponse++;
            }
            Assert.assertEquals(succeedeResponse, 1);
            Assert.assertEquals(failedResponse, 9);
        } finally {
            b.deleteBundle(prism);
        }
    }


    @Test(groups = {"multiCluster"})
    public void schedulePrismParallel() throws Exception {
        try{
            b.submitBundle(prism);
            for (int i = 1; i <= brothers.length; i++) {
                brothers[i - 1] =
                        new Brother("brother" + i, "schedule", ENTITY_TYPE.PROCESS, brotherGrimm, b,
                                prism,
                                URLS.SCHEDULE_URL);
            }
            for (Brother brother : brothers) {
                brother.start();
            }
            Thread.sleep(60000);
            for (Brother brother : brothers) {
                if (brother.getOutput().getMessage().contains("SUCCEEDED"))
                    succeedeResponse++;
                else if (brother.getOutput().getMessage().contains("FAILED"))
                    failedResponse++;
            }
            Assert.assertEquals(succeedeResponse, 1);
            Assert.assertEquals(failedResponse, 9);
        } finally {
            b.deleteBundle(prism);
        }
    }


    @Test(groups = {"multiCluster"})
    public void resumeAnsSuspendParallel() throws Exception {
        try{
            brothers = new Brother[4];
            prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, b.getClusters().get(0));
            prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, b.getDataSets().get(0));
            ServiceResponse r =
                    prism.getFeedHelper().schedule(URLS.SCHEDULE_URL, b.getDataSets().get(0));
            Thread.sleep(15000);
            prism.getFeedHelper().suspend(URLS.SUSPEND_URL, b.getDataSets().get(0));
            Thread.sleep(15000);
            for (int i = 1; i <= 2; i++) {
                brothers[i - 1] =
                        new Brother("brother" + i, "resume", ENTITY_TYPE.DATA, brotherGrimm, b,
                                prism,
                                URLS.RESUME_URL);
            }
            for (int i = 3; i <= 4; i++) {
                brothers[i - 1] =
                        new Brother("brother" + i, "suspend", ENTITY_TYPE.DATA, brotherGrimm, b,
                                prism,
                                URLS.SUSPEND_URL);
            }
            for (Brother brother : brothers) {
                brother.start();
            }
            Thread.sleep(60000);
            for (Brother brother : brothers) {
                if (brother.getOutput().getMessage().contains("SUCCEEDED"))
                    succeedeResponse++;
                else if (brother.getOutput().getMessage().contains("FAILED"))
                    failedResponse++;
            }
            Assert.assertEquals(succeedeResponse, 1);
            Assert.assertEquals(failedResponse, 3);
        } finally {
            b.deleteBundle(prism);
        }
    }

    @Test(groups = {"multiCluster"})
    public void resumeParallel() throws Exception {
        try{
            prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, b.getClusters().get(0));
            prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, b.getDataSets().get(0));
            ServiceResponse r =
                    prism.getFeedHelper().schedule(URLS.SCHEDULE_URL, b.getDataSets().get(0));
            Thread.sleep(15000);
            prism.getFeedHelper().resume(URLS.RESUME_URL, b.getDataSets().get(0));
            Thread.sleep(5000);
            prism.getFeedHelper().suspend(URLS.SUSPEND_URL, b.getDataSets().get(0));
            Thread.sleep(15000);
            for (int i = 1; i <= brothers.length; i++) {
                brothers[i - 1] =
                        new Brother("brother" + i, "resume", ENTITY_TYPE.DATA, brotherGrimm, b,
                                prism,
                                URLS.RESUME_URL);
            }
            for (Brother brother : brothers) {
                brother.start();
            }
            Thread.sleep(60000);
            for (Brother brother : brothers) {
                if (brother.getOutput().getMessage().contains("SUCCEEDED"))
                    succeedeResponse++;
                else if (brother.getOutput().getMessage().contains("FAILED"))
                    failedResponse++;
            }
            Assert.assertEquals(succeedeResponse, 1);
            Assert.assertEquals(failedResponse, 9);
        } finally {
            b.deleteBundle(prism);
        }
    }


    @Test(groups = {"multiCluster"})
    public void submitSameClusterParallel() throws Exception {
        for (int i = 1; i <= brothers.length; i++) {
            brothers[i - 1] =
                    new Brother("brother" + i, "submit", ENTITY_TYPE.CLUSTER, brotherGrimm, b,
                            prism,
                            URLS.SUBMIT_URL);
        }
        for (Brother brother : brothers) {
            brother.start();
        }
        Thread.sleep(60000);
        for (Brother brother : brothers) {
            if (brother.getOutput().getMessage().contains("SUCCEEDED"))
                succeedeResponse++;
            else if (brother.getOutput().getMessage().contains("FAILED"))
                failedResponse++;
        }
        Assert.assertEquals(succeedeResponse, 1);
        Assert.assertEquals(failedResponse, 9);
    }

}
