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

package org.apache.falcon.regression.ui;


import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.generated.process.Process;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.testHelper.BaseUITestClass;
import org.apache.falcon.regression.ui.pages.ClustersPage;
import org.apache.falcon.regression.ui.pages.EntitiesPage;
import org.apache.falcon.regression.ui.pages.EntitiesPage.EntityStatus;
import org.apache.falcon.regression.ui.pages.ProcessPage;
import org.apache.falcon.regression.ui.pages.ProcessesPage;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.Assert;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.lang.reflect.Method;


public class TestUISample extends BaseUITestClass {

    private ColoHelper cluster = servers.get(0);
    private String aggregateWorkflowDir = baseHDFSDir + "/TestUISample/aggregator";

    @BeforeMethod
    public void setUp() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
        openBrowser();
        bundles[0] = BundleUtil.readELBundles()[0][0];
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle();
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[0].submitBundle(cluster);
    }

    @AfterMethod
    public void tearDown(Method method) throws IOException {
        closeBrowser();
        removeBundles();
    }

    @Test
    public void testFalconEntities() throws JAXBException, IOException {

        EntitiesPage page = new ProcessesPage(DRIVER, cluster);
        page.navigateTo();
        EntityStatus status = page.getEntityStatus(bundles[0].getProcessName());
        Assert.assertNotNull(status);
        Assert.assertEquals(status, EntityStatus.SUBMITTED);

        page = new ClustersPage(DRIVER, cluster);
        page.navigateTo();
        status = page.getEntityStatus(bundles[0].getClusterNames().get(0));
        Assert.assertNotNull(status);
        Assert.assertEquals(status, EntityStatus.SUBMITTED);

        ProcessPage page2 = new ProcessPage(DRIVER, cluster, bundles[0].getProcessName());
        page2.navigateTo();
        Process process = page2.getEntity();
        Assert.assertEquals(InstanceUtil.processToString(process),
                InstanceUtil.processToString(InstanceUtil.getProcessElement(bundles[0])));
    }
}
