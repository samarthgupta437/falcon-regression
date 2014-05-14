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


import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.falcon.regression.ui.pages.EntitiesPage;
import org.apache.falcon.regression.ui.pages.FeedsPage;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.Assert;


import java.io.IOException;

public class TestUISample extends BaseTestClass {

    @BeforeMethod
    public void setUp() {
        openBrowser();
    }

    @AfterMethod
    public void tearDown() {
        closeBrowser();
    }

    @Test
    public void test() throws InterruptedException, IOException {
        EntitiesPage page = new FeedsPage(DRIVER, prism);
        page.navitageTo();
        String status = page.getEntityStatus("agregated-logs16-67fb8cb3-4660-498d-b1e5-bbfa39b4c943");
        Assert.assertNotNull(status);
        Assert.assertEquals(status, "UNKNOWN");
    }
}
