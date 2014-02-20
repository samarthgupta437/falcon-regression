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

import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.ELUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;


/**
 * EL Validations tests.
 */
@Test(groups = "embedded")
public class ELValidationsTest extends BaseTestClass {

    ColoHelper cluster;

    public ELValidationsTest(){
        super();
        cluster = servers.get(0);
    }

    //test for instance when process time line is subset of feed time
    @BeforeMethod
    public void testName(Method method) {
        Util.print("test name: " + method.getName());
    }

    @Test(groups = {"0.1", "0.2"})
    public void startInstBeforeFeedStart_today02() throws Exception {
        String response = ELUtil.testWith(prism, cluster, "2009-02-02T20:00Z", "2011-12-31T00:00Z", "2009-02-02T20:00Z",
                "2011-12-31T00:00Z", "now(-40,0)", "currentYear(20,30,24,20)", false);
        validate(response);
    }

    @Test(groups = {"singleCluster"})
    public void startInstAfterFeedEnd() throws Exception {
        String response = ELUtil.testWith(prism, cluster, "currentYear(10,0,22,0)", "now(4,20)", false);
        validate(response);
    }

    @Test(groups = {"singleCluster"})
    public void bothInstReverse() throws Exception {
        String response = ELUtil.testWith(prism, cluster, "now(0,0)", "now(-100,0)", false);
        validate(response);
    }


    private void validate(String response) {
        if ((response.contains("End instance ") || response.contains("Start instance"))
                && (response.contains("for feed") || response.contains("of feed"))
                && (response.contains("is before the start of feed") || response.contains("is after the end of feed"))) {
            return;
        }
        if (response.contains("End instance") && response.contains("is before the start instance")) {
            return;
        }

        Assert.fail("Response is not valid");
    }
}
