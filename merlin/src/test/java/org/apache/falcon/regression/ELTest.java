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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

/**
 * ELTest.
 */
@Test(groups = "embedded")
public class ELTest extends BaseTestClass {

    ColoHelper cluster;

    public ELTest(){
        super();
        cluster = servers.get(1);
    }

    @BeforeMethod(alwaysRun = true)
    public void testName(Method method) {
        Util.print("test name: " + method.getName());
    }

    @Test(groups = {"singleCluster"}, dataProvider = "EL-DP")
    public void ExpressionLanguageTest(String startInstance, String endInstance) throws Exception {
        ELUtil.testWith(prism, cluster, startInstance, endInstance, true);
    }

    @DataProvider(name = "EL-DP")
    public Object[][] getELData(Method m) throws Exception {
        return new Object[][]{

                {"now(-3,0)","now(4,20)"},
                {"yesterday(22,0)","now(4,20)"},
                {"currentMonth(0,22,0)","now(4,20)"},
                {"lastMonth(30,22,0)","now(4,20)"},
                {"currentYear(0,0,22,0)","currentYear(1,1,22,0)"},
                {"currentMonth(0,22,0)","currentMonth(1,22,20)"},
                {"lastMonth(30,22,0)","lastMonth(60,2,40)"},
                {"lastYear(12,0,22,0)", "lastYear(13,1,22,0)"}
        };
    }
}
