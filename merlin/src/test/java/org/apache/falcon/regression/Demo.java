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

import com.jcraft.jsch.JSchException;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.PrismHelper;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.XOozieClient;
import org.apache.oozie.client.rest.RestConstants;
import org.testng.annotations.Test;

import java.io.IOException;
import java.text.ParseException;
import java.util.Properties;


/**
 * Demo.
 */
public class Demo extends BaseTestClass{

    public static void main(String[] args) {
        String file = "prism.properties";
        Properties properties = Util.getPropertiesObj(file);
        System.out.println("properties = " + properties);
    }


    @Test
    public void FeedMerlinTest() throws ParseException, JSchException,
      IOException {



    }


/*
    @Test(dataProvider = "demo-DP")
    public void demoBundle(Bundle bundle) throws Exception {

        try {
            bundle.submitAndScheduleBundle();

        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {
            bundle.deleteBundle();
        }
    }

    @DataProvider(name = "demo-DP")
    public static Object[][] getTestData(Method m) throws Exception {

        return Util.readDemoBundle();
    }
*/


}
