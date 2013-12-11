package org.apache.falcon.regression.testHelper;

import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.PrismHelper;
import org.apache.falcon.regression.core.util.Util;

import java.util.Arrays;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public abstract class TestClassHelper {

    protected PrismHelper prism = new PrismHelper("prism.properties");
    protected ColoHelper server1 = new ColoHelper("mk-qa.config.properties");
    protected ColoHelper server2 = new ColoHelper("ivoryqa-1.config.properties");
    protected ColoHelper server3 = new ColoHelper("gs1001.config.properties");
    protected ColoHelper server4 = new ColoHelper("ua4.properties");

    public boolean checkServices() {
        //restart server as precaution
        try {
            Util.restartService(server1.getClusterHelper());
            Util.restartService(server2.getClusterHelper());
            Util.restartService(server3.getClusterHelper());
            Util.restartService(server4.getClusterHelper());

        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File
            // Templates.
        }
        return true;
    }

    public Bundle getBundle(ColoHelper cluster, String... xmlLocation) {
        Bundle b;
        try {
            if (xmlLocation.length == 1)
                b = (Bundle) Bundle.readBundle(xmlLocation[0])[0][0];
            else if (xmlLocation.length == 0)
                b = (Bundle) Util.readELBundles()[0][0];
            else {
                System.out.println("invalid size of xmlLocaltions return null");
                return null;
            }

            b.generateUniqueBundle();
            return new Bundle(b, cluster.getEnvFileName());
        } catch (Exception e) {
            System.out.println(Arrays.toString(e.getStackTrace()));
        }
        return null;
    }

}
