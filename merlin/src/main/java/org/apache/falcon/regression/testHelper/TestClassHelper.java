package org.apache.falcon.regression.testHelper;

import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.PrismHelper;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.hadoop.fs.FileSystem;
import org.apache.oozie.client.OozieClient;

import java.io.IOException;
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

public class TestClassHelper {

    public PrismHelper prism = new PrismHelper("prism.properties");
    public ColoHelper server1 = new ColoHelper("mk-qa.config.properties");
    public ColoHelper server2 = new ColoHelper("ivoryqa-1.config.properties");
    public ColoHelper server3 = new ColoHelper("gs1001.config.properties");
    public FileSystem server1FS, server2FS, server3FS = null;
    public OozieClient server1OC, server2OC, server3OC = null;
    public String baseHDFSDir = "/tmp/falcon-regression";

    public TestClassHelper() throws IOException {
        server1FS = server1.getClusterHelper().getHadoopFS();
        server2FS = server2.getClusterHelper().getHadoopFS();
        server3FS = server3.getClusterHelper().getHadoopFS();
        server1OC = server1.getClusterHelper().getOozieClient();
        server2OC = server2.getClusterHelper().getOozieClient();
        server3OC = server3.getClusterHelper().getOozieClient();
        HadoopUtil.createDir(baseHDFSDir, server1FS, server2FS, server3FS);
    }

    public boolean checkServices() {
        //restart server as precaution
        try {
            Util.restartService(server1.getClusterHelper());
            Util.restartService(server2.getClusterHelper());
            Util.restartService(server3.getClusterHelper());
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
