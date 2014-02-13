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

package org.apache.falcon.regression.testHelper;

import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.PrismHelper;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.hadoop.fs.FileSystem;
import org.apache.oozie.client.OozieClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class BaseTestClass {
    private static List<String> serverNames;

    static {
        try {
            prepareProperties();
        } catch (Exception e) {
            System.out.println(e.getMessage());  //To change body of catch statement use
            System.exit(1);
        }
    }

    public PrismHelper prism;
    public List<ColoHelper> servers;
    public List<FileSystem> serverFS;
    public List<OozieClient> serverOC;
    public String baseHDFSDir = "/tmp/falcon-regression";
    public String baseWorkflowDir = baseHDFSDir + "/workflows";
    public static final String MERLIN_PROPERTIES = "Merlin.properties";
    public static final String PRISM_PREFIX = "prism";


    public BaseTestClass() {

        prism = new PrismHelper(MERLIN_PROPERTIES, PRISM_PREFIX);
        servers = getServers();
        serverFS = new ArrayList<FileSystem>();
        serverOC = new ArrayList<OozieClient>();
        for (ColoHelper server : servers) {
            try {
                serverFS.add(server.getClusterHelper().getHadoopFS());

                serverOC.add(server.getClusterHelper().getOozieClient());
                HadoopUtil.createDir(baseHDFSDir, serverFS.get(serverFS.size
                  ()-1));
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }

    private static void prepareProperties() throws Exception {

        Properties merlinProp = Util.getPropertiesObj(MERLIN_PROPERTIES);
        serverNames = new ArrayList<String>(Arrays.asList(merlinProp.getProperty("servers").split
                (",")));
        for (int i = 0; i < serverNames.size(); i++)
            serverNames.set(i, serverNames.get(i).trim());
    }

    private List<ColoHelper> getServers() {
        ArrayList<ColoHelper> returnList = new ArrayList<ColoHelper>();
        for (int i = 0; i < serverNames.size(); i++)
            returnList.add(new ColoHelper(MERLIN_PROPERTIES, serverNames.get(i)));

        return returnList;
    }

}
