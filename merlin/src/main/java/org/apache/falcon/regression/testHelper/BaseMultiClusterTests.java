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
package org.apache.falcon.regression.testHelper;

import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.PrismHelper;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.oozie.client.OozieClient;

import java.io.IOException;

public class BaseMultiClusterTests extends  BaseSingleClusterTests{

    public ColoHelper server2 = new ColoHelper("mk-qa.config.properties", "");
    public ColoHelper server3 = new ColoHelper("gs1001.config.properties", "");
    public FileSystem server2FS, server3FS = null;
    public OozieClient server2OC, server3OC = null;

    public BaseMultiClusterTests() {
        super();
        try {
            server2FS = server2.getClusterHelper().getHadoopFS();
            server2OC = server2.getClusterHelper().getOozieClient();
            server3FS = server3.getClusterHelper().getHadoopFS();
            server3OC = server3.getClusterHelper().getOozieClient();
            HadoopUtil.createDir(baseHDFSDir, server2FS, server3FS);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
