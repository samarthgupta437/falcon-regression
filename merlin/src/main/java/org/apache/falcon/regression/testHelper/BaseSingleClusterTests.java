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

public class BaseSingleClusterTests {

    public PrismHelper prism = new PrismHelper("prism.properties", "");
    public ColoHelper server1 = new ColoHelper("ivoryqa-1.config.properties", "");
    public FileSystem server1FS = null;
    public OozieClient server1OC = null;
    public String baseHDFSDir = "/tmp/falcon-regression";

    public BaseSingleClusterTests() {
        server1OC = server1.getClusterHelper().getOozieClient();
        try {
            server1FS = server1.getClusterHelper().getHadoopFS();
            HadoopUtil.createDir(baseHDFSDir, server1FS);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
