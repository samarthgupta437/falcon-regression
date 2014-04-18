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

package org.apache.falcon.regression.prism;

import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;

import java.util.List;

public class TestRunId {
    private static Logger logger = Logger.getLogger(TestRunId.class);

    public static void main(String[] args) throws OozieClientException {
        OozieClient client = new OozieClient("http://mk-qa-63:11000/oozie/");
        CoordinatorJob coordInfo = client.getCoordJobInfo("0000248-120618130937367-testuser-C");
        List<CoordinatorAction> actions = coordInfo.getActions();
        for (CoordinatorAction action : actions) {
            WorkflowJob job = client.getJobInfo(action.getExternalId());
            logger.info(job.getRun());
        }
    }
}
