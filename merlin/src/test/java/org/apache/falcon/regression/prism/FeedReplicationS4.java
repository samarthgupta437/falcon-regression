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

import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.generated.feed.ActionType;
import org.apache.falcon.regression.core.generated.feed.ClusterType;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.PrismHelper;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.testng.annotations.Test;

public class FeedReplicationS4 {

    PrismHelper prismHelper = new PrismHelper("prism.properties");

    ColoHelper ua1 = new ColoHelper("gs1001.config.properties");
    ColoHelper ua2 = new ColoHelper("ivoryqa-1.config.properties");

    @Test(enabled = true, timeOut = 1200000)
    public void ReplicationFromS4() throws Exception {

        Bundle b1 = (Bundle) Bundle.readBundle("S4Replication")[0][0];
        b1.generateUniqueBundle();

        Bundle b2 = (Bundle) Bundle.readBundle("S4Replication")[0][0];
        b2.generateUniqueBundle();


        try {
            b1 = new Bundle(b1, ua1.getEnvFileName());
            b2 = new Bundle(b2, ua2.getEnvFileName());

            //Bundle.submitCluster(b1,b2);

            String feedOutput01 = b1.getDataSets().get(0);
            feedOutput01 = InstanceUtil
                    .setFeedCluster(feedOutput01,
                            XmlUtil.createValidity("2010-10-01T12:00Z", "2099-01-01T00:00Z"),
                            XmlUtil.createRtention("days(10000)", ActionType.DELETE), null,
                            ClusterType.SOURCE, null,
                            null);
            feedOutput01 = InstanceUtil
                    .setFeedCluster(feedOutput01,
                            XmlUtil.createValidity("2012-12-06T05:00Z", "2099-10-01T12:10Z"),
                            XmlUtil.createRtention("minutes(5)", ActionType.DELETE),
                            Util.readClusterName(b2.getClusters().get(0)), ClusterType.SOURCE, "",
                            "s4://inmobi-iat-data/userplatform/${YEAR}/${MONTH}/${DAY}/${HOUR}");
            feedOutput01 = InstanceUtil
                    .setFeedCluster(feedOutput01,
                            XmlUtil.createValidity("2012-12-06T05:00Z", "2099-10-01T12:25Z"),
                            XmlUtil.createRtention("minutes(5)", ActionType.DELETE),
                            Util.readClusterName(b1.getClusters().get(0)), ClusterType.TARGET, "",
                            "/replicationS4/${YEAR}/${MONTH}/${DAY}/${HOUR}");

            System.out.println(feedOutput01);
            Bundle.submitCluster(b1, b2);
            prismHelper.getFeedHelper()
                    .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feedOutput01);

        } finally {
            b1.deleteBundle(prismHelper);
        }
    }

}
