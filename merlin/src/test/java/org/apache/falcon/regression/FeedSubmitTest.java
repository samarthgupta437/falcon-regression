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


import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseSingleClusterTests;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

/**
 * Feed submission tests.
 */
public class FeedSubmitTest extends BaseSingleClusterTests {

    private Bundle bundle;
    private String feed;

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        bundle = Util.readELBundles()[0][0];
        bundle.generateUniqueBundle();
        bundle = new Bundle(bundle, server1.getEnvFileName());

        //submit the cluster
        ServiceResponse response =prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle.getClusters().get(0));
        Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 200);
        Assert.assertNotNull(Util.parseResponse(response).getMessage());

        feed = Util.getInputFeedFromBundle(bundle);
    }
    
    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        prism.getFeedHelper().delete(URLS.DELETE_URL, Util.getInputFeedFromBundle(bundle));
    }

    @Test(groups = {"singleCluster"})
    public void submitValidFeed() throws Exception {

        ServiceResponse response = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
        Util.assertSucceeded(response);
    }

    @Test(groups = {"singleCluster"})
    public void submitValidFeedPostDeletion() throws Exception {

        ServiceResponse response = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
        Util.assertSucceeded(response);

        response = prism.getFeedHelper().delete(URLS.DELETE_URL, feed);
        Util.assertSucceeded(response);

        prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
    }

    @Test(groups = {"singleCluster"})
    public void submitValidFeedPostGet() throws Exception {

        ServiceResponse response = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
        Util.assertSucceeded(response);

        response = prism.getFeedHelper().getEntityDefinition(URLS.GET_ENTITY_DEFINITION, feed);
        Util.assertSucceeded(response);

        response = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
        Util.assertSucceeded(response);
    }

    @Test(groups = {"singleCluster"})
    public void submitValidFeedTwice() throws Exception {

        ServiceResponse response = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
        Util.assertSucceeded(response);

        response = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
        Util.assertSucceeded(response);
    }
}
