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

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.falcon.regression.core.helpers;

import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.interfaces.EntityHelperFactory;
import org.apache.falcon.regression.core.interfaces.IEntityManagerHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.supportClasses.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;


public class PrismHelper {

    protected IEntityManagerHelper clusterHelper;
    protected IEntityManagerHelper processHelper;
    protected IEntityManagerHelper feedHelper;
    protected InstanceUtil instanceUtil;

    protected Util util;

    public IEntityManagerHelper getClusterHelper() {
        return clusterHelper;
    }

    public IEntityManagerHelper getFeedHelper() {
        return feedHelper;
    }

    public IEntityManagerHelper getProcessHelper() {
        return processHelper;
    }

    protected String envFileName;

    public String getEnvFileName() {
        return envFileName;
    }

    public PrismHelper(String envFileName) {
        try {
            this.envFileName = envFileName;
            clusterHelper =
                    EntityHelperFactory.getEntityHelper(ENTITY_TYPE.CLUSTER, this.envFileName);
            processHelper =
                    EntityHelperFactory.getEntityHelper(ENTITY_TYPE.PROCESS, this.envFileName);
            feedHelper = EntityHelperFactory.getEntityHelper(ENTITY_TYPE.DATA, this.envFileName);
            instanceUtil = new InstanceUtil(this.envFileName);

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

    }

    public ServiceResponse submitCluster(Bundle bundle) throws Exception {
        return clusterHelper.submitEntity(Util.URLS.SUBMIT_URL, bundle.getClusterData());
    }

    /*public List<ServiceResponse> submitFeed(Bundle bundle) throws Exception {
        List<ServiceResponse> responseList = new ArrayList<ServiceResponse>();
        for (String feed : bundle.getDataSets()) {
            responseList.add(feedHelper.submitEntity(Util.URLS.SUBMIT_URL, feed));
        }
        return responseList;
    }*/

    /*public ServiceResponse submitProcess(Bundle bundle) throws Exception {
        return processHelper.submitEntity(Util.URLS.SUBMIT_URL, bundle.getProcessData());
    }*/

    /*public ServiceResponse scheduleProcess(Bundle bundle) throws Exception {
        return processHelper
                .schedule(Util.URLS.SCHEDULE_URL, Util.readEntityName(bundle.getProcessData()));
    }*/
}
