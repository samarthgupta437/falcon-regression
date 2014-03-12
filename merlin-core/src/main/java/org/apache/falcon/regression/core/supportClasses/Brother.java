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
package org.apache.falcon.regression.core.supportClasses;

import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.PrismHelper;
import org.apache.falcon.regression.core.interfaces.EntityHelperFactory;
import org.apache.falcon.regression.core.interfaces.IEntityManagerHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.testng.TestNGException;
import org.apache.log4j.Logger;

public class Brother extends Thread {

    String operation;
    String data;
    String url;
    ServiceResponse output;
    private Logger logger = Logger.getLogger(this.getClass());

    public ServiceResponse getOutput() {
        return output;
    }

    IEntityManagerHelper entityManagerHelper;
    PrismHelper p;

    public Brother(String threadName, String operation, ENTITY_TYPE entityType, ThreadGroup tGroup,
                   String data,
                   URLS url) {
        super(tGroup, threadName);
        this.operation = operation;
        this.entityManagerHelper = EntityHelperFactory.getEntityHelper(entityType);
        this.data = data;
        this.url = url.getValue();
        this.output = new ServiceResponse();
    }


    public Brother(String threadName, String operation, ENTITY_TYPE entityType, ThreadGroup tGroup,
                   Bundle b,
                   PrismHelper p, URLS url) {
        super(tGroup, threadName);
        this.operation = operation;
        this.p = p;

        if (entityType.equals(ENTITY_TYPE.PROCESS)) {
            this.data = b.getProcessData();
            this.entityManagerHelper = p.getProcessHelper();

        } else if (entityType.equals(ENTITY_TYPE.CLUSTER)) {

            this.entityManagerHelper = p.getClusterHelper();
            this.data = b.getClusters().get(0);
        } else {
            this.entityManagerHelper = p.getFeedHelper();
            this.data = b.getDataSets().get(0);
        }

        this.url = p.getClusterHelper().getHostname() + url.getValue();
        this.output = new ServiceResponse();
    }


    public String getData() {
        return data;
    }


    public void run() {
        try {
            sleep(50L);
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        }


        //System.out.println("Brother "+this.getName()+" will be executing "+operation);
        logger.info("Brother " + this.getName() + " will be executing " + operation);

        try {
            if (operation.equalsIgnoreCase("submit")) {
                output = entityManagerHelper.submitEntity(url, data);

            } else if (operation.equalsIgnoreCase("get")) {
                output = entityManagerHelper.getEntityDefinition(url, data);
                //System.out.println("Brother "+this.getName()+"'s response to the "+operation+"
                // is: "+output);
            } else if (operation.equalsIgnoreCase("delete")) {
                output = entityManagerHelper.delete(url, data);
                //System.out.println("Brother "+this.getName()+"'s response to the "+operation+"
                // is: "+output);
            } else if (operation.equalsIgnoreCase("suspend")) {
                output = entityManagerHelper.suspend(url, data);
                //System.out.println("Brother "+this.getName()+"'s response to the "+operation+"
                // is: "+output);
            } else if (operation.equalsIgnoreCase("schedule")) {
                output = entityManagerHelper.schedule(url, data);
                //System.out.println("Brother "+this.getName()+"'s response to the "+operation+"
                // is: "+output);
            } else if (operation.equalsIgnoreCase("resume")) {
                output = entityManagerHelper.resume(url, data);
            } else if (operation.equalsIgnoreCase("SnS")) {
                output = entityManagerHelper.submitAndSchedule(url, data);
            } else if (operation.equalsIgnoreCase("status")) {
                output = entityManagerHelper.getStatus(url, data);
            }

            //System.out.println("Brother "+this.getName()+"'s response to the "+operation+" is:
            // "+output);
            logger.info("Brother " + this.getName() + "'s response to the " + operation + " is: " +
                    output);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
