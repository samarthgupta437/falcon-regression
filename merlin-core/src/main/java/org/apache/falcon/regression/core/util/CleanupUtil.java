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

package org.apache.falcon.regression.core.util;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.falcon.regression.core.helpers.PrismHelper;
import org.apache.falcon.regression.core.interfaces.IEntityManagerHelper;
import org.apache.falcon.regression.core.response.EntitiesResult;
import org.apache.falcon.regression.core.response.EntityResult;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.IOException;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class CleanupUtil {
    private static Logger logger = Logger.getLogger(CleanupUtil.class);

    public static List<String> getAllProcesses(PrismHelper prism)
        throws IOException, URISyntaxException, AuthenticationException, JAXBException {
        return getAllEntitiesOfOneType(prism.getProcessHelper());
    }

    public static List<String> getAllFeeds(PrismHelper prism)
        throws IOException, URISyntaxException, AuthenticationException, JAXBException {
        return getAllEntitiesOfOneType(prism.getFeedHelper());
    }

    public static List<String> getAllClusters(PrismHelper prism)
        throws IOException, URISyntaxException, AuthenticationException, JAXBException {
        return getAllEntitiesOfOneType(prism.getClusterHelper());
    }

    private static List<String> getAllEntitiesOfOneType(IEntityManagerHelper iEntityManagerHelper)
        throws IOException, URISyntaxException, AuthenticationException, JAXBException {
        final EntitiesResult entitiesResult = getEntitiesResultOfOneType(iEntityManagerHelper);
        List<String> clusters = new ArrayList<String>();
        for (EntityResult entity : entitiesResult.getEntities()) {
            clusters.add(entity.getName());
        }
        return clusters;
    }

    private static EntitiesResult getEntitiesResultOfOneType(
        IEntityManagerHelper iEntityManagerHelper)
        throws IOException, URISyntaxException, AuthenticationException, JAXBException {
        final ServiceResponse clusterResponse =
            iEntityManagerHelper.listEntities(Util.URLS.LIST_URL);
        JAXBContext jc = JAXBContext.newInstance(EntitiesResult.class);
        Unmarshaller u = jc.createUnmarshaller();
        return (EntitiesResult) u.unmarshal(
            new StringReader(clusterResponse.getMessage()));
    }

    public static void cleanAllClustersQuietly(PrismHelper prism) {
        try {
            final List<String> clusters = getAllClusters(prism);
            for (String cluster : clusters) {
                try {
                    prism.getClusterHelper().deleteByName(Util.URLS.DELETE_URL, cluster, null);
                } catch (Exception e) {
                    logger.warn("Caught exception: " + ExceptionUtils.getStackTrace(e));
                }
            }
        } catch (Exception e) {
            logger.warn("Unable to get a list of clusters because of exception: " +
                ExceptionUtils.getStackTrace(e));
        }
    }

    public static void cleanAllFeedsQuietly(PrismHelper prism) {
        try {
            final List<String> feeds = getAllFeeds(prism);
            for (String feed : feeds) {
                try {
                    prism.getFeedHelper().deleteByName(Util.URLS.DELETE_URL, feed, null);
                } catch (Exception e) {
                    logger.warn("Caught exception: " + ExceptionUtils.getStackTrace(e));
                }
            }
        } catch (Exception e) {
            logger.warn("Unable to get a list of feeds because of exception: " +
                ExceptionUtils.getStackTrace(e));
        }
    }

    public static void cleanAllProcessesQuietly(PrismHelper prism,
                                                IEntityManagerHelper entityManagerHelper) {
        try {
            final List<String> processes = getAllProcesses(prism);
            for (String process : processes) {
                try {
                    entityManagerHelper.deleteByName(Util.URLS.DELETE_URL, process, null);
                } catch (Exception e) {
                    logger.warn("Caught exception: " + ExceptionUtils.getStackTrace(e));
                }
            }
        } catch (Exception e) {
            logger.warn("Unable to get a list of feeds because of exception: " +
                ExceptionUtils.getStackTrace(e));
        }
    }

    public static void cleanAllEntities(PrismHelper prism) {
        cleanAllProcessesQuietly(prism, prism.getProcessHelper());
        cleanAllFeedsQuietly(prism);
        cleanAllClustersQuietly(prism);
    }

}
