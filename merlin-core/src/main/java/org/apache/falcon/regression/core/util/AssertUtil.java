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

import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.response.APIResult;
import org.apache.falcon.regression.core.response.ProcessInstancesResult;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.enumsAndConstants.ENTITY_TYPE;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.testng.Assert;

import javax.xml.bind.JAXBException;
import java.util.List;

public class AssertUtil {

    /**
     * Checks that any path in list doesn't contains a string
     * @param paths list of paths
     * @param shouldNotBePresent string that shouldn't be present
     */
    public static void failIfStringFoundInPath(
            List<Path> paths, String... shouldNotBePresent) {
        for (Path path : paths) {
            for (String aShouldNotBePresent : shouldNotBePresent)
                if (path.toUri().toString().contains(aShouldNotBePresent))
                    Assert.fail("String " + aShouldNotBePresent + " was not expected in path " +
                            path.toUri().toString());
        }
    }

    /**
     * Checks that two lists has the same size
     * @param oneList one list of paths
     * @param anotherList another list of paths
     */
    public static void checkForPathsSizes(List<Path> oneList,
                                          List<Path> anotherList) {
        Assert.assertEquals(oneList.size(), anotherList.size(),
                "array size of the 2 paths array list is not the same");
    }

    /**
     * Checks that ServiceResponse status is SUCCEEDED
     * @param response ServiceResponse
     * @throws JAXBException
     */
    public static void assertSucceeded(ServiceResponse response) throws JAXBException {
        Assert.assertEquals(Util.parseResponse(response).getStatus(),
                APIResult.Status.SUCCEEDED, "Status should be SUCCEEDED");
        Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 200,
                "Status code should be 200");
        Assert.assertNotNull(Util.parseResponse(response).getMessage(), "Status message is null");
    }

    /**
     * Checks that ProcessInstancesResult status is SUCCEEDED
     * @param response ProcessInstancesResult
     */
    public static void assertSucceeded(ProcessInstancesResult response) {
        Assert.assertNotNull(response.getMessage());
        Assert.assertEquals(response.getStatus(), APIResult.Status.SUCCEEDED,
                "Status should be SUCCEEDED");
    }

    /**
     * Checks that ServiceResponse status is status FAILED
     * @param response ServiceResponse
     * @param message message for exception
     * @throws JAXBException
     */
    public static void assertFailed(final ServiceResponse response, final String message)
    throws JAXBException {
        assertFailedWithStatus(response, 400, message);
    }

    /**
     * Checks that ServiceResponse status is status FAILED with some status code
     * @param response ServiceResponse
     * @param statusCode expected status code
     * @param message message for exception
     * @throws JAXBException
     */
    public static void assertFailedWithStatus(final ServiceResponse response, final int statusCode,
                                              final String message) throws JAXBException {
        Assert.assertNotEquals(response.message, "null", "response message should not be null");
        Assert.assertEquals(Util.parseResponse(response).getStatus(),
                APIResult.Status.FAILED, message);
        Assert.assertEquals(Util.parseResponse(response).getStatusCode(), statusCode,
                message);
        Assert.assertNotNull(Util.parseResponse(response).getRequestId(), "RequestId is null");
    }

    /**
     * Checks that ServiceResponse status is status PARTIAL
     * @param response ServiceResponse
     * @throws JAXBException
     */
    public static void assertPartial(ServiceResponse response) throws JAXBException {
        Assert.assertEquals(Util.parseResponse(response).getStatus(), APIResult.Status.PARTIAL);
        Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 400);
        Assert.assertNotNull(Util.parseResponse(response).getMessage());
    }

    /**
     * Checks that ServiceResponse status is status FAILED with status code 400
     * @param response ServiceResponse
     * @throws JAXBException
     */
    public static void assertFailed(ServiceResponse response) throws JAXBException {
        Assert.assertNotEquals(response.message, "null", "response message should not be null");

        Assert.assertEquals(Util.parseResponse(response).getStatus(), APIResult.Status.FAILED);
        Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 400);
    }

    /**
     * Checks that status of some entity job is equal to expected. Method can wait
     * 100 seconds for expected status.
     * @param oozieClient OozieClient
     * @param entityType FEED or PROCESS
     * @param data feed or proceess XML
     * @param expectedStatus expected Job.Status of entity
     * @throws JAXBException
     * @throws OozieClientException
     * @throws InterruptedException
     */
    public static void checkStatus(OozieClient oozieClient, ENTITY_TYPE entityType, String data,
                                   Job.Status expectedStatus)
            throws JAXBException, OozieClientException, InterruptedException {
        String name = null;
        if (entityType == ENTITY_TYPE.FEED) {
            name = Util.readDatasetName(data);
        } else if (entityType == ENTITY_TYPE.PROCESS) {
            name = Util.readEntityName(data);
        }
        Assert.assertEquals(
                OozieUtil.verifyOozieJobStatus(oozieClient, name, entityType, expectedStatus), true,
                "Status should be " + expectedStatus);
    }

    /**
     * Checks that status of some entity job is equal to expected. Method can wait
     * 100 seconds for expected status.
     * @param oozieClient OozieClient
     * @param entityType FEED or PROCESS
     * @param bundle Bundle with feed or process data
     * @param expectedStatus expected Job.Status of entity
     * @throws JAXBException
     * @throws OozieClientException
     * @throws InterruptedException
     */
    public static void checkStatus(OozieClient oozieClient, ENTITY_TYPE entityType, Bundle bundle,
                                   Job.Status expectedStatus)
            throws InterruptedException, OozieClientException, JAXBException {
        String data = null;
        if (entityType == ENTITY_TYPE.FEED) {
            data = bundle.getDataSets().get(0);
        } else if (entityType == ENTITY_TYPE.PROCESS) {
            data = bundle.getProcessData();
        }
        checkStatus(oozieClient, entityType, data, expectedStatus);
    }

    /**
     * Checks that status of some entity job is NOT equal to expected
     * @param oozieClient OozieClient
     * @param entityType FEED or PROCESS
     * @param data feed or proceess XML
     * @param expectedStatus expected Job.Status of entity
     * @throws JAXBException
     * @throws OozieClientException
     */
    public static void checkNotStatus(OozieClient oozieClient, ENTITY_TYPE entityType, String data,
                                      Job.Status expectedStatus)
            throws JAXBException, OozieClientException {
        String processName = null;
        if (entityType == ENTITY_TYPE.FEED) {
            processName = Util.readDatasetName(data);
        } else if (entityType == ENTITY_TYPE.PROCESS) {
            processName = Util.readEntityName(data);
        }
        Assert.assertNotEquals(OozieUtil.getOozieJobStatus(oozieClient, processName,
                entityType), expectedStatus, "Status should not be " + expectedStatus);
    }

    /**
     * Checks that status of some entity job is NOT equal to expected
     * @param oozieClient OozieClient
     * @param entityType FEED or PROCESS
     * @param bundle Bundle with feed or process data
     * @param expectedStatus expected Job.Status of entity
     * @throws JAXBException
     * @throws OozieClientException
     */
    public static void checkNotStatus(OozieClient oozieClient, ENTITY_TYPE entityType,
                                      Bundle bundle, Job.Status expectedStatus)
            throws JAXBException, OozieClientException {
        String data = null;
        if (entityType == ENTITY_TYPE.FEED) {
            data = bundle.getDataSets().get(0);
        } else if (entityType == ENTITY_TYPE.PROCESS) {
            data = bundle.getProcessData();
        }
        checkNotStatus(oozieClient, entityType, data, expectedStatus);
    }

}
