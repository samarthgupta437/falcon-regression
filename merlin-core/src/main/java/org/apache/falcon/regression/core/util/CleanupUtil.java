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

import org.apache.falcon.regression.core.helpers.PrismHelper;
import org.apache.falcon.regression.core.interfaces.IEntityManagerHelper;
import org.apache.falcon.regression.core.response.EntitiesResult;
import org.apache.falcon.regression.core.response.EntityResult;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.hadoop.security.authentication.client.AuthenticationException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.IOException;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class CleanupUtil {
    public static List<String> getAllProcesses(PrismHelper prism)
        throws IOException, URISyntaxException, AuthenticationException, JAXBException {
        return extractEntityNames(prism.getProcessHelper());
    }

    public static List<String> getAllFeeds(PrismHelper prism)
        throws IOException, URISyntaxException, AuthenticationException, JAXBException {
        return extractEntityNames(prism.getFeedHelper());
    }

    public static List<String> getAllClusters(PrismHelper prism)
        throws IOException, URISyntaxException, AuthenticationException, JAXBException {
        return extractEntityNames(prism.getClusterHelper());
    }

    public static List<String> extractEntityNames(IEntityManagerHelper iEntityManagerHelper)
        throws IOException, URISyntaxException, AuthenticationException, JAXBException {
        final EntitiesResult entitiesResult = getEntitiesResult(iEntityManagerHelper);
        List<String> clusters = new ArrayList<String>();
        for (EntityResult entity : entitiesResult.getEntities()) {
            clusters.add(entity.getName());
        }
        return clusters;
    }

    public static EntitiesResult getEntitiesResult(IEntityManagerHelper iEntityManagerHelper)
        throws IOException, URISyntaxException, AuthenticationException, JAXBException {
        final ServiceResponse clusterResponse =
            iEntityManagerHelper.listEntities(Util.URLS.LIST_URL);
        JAXBContext jc = JAXBContext.newInstance(EntitiesResult.class);
        Unmarshaller u = jc.createUnmarshaller();
        return (EntitiesResult) u.unmarshal(
            new StringReader(clusterResponse.getMessage()));
    }
}
