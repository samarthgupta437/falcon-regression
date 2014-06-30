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

package org.apache.falcon.regression.core.helpers;

import org.apache.falcon.regression.core.interfaces.EntityHelperFactory;
import org.apache.falcon.regression.core.interfaces.IEntityManagerHelper;
import org.apache.falcon.regression.core.enumsAndConstants.ENTITY_TYPE;
import org.apache.log4j.Logger;


public class PrismHelper {

    private static Logger logger = Logger.getLogger(PrismHelper.class);
    protected IEntityManagerHelper clusterHelper;
    protected IEntityManagerHelper processHelper;
    protected IEntityManagerHelper feedHelper;

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

    public String getPrefix() {
        return prefix;
    }

    protected String prefix;

    public String getEnvFileName() {
        return envFileName;
    }

    public PrismHelper(String envFileName, String prefix) {
        try {
            this.envFileName = envFileName;
            this.prefix = prefix;
            clusterHelper =
                EntityHelperFactory.getEntityHelper(ENTITY_TYPE.CLUSTER, this.envFileName,
                    prefix);
            processHelper =
                EntityHelperFactory
                    .getEntityHelper(ENTITY_TYPE.PROCESS, this.envFileName, prefix);
            feedHelper =
                EntityHelperFactory.getEntityHelper(ENTITY_TYPE.FEED, this.envFileName, prefix);

        } catch (Exception e) {
            logger.info(e.getMessage());
        }

    }

}
