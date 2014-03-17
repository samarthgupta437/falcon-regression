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
package org.apache.falcon.regression.core.interfaces;

import org.apache.falcon.regression.core.helpers.ClusterEntityHelperImpl;
import org.apache.falcon.regression.core.helpers.DataEntityHelperImpl;
import org.apache.falcon.regression.core.helpers.ProcessEntityHelperImpl;
import org.apache.falcon.regression.core.enumsAndConstants.ENTITY_TYPE;

public class EntityHelperFactory {

    public static IEntityManagerHelper getEntityHelper(ENTITY_TYPE type) {
        if (type.equals(ENTITY_TYPE.DATA)) {
            return new DataEntityHelperImpl();
        }

        if (type.equals(ENTITY_TYPE.CLUSTER)) {
            return new ClusterEntityHelperImpl();
        }

        if (type.equals(ENTITY_TYPE.PROCESS)) {
            return new ProcessEntityHelperImpl();
        }

        return null;
    }

    public static IEntityManagerHelper getEntityHelper(ENTITY_TYPE type, String envFileName,
                                                       String prefix)
     {
        if (type.equals(ENTITY_TYPE.DATA)) {
            return new DataEntityHelperImpl(envFileName, prefix);
        }

        if (type.equals(ENTITY_TYPE.CLUSTER)) {
            return new ClusterEntityHelperImpl(envFileName, prefix);
        }

        if (type.equals(ENTITY_TYPE.PROCESS)) {
            return new ProcessEntityHelperImpl(envFileName, prefix);
        }

        return null;
    }
}
