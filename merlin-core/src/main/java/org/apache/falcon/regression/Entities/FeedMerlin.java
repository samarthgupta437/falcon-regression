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

package org.apache.falcon.regression.Entities;

import org.apache.falcon.regression.core.generated.feed.Cluster;
import org.apache.falcon.regression.core.generated.feed.ClusterType;
import org.apache.falcon.regression.core.generated.feed.Feed;
import org.apache.falcon.regression.core.util.InstanceUtil;

import javax.xml.bind.JAXBException;

public class FeedMerlin extends org.apache.falcon.regression.core.generated
  .feed.Feed {
  Feed element;

  public FeedMerlin(String entity) throws JAXBException {
    element = InstanceUtil.getFeedElement(entity);
  }

  /*
    all Merlin specific operations
     */
  public String getTargetCluster(){

    for(Cluster c : getClusters().getCluster()){
       if(c.getType().equals(ClusterType.TARGET))
         return c.getName();
    }

    return "";
  }
}
