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

import org.apache.falcon.regression.core.generated.cluster.Cluster;
import org.apache.falcon.regression.core.generated.cluster.Location;
import org.apache.falcon.regression.core.generated.cluster.Locations;
import org.apache.falcon.regression.core.supportClasses.ClusterLocationTypes;
import org.apache.falcon.regression.core.util.InstanceUtil;

import javax.xml.bind.JAXBException;

public class ClusterMerlin extends org.apache.falcon.regression.core.generated
  .cluster.Cluster {

  public Cluster element;

  public ClusterMerlin(String clusterData) throws JAXBException {
    element = InstanceUtil.getClusterElement(clusterData);
  }

  public String getLocation(ClusterLocationTypes locationType) {
    for(Location l : element.getLocations().getLocation()) {
       if (locationType.getValue().equals(l.getName()))
          return l.getPath();
    }
    return null;
  }
}
