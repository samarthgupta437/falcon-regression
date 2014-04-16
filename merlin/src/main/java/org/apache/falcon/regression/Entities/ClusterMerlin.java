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

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.falcon.regression.core.generated.cluster.Cluster;
import org.apache.falcon.regression.core.generated.cluster.Location;
import org.apache.falcon.regression.core.enumsAndConstants.ClusterLocationTypes;
import org.apache.falcon.regression.core.util.InstanceUtil;

import javax.xml.bind.JAXBException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

public class ClusterMerlin extends org.apache.falcon.regression.core.generated
  .cluster.Cluster {


  public ClusterMerlin(String clusterData) throws JAXBException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
    Cluster element = InstanceUtil.getClusterElement(clusterData);
    Field[] fields = Cluster.class.getDeclaredFields();
    for (Field fld : fields) {
      PropertyUtils.setProperty(this, fld.getName(),
        PropertyUtils.getProperty(element, fld.getName()));
    }
  }

  public String getLocation(ClusterLocationTypes locationType) {
    for(Location l : getLocations().getLocation()) {
       if (locationType.getValue().equals(l.getName()))
          return l.getPath();
    }
    return null;
  }
}
