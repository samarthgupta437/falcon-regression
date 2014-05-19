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

package org.apache.falcon.regression.core.response.graph;

import com.google.gson.annotations.SerializedName;

public class Vertex {
    public static enum _TYPE {
        @SerializedName("VERTEX")VERTEX,
        @SerializedName("cluster-entity")EDGE
    }

    public static enum TYPE {
        @SerializedName("cluster-entity")CLUSTER_ENTITY,
        @SerializedName("feed-entity")FEED_ENTITY,
        @SerializedName("data-center")DATA_CENTER;
    }

    int _id;
    _TYPE _type;
    String name;
    TYPE type;
    String timestamp;

    public String getTimestamp() {
        return timestamp;
    }

    public TYPE getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public _TYPE get_type() {
        return _type;
    }

    public int get_id() {
        return _id;
    }

    @Override
    public String toString() {
        return "Vertex{" +
            "_id=" + _id +
            ", _type='" + _type + '\'' +
            ", name='" + name + '\'' +
            ", type='" + type + '\'' +
            ", timestamp='" + timestamp + '\'' +
            '}';
    }
}
