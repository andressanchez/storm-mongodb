/**
 *  Copyright 2014 Andrés Sánchez Pascual
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.hipstogram.trident.mongodb;

import backtype.storm.task.IMetricsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

/**
 * StateFactory for MongoDB
 * @author Andrés Sánchez
 */
public class MongoDBStateFactory implements StateFactory
{
    // Property Names
    public static final String MONGODB_HOSTS = "mongodb.hosts";
    public static final String MONGODB_DB = "mongodb.db";
    public static final String MONGODB_COLLECTION = "mongodb.coll";

    // Serialization
    private static final long serialVersionUID = 1L;

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(MongoDBStateFactory.class);

    // A MongoDB client
    private static MongoDBClient client;

    @Override
    public State makeState(Map configuration, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        // worth synchronizing here?
        if (client == null) {
            client = new MongoDBClient(configuration);
        }
        LOG.debug("Creating State for partition [{}] of [{}]", new Object[]{partitionIndex, numPartitions});
        return new MongoDBState(MongoDBStateFactory.client, configuration);
    }
}
