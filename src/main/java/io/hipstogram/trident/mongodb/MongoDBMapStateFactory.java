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
import backtype.storm.tuple.Values;
import io.hipstogram.trident.mongodb.mappers.MongoDBRowMapper;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.map.*;

import java.util.Map;

/**
 * StateFactory for MongoDB
 * @author Andrés Sánchez
 */
public class MongoDBMapStateFactory implements StateFactory {
    private static final long serialVersionUID = 1L;

    private MongoDBClient clientFactory;
    private StateType stateType;
    private MongoDBMapState.Options<?> options;
    private MongoDBRowMapper mapper;

    /**
     * Create a new StateFactory for MongoDB
     * @param mapper MongoDB Row Mapper
     * @param stateType State type: Non-transactional, transactional or opaque
     * @param options Set of options for MongoDB
     */
    public MongoDBMapStateFactory(MongoDBRowMapper mapper, StateType stateType, MongoDBMapState.Options options) {
        this.stateType = stateType;
        this.options = options;
        this.mapper = mapper;
    }

    /**
     * Create a new state from a given configuration
     * @param configuration A set of properties
     * @param metrics Storm metrics
     * @param partitionIndex Partition index
     * @param numPartitions Number of partitions
     * @return A new state
     */
    public State makeState(Map configuration, IMetricsContext metrics, int partitionIndex, int numPartitions) {

        if (clientFactory == null) {
            clientFactory = new MongoDBClient(configuration);
        }

        MongoDBMapState state = new MongoDBMapState(clientFactory, mapper, options, configuration);
        state.registerMetrics(configuration, metrics);

        CachedMap cachedMap = new CachedMap(state, options.localCacheSize);

        MapState mapState;
        if (stateType == StateType.NON_TRANSACTIONAL) {
            mapState = NonTransactionalMap.build(cachedMap);
        } else if (stateType == StateType.OPAQUE) {
            mapState = OpaqueMap.build(cachedMap);
        } else if (stateType == StateType.TRANSACTIONAL) {
            mapState = TransactionalMap.build(cachedMap);
        } else {
            throw new RuntimeException("Unknown state type: " + stateType);
        }

        return new SnapshottableMap(mapState, new Values(options.globalKey));
    }

}
