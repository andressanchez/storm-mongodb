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

import com.mongodb.BasicDBObject;
import io.hipstogram.trident.mongodb.mappers.MongoDBRowMapper;
import io.hipstogram.trident.mongodb.operation.CRUDOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.List;
import java.util.Map;

/**
 * MongoDB State Updater
 * @param <K> Key type
 * @param <V> Value type
 * @author Andrés Sánchez
 */
public class MongoDBStateUpdater<K, V> implements StateUpdater<MongoDBState>
{
    // Serialization
    private static final long serialVersionUID = 1L;

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(MongoDBStateUpdater.class);

    // MongoDB Mapper
    private MongoDBRowMapper<K,V> mapper = null;

    /**
     * Create a new instance of MongoDBStateUpdater
     * @param mapper MongoDB Row Mapper
     */
    public MongoDBStateUpdater(MongoDBRowMapper<K, V> mapper) {
        this.mapper = mapper;
    }

    @Override
    public void prepare(Map configuration, TridentOperationContext context) {
        LOG.debug("Preparing updater with [{}]", configuration);
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void updateState(MongoDBState state, List<TridentTuple> tuples, TridentCollector collector) {
        for (TridentTuple tuple : tuples) {
            CRUDOperation operation = this.mapper.map(tuple);
            state.addOperation(operation);
        }
    }
}