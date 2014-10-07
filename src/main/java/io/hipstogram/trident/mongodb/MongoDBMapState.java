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

import backtype.storm.Config;
import backtype.storm.metric.api.CountMetric;
import backtype.storm.task.IMetricsContext;
import com.mongodb.*;
import io.hipstogram.trident.mongodb.mappers.MongoDBRowMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.state.OpaqueValue;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.TransactionalValue;
import storm.trident.state.map.IBackingMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * MongoDB Map State
 * @param <T> The generic state to back
 * @author Andrés Sánchez
 */
public class MongoDBMapState<T> implements IBackingMap<T>
{
    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(MongoDBMapState.class);

    // MongoDB Mapper
    private MongoDBRowMapper mapper;

    // MongoDB Client
    private MongoDBClient client;

    // Options
    private  Options<?> options;

    // Set of properties
    private Map configuration;

    // Collection
    private DBCollection coll;

    // Metrics for storm metrics registering
    private CountMetric _mreads;
    private CountMetric _mwrites;
    private CountMetric _mexceptions;

    // MongoDB Options class
    public static class Options<T> implements Serializable {
        public int localCacheSize = 5000;
        public String globalKey = "globalkey";
        public String db = "test";
        public String collection = "mycollection";
    }

    /**
     * Create a MongoDB Map State
     * @param client A MongoDB client
     * @param mapper Row Mapper
     * @param options Options for MongoDB
     * @param configuration A set of properties
     */
    public MongoDBMapState(MongoDBClient client, MongoDBRowMapper mapper, Options<?> options, Map configuration)
    {
        this.client = client;
        this.mapper = mapper;
        this.options = options;
        this.configuration = configuration;
        this.coll = client.getDB(options.db).getCollection(options.collection);
    }

    /**
     * Register Metrics in Storm
     * @param conf A set of properties
     * @param context Metrics context
     */
    public void registerMetrics(Map conf, IMetricsContext context) {
        int bucketSize = (Integer) (conf.get(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS));
        _mreads = context.registerMetric("mongodb/readCount", new CountMetric(), bucketSize);
        _mwrites = context.registerMetric("mongodb/writeCount", new CountMetric(), bucketSize);
        _mexceptions = context.registerMetric("mongodb/exceptionCount", new CountMetric(), bucketSize);
    }

    /**
     * Create a Opaque StateFactory
     * @param mapper Row Mapper
     * @return A Opaque StateFactory
     */
    public static StateFactory opaque(MongoDBRowMapper mapper) {
        Options<OpaqueValue> options = new Options<OpaqueValue>();
        return opaque(mapper, options);
    }

    /**
     * Create a Opaque StateFactory
     * @param mapper Row Mapper
     * @param opts Options for MongoDB
     * @return A Opaque StateFactory
     */
    public static StateFactory opaque(MongoDBRowMapper mapper, Options<OpaqueValue> opts) {
        return new MongoDBMapStateFactory(mapper, StateType.OPAQUE, opts);
    }

    /**
     * Create a Transactional StateFactory
     * @param mapper Row Mapper
     * @return A Transactional StateFactory
     */
    public static StateFactory transactional(MongoDBRowMapper mapper) {
        Options<TransactionalValue> options = new Options<TransactionalValue>();
        return transactional(mapper, options);
    }

    /**
     * Create a Transactional StateFactory
     * @param mapper Row Mapper
     * @param opts Options for MongoDB
     * @return A Transactional StateFactory
     */
    public static StateFactory transactional(MongoDBRowMapper mapper, Options<TransactionalValue> opts) {
        return new MongoDBMapStateFactory(mapper, StateType.TRANSACTIONAL, opts);
    }

    /**
     * Create a Non-Transactional StateFactory
     * @param mapper Row Mapper
     * @return A Non-Transactional StateFactory
     */
    public static StateFactory nonTransactional(MongoDBRowMapper mapper) {
        Options<Object> options = new Options<Object>();
        return nonTransactional(mapper, options);
    }

    /**
     * Create a Non-Transactional StateFactory
     * @param mapper Row Mapper
     * @param opts Options for MongoDB
     * @return A Non-Transactional StateFactory
     */
    public static StateFactory nonTransactional(MongoDBRowMapper mapper, Options<Object> opts) {
        return new MongoDBMapStateFactory(mapper, StateType.NON_TRANSACTIONAL, opts);
    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        try {
            List<T> values = new ArrayList<T>();

            for (List<Object> rowKey : keys) {
                BasicDBObject statement = mapper.retrieve(rowKey);
                DBCursor results = coll.find(statement);

                Iterator<DBObject> docIter = results.iterator();
                DBObject doc;
                if (results != null && docIter.hasNext() && (doc = docIter.next()) != null) {
                    if (docIter.hasNext()) {
                        LOG.error("Found non-unique value for key [{}]", rowKey);
                    } else {
                        values.add((T) mapper.getValue(doc));
                    }
                } else {
                    values.add(null);
                }
            }

            _mreads.incrBy(values.size());
            LOG.debug("Retrieving the following keys: {} with values: {}", keys, values);
            return values;
        } catch (Exception e) {
            throw new IllegalStateException("Impossible to reach this code");
        }
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> values) {
        LOG.debug("Putting the following keys: {} with values: {}", keys, values);
        try {
            BulkWriteOperation builder = coll.initializeOrderedBulkOperation();

            // Retrieve the mapping statement for the key,val pair
            for (int i = 0; i < keys.size(); i++) {
                List<Object> key = keys.get(i);
                T val = values.get(i);
                BasicDBObject statement = mapper.map(key, val);
                builder.insert(statement);
            }

            builder.execute();

            _mwrites.incrBy(keys.size());
        } catch (Exception e) {
            LOG.error("Exception {} caught.", e);
        }
    }
}
