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

import com.mongodb.*;
import io.hipstogram.trident.mongodb.operation.CRUDOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.state.State;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MongoDBState implements State
{
    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(MongoDBState.class);

    // Default batch size
    private static final int DEFAULT_MAX_BATCH_SIZE = 100;

    // A MongoDB client
    private MongoDBClient client;

    // A set of properties
    private Map configuration;

    // The actual batch size
    private int maxBatchSize;

    // List with a MongoDB operations
    List<CRUDOperation> operations = new ArrayList<CRUDOperation>();

    /**
     * Create a new MongoDB State
     * @param client A MongoDB client
     * @param configuration A set of properties
     */
    public MongoDBState(MongoDBClient client, Map configuration) {
        this.client = client;
        this.configuration = configuration;
        this.maxBatchSize = DEFAULT_MAX_BATCH_SIZE;
    }

    /**
     * Create a new MongoDB State
     * @param client A MongoDB client
     * @param maxBatchSize Batch size
     * @param configuration A set of properties
     */
    public MongoDBState(MongoDBClient client, int maxBatchSize, Map configuration) {
        this.client = client;
        this.configuration = configuration;
        this.maxBatchSize = maxBatchSize;
    }

    /**
     * Add a new operation to the operation list
     * @param operation A CRUD operation
     */
    public void addOperation(CRUDOperation operation) {
        this.operations.add(operation);
    }

    /**
     * Execute a MongoDB statement
     * @param statement A CRUD statement
     * @return The result of the operation
     */
    public DBCursor execute(BasicDBObject statement){
        return client.getCollection(configuration).find(statement);
    }

    @Override
    public void beginCommit(Long txid) {
    }

    @Override
    public void commit(Long txid) {
        LOG.debug("Commiting [{}]", txid);
        DBCollection coll = client.getCollection(configuration);
        BulkWriteOperation builder = coll.initializeOrderedBulkOperation();

        int i = 0;
        for(CRUDOperation operation : this.operations) {
            operation.addToBulkOperation(builder);
            i++;
            if(i >= this.maxBatchSize) {
                builder.execute();
                builder = coll.initializeOrderedBulkOperation();
                i = 0;
            }
        }

        builder.execute();
    }
}
