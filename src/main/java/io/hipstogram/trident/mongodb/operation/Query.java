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

package io.hipstogram.trident.mongodb.operation;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;

/**
 * Query Operation
 * @author Andrés Sánchez
 */
public class Query extends CRUDOperation
{
    // Query
    private BasicDBObject query;

    // Projection
    private BasicDBObject projection;

    /**
     * Create a new query operation
     * @param query Query
     */
    public Query(BasicDBObject query) {
        this(query, null);
    }

    /**
     * Create a new query operation
     * @param query Query
     * @param projection Projection
     */
    public Query(BasicDBObject query, BasicDBObject projection) {
        super(Type.QUERY);
        this.query = query;
        this.projection = projection;
    }

    /**
     * Get query
     * @return Query
     */
    public BasicDBObject getQuery() {
        return query;
    }

    /**
     * Get projection
     * @return Projection
     */
    public BasicDBObject getProjection() {
        return projection;
    }

    @Override
    public void addToBulkOperation(BulkWriteOperation bulk) {}
}
