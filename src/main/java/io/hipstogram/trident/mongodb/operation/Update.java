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
 * Update Operation
 * @author Andrés Sánchez
 */
public class Update extends CRUDOperation
{
    // Update query
    private BasicDBObject query;

    // Update statement
    private BasicDBObject statement;

    /**
     * Create a new update operation
     * @param query Update query
     * @param statement Update statement
     */
    public Update(BasicDBObject query, BasicDBObject statement) {
        super(Type.UPDATE);
        this.query = query;
        this.statement = statement;
    }

    @Override
    public void addToBulkOperation(BulkWriteOperation bulk) {
        bulk.find(query).update(statement);
    }
}
