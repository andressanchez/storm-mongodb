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
 * Insert Operation
 * @author Andrés Sánchez
 */
public class Insert extends CRUDOperation
{
    // Insert statement
    private BasicDBObject dbObject;

    /**
     * Create a new insert operation
     * @param dbObject Insert statement
     */
    public Insert(BasicDBObject dbObject) {
        super(Type.INSERT);
        this.dbObject = dbObject;
    }

    @Override
    public void addToBulkOperation(BulkWriteOperation bulk) {
        bulk.insert(dbObject);
    }
}
