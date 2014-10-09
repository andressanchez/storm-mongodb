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

import com.mongodb.BulkWriteOperation;

/**
 * MongoDB CRUD Operation
 * @author Andrés Sánchez
 */
public abstract class CRUDOperation
{
    // Operation types
    public enum Type { INSERT, QUERY, UPDATE, UPSERT }

    // Type associated to this operation
    private Type type;

    /**
     * Create a new CRUD Operation
     * @param type Associated type
     */
    public CRUDOperation(Type type) {
        this.type = type;
    }

    /**
     * Get the associated type
     * @return Associated type
     */
    public Type getType() {
        return type;
    }

    /**
     * Set the associated type
     * @param type Associated type
     */
    public void setType(Type type) {
        this.type = type;
    }

    /**
     * Add this operation to a bulk operation
     * @param bulk Bulk operation
     */
    public abstract void addToBulkOperation(BulkWriteOperation bulk);
}
