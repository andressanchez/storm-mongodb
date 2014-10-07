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

package io.hipstogram.trident.mongodb.mappers;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import storm.trident.tuple.TridentTuple;

/**
 * MongoDB Row Mapper
 * @param <K> Key type
 * @param <V> Value type
 * @author Andrés Sánchez
 */
public interface MongoDBRowMapper <K, V>
{
    public BasicDBObject map(K key, V value);
    public BasicDBObject map(TridentTuple tuple);
    public BasicDBObject retrieve(K key);
    public V getValue(DBObject doc);
}