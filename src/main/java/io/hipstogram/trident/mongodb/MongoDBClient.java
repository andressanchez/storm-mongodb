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

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.*;

/**
 * A MongoDB Client
 * @author Andrés Sánchez
 */
public class MongoDBClient
{
    // Serialization
    private static final long serialVersionUID = 1L;

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(MongoDBClient.class);

    // Database connections
    private Map<String, DB> databases = new HashMap<String, DB>();

    // MongoDB hosts
    private String[] hosts;

    // A MongoDB client (MongoDB Java API)
    private MongoClient client;

    /**
     * Create a new instance of a MongoDBClient
     * @param configuration Set of properties
     */
    public MongoDBClient(Map configuration) {
        String hostProperty = (String) configuration.get(MongoDBStateFactory.MONGODB_HOSTS);
        hosts = hostProperty.split(",");
    }

    /**
     * Get a MongoDB Collection given a set of properties
     * @param configuration Set of properties
     * @return The specified MongoDB Collection
     */
    public synchronized DBCollection getCollection(Map configuration) {
        String dbName = (String) configuration.get(MongoDBStateFactory.MONGODB_DB);
        String collName = (String) configuration.get(MongoDBStateFactory.MONGODB_COLLECTION);
        return getClient().getDB(dbName).getCollection(collName);
    }

    /**
     * Get an DB object to access a specific database
     * @param dbName Name of the database
     * @return A object to access that database
     */
    public synchronized DB getDB(String dbName) {
        DB db =  databases.get(dbName);
        if (db == null) {
            LOG.debug("Constructing DBCollection for collection [" + dbName + "]");
            db = getClient().getDB(dbName);
            databases.put(dbName, db);
        }
        return db;
    }

    /**
     * Get a MongoDB client (MongoDB Java API)
     * @return A MongoDB client
     */
    private MongoClient getClient() {
        if (client != null) {
            try {
                List<ServerAddress> addresses = new LinkedList<ServerAddress>();
                for (String host : hosts) addresses.add(new ServerAddress(host));
                client = new MongoClient(addresses);
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }
        return client;
    }
}
