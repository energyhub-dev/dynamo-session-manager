/***********************************************************************************************************************
 *
 * Dynamo Tomcat Sessions
 * ==========================================
 *
 * Copyright (C) 2012 by Dawson Systems Ltd (http://www.dawsonsystems.com)
 * Copyright (C) 2013 by EnergyHub Inc. (http://www.energyhub.com)
 *
 ***********************************************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package net.energyhub.session;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.util.Tables;
import org.apache.catalina.Container;
import org.apache.catalina.Context;
import org.apache.catalina.Lifecycle;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.LifecycleListener;
import org.apache.catalina.LifecycleState;
import org.apache.catalina.Loader;
import org.apache.catalina.Manager;
import org.apache.catalina.Session;
import org.apache.catalina.Valve;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DynamoManager implements Manager, Lifecycle, PropertyChangeListener {
    private static final Logger log = Logger.getLogger(DynamoManager.class.getName());

    public static final String COLUMN_ID = "id";
    public static final String COLUMN_LAST_ACCESSED = "lastAccessed";
    public static final String COLUMN_DATA = "data";

    public static final String INDEX_LAST_ACCESSED = "LastAccessedIndex";

    private final Object lifecycleMonitor = new Object();

    private String sessionTableName = "tomcat-sessions";

    private final ExecutorService executorService = Executors.newCachedThreadPool();

    private final ThreadLocal<DynamoSession> currentSession = new ThreadLocal<>();

    protected String awsAccessKey = "";  // Required for production environment
    protected String awsSecretKey = "";  // Required for production environment
    protected String dynamoEndpoint = ""; // used only for QA mock dynamo connections (not production)

    protected int maxInactiveInterval = 3600; // default in seconds

    protected boolean logSessionContents;
    protected boolean eventualConsistency;

    protected String statsdHost = "";
    protected int statsdPort = 8125;

    // maintain hash of attributes on load so we can compare against a
    // hash when deciding to update in dynamo
    private int originalAttributeHash = 0;

    //Either 'kryo' or 'java'

    private String serializationStrategyClass = "net.energyhub.session.JavaSerializer";

    private Container container;
    private Serializer serializer;
    private StatsdClient statsdClient;

    private DynamoDB dynamoDb;

    private volatile LifecycleState lifecycleState = LifecycleState.NEW;

    /////////////////////////////////////////////////////////////////
    //   Getters and Setters for Implementation Properties
    /////////////////////////////////////////////////////////////////
    public String getDynamoEndpoint() {
        return dynamoEndpoint;
    }

    public void setDynamoEndpoint(String endpoint) {
        this.dynamoEndpoint = endpoint;
    }

    public String getAwsAccessKey() {
        return awsAccessKey;
    }

    public void setAwsAccessKey(String awsAccessKey) {
        this.awsAccessKey = awsAccessKey;
    }

    public String getAwsSecretKey() {
        return awsSecretKey;
    }

    public void setAwsSecretKey(String awsSecretKey) {
        this.awsSecretKey = awsSecretKey;
    }

    public boolean getLogSessionContents() {
        return logSessionContents;
    }

    public void setLogSessionContents(boolean logSessionContents) {
        this.logSessionContents = logSessionContents;
    }

    public boolean getEventualConsistency() {
        return eventualConsistency;
    }

    public void setEventualConsistency(boolean eventualConsistency) {
        this.eventualConsistency = eventualConsistency;
    }

    public void setStatsdHost(String statsdHost) {
        this.statsdHost = statsdHost;
    }
    public String getStatsdHost() {
        return statsdHost;
    }

    public void setStatsdPort(int statsdPort) {
        this.statsdPort = statsdPort;
    }
    public int getStatsdPort() {
        return statsdPort;
    }

    public void setSessionTableName(String sessionTableName) {
        this.sessionTableName = sessionTableName;
    }

    public void setSerializationStrategyClass(String strategy) {
        this.serializationStrategyClass = strategy;
    }


    ////////////////////////////////////////////////////////////////////////////////
    //   Implement methods of Lifecycle
    ////////////////////////////////////////////////////////////////////////////////

    @Override
    public void addLifecycleListener(LifecycleListener lifecycleListener) {
    }

    @Override
    public LifecycleListener[] findLifecycleListeners() {
        return new LifecycleListener[0];  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void removeLifecycleListener(LifecycleListener lifecycleListener) {
    }

    @Override
    public void init() throws LifecycleException {
        updateLifecycleState(LifecycleState.INITIALIZED);
    }

    @Override
    public void start() throws LifecycleException {
        updateLifecycleState(LifecycleState.STARTING);

        log.info("Starting Dynamo Session Manager in container: " + this.getContainer().getName());
        for (Valve valve : getContainer().getPipeline().getValves()) {
            if (valve instanceof DynamoSessionTrackerValve) {
                DynamoSessionTrackerValve trackerValve = (DynamoSessionTrackerValve) valve;
                trackerValve.setDynamoManager(this);
                log.info("Attached to Dynamo Tracker Valve");
                break;
            }
        }

        try {
            initSerializer();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            log.log(Level.SEVERE, "Unable to load serializer", e);
            throw new LifecycleException(e);
        }

        initDbConnection();

        if (!getStatsdHost().isEmpty()) {
            log.info("Configuring statsd client on " + getStatsdHost() + ":" + getStatsdPort());
            this.statsdClient = new StatsdClient(getStatsdHost(), getStatsdPort());
        }
        log.info("Finished starting manager");

        updateLifecycleState(LifecycleState.STARTED);
    }

    protected AmazonDynamoDB initDynamoClient() {
        AmazonDynamoDB dynamoClient;
        if (awsAccessKey.isEmpty() || awsSecretKey.isEmpty()) {
            log.info("Connecting with default AWS credential provider chain");
            dynamoClient = new AmazonDynamoDBClient();
        } else {
            log.info("Connecting with explicitly provided credentials");
            dynamoClient = new AmazonDynamoDBClient(new BasicAWSCredentials(awsAccessKey, awsSecretKey));
        }

        if (!dynamoEndpoint.isEmpty()) {
            log.info("Setting dynamo endpoint: "  +  dynamoEndpoint);
            dynamoClient.setEndpoint(dynamoEndpoint);
        }
        return dynamoClient;
    }

    private void initDbConnection() throws LifecycleException {
        try {
            AmazonDynamoDB dynamoClient = initDynamoClient();
            dynamoDb = new DynamoDB(dynamoClient); // high level API
            if (!Tables.doesTableExist(dynamoClient, sessionTableName)) {
                log.info("Table " + sessionTableName + " doesn't exist -- creating");
                createTable();
            }
            Tables.awaitTableToBecomeActive(dynamoClient, sessionTableName);
            log.info("Connected to Dynamo for session storage. Session live time = " + (getMaxInactiveInterval()) + "s");

        } catch (Exception e) {
            throw new LifecycleException("Error Connecting to Dynamo", e);
        }
    }

    private void createTable() {
        // define schema: primary string index on id
        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition()
                .withAttributeName(COLUMN_ID)
                .withAttributeType(ScalarAttributeType.S));
        attributeDefinitions.add(new AttributeDefinition()
                .withAttributeName(COLUMN_LAST_ACCESSED)
                .withAttributeType(ScalarAttributeType.N));

        // primary index
        List<KeySchemaElement> keySchema = new ArrayList<>();
        keySchema.add(new KeySchemaElement().withAttributeName(COLUMN_ID).withKeyType(KeyType.HASH));

        // provisioned throughput for table
        ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput()
                .withWriteCapacityUnits(100l)
                .withReadCapacityUnits(100l);

        // secondary key index
        GlobalSecondaryIndex lastAccessedIndex = new GlobalSecondaryIndex()
                .withIndexName(INDEX_LAST_ACCESSED)
                .withKeySchema(new KeySchemaElement().withAttributeName(COLUMN_ID).withKeyType(KeyType.HASH),
                        new KeySchemaElement().withAttributeName(COLUMN_LAST_ACCESSED).withKeyType(KeyType.RANGE))
                .withProvisionedThroughput(new ProvisionedThroughput()
                        .withWriteCapacityUnits(100l)
                        .withReadCapacityUnits(100l))
                .withProjection(new Projection().withProjectionType(ProjectionType.KEYS_ONLY));

        CreateTableRequest createTableRequest  = new CreateTableRequest()
                .withTableName(sessionTableName)
                .withKeySchema(keySchema)
                .withAttributeDefinitions(attributeDefinitions)
                .withProvisionedThroughput(provisionedThroughput)
                .withGlobalSecondaryIndexes(lastAccessedIndex);

        try {
            dynamoDb.createTable(createTableRequest);
        } catch (ResourceInUseException e) {
            log.info("Tried to create an already existent table.");
        }
    }

    private void initSerializer() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        log.info("Attempting to use serializer :" + serializationStrategyClass);
        serializer = (Serializer) Class.forName(serializationStrategyClass).newInstance();

        Loader loader = null;

        if (container != null) {
            loader = container.getLoader();
        }
        ClassLoader classLoader = null;

        if (loader != null) {
            classLoader = loader.getClassLoader();
        }
        serializer.setClassLoader(classLoader);
    }

    @Override
    public void stop() throws LifecycleException {
        updateLifecycleState(LifecycleState.STOPPING);
        dynamoDb.shutdown();
        updateLifecycleState(LifecycleState.STOPPED);
    }

    @Override
    public void destroy() throws LifecycleException {
        executorService.shutdown();
        updateLifecycleState(LifecycleState.DESTROYED);
    }

    @Override
    public LifecycleState getState() {
        return lifecycleState;
    }

    @Override
    public String getStateName() {
        return lifecycleState.name();
    }

    //////////////////////////////////////////////////////////////////////////////////
    // Implement methods of Manager
    //////////////////////////////////////////////////////////////////////////////////

    @Override
    public Container getContainer() {
        return container;
    }

    @Override
    public void setContainer(Container container) {
        // de-register if necessary
        if (this.container != null && this.container instanceof Context) {
            this.container.removePropertyChangeListener(this);
        }
        this.container = container;
        if (this.container != null && this.container instanceof Context) {
            int interval = ((Context)this.container).getSessionTimeout() * 60;
            this.setMaxInactiveInterval(interval);
            // register for relevant configs
            this.container.addPropertyChangeListener(this);
        }
    }

    /**
     * Process property change events from our associated Context.
     * @param event The property change event that has occurred
     */
    @Override
    public void propertyChange(PropertyChangeEvent event) {
        if (!(event.getSource() instanceof Context)) {
            return;
        }
        // only the sessionTimeout property is relevant
        if (event.getPropertyName().equals("sessionTimeout")) {
            try {
                int interval = ((Integer)event.getNewValue()) * 60;
                setMaxInactiveInterval(interval);
            } catch (NumberFormatException e) {
                log.severe("Invalid sessionTimeout setting: " + event.getNewValue().toString());
            }
        }
    }

    @Override
    public boolean getDistributable() {
        return false;
    }

    @Override
    public void setDistributable(boolean b) {

    }

    @Override
    public String getInfo() {
        return "Dynamo Session Manager";
    }

    @Override
    public int getMaxInactiveInterval() {
        return maxInactiveInterval;
    }

    @Override
    public void setMaxInactiveInterval(int i) {
        log.info("Session timeout is set to " + i + " seconds");
        maxInactiveInterval = i;
    }

    @Override
    public int getSessionIdLength() {
        return 37;
    }

    @Override
    public void setSessionIdLength(int i) {

    }

    @Override
    public long getSessionCounter() {
        return 10000000;
    }

    @Override
    public void setSessionCounter(long l) {

    }

    @Override
    public int getMaxActive() {
        return 1000000;
    }

    @Override
    public void setMaxActive(int i) {

    }

    @Override
    public int getActiveSessions() {
        return 1000000;
    }

    @Override
    public long getExpiredSessions() {
        return 0;
    }

    @Override
    public void setExpiredSessions(long l) {

    }

    @Override
    public int getRejectedSessions() {
        return 0;
    }

    @Override
    public int getSessionMaxAliveTime() {
        return maxInactiveInterval;
    }

    @Override
    public void setSessionMaxAliveTime(int i) {

    }

    @Override
    public int getSessionAverageAliveTime() {
        return 0;
    }

    @Override
    public int getSessionCreateRate() {
        return 0;
    }

    @Override
    public int getSessionExpireRate() {
        return 0;
    }

    @Override
    public void load() throws ClassNotFoundException, IOException {

    }

    @Override
    public void unload() throws IOException {

    }

    @Override
    public void add(Session session) {
        try {
            save((DynamoSession)session);
        } catch (IOException ex) {
            log.log(Level.SEVERE, "Error adding new session", ex);
        }
    }

    @Override
    public void addPropertyChangeListener(PropertyChangeListener propertyChangeListener) {

    }

    @Override
    public void removePropertyChangeListener(PropertyChangeListener propertyChangeListener) {

    }

    @Override
    public void backgroundProcess() {
        long cutoffMillis = System.currentTimeMillis() - (maxInactiveInterval * 1000l);

        String conditionExpression = "#la < :v_last_accessed_cutoff";

        NameMap nameMap = new NameMap().with("#la", COLUMN_LAST_ACCESSED);
        ValueMap valueMap= new ValueMap().withNumber(":v_last_accessed_cutoff", cutoffMillis);
        ScanSpec scanSpec = new ScanSpec()
                .withFilterExpression(conditionExpression)
                .withNameMap(nameMap)
                .withValueMap(valueMap);

        Table table = dynamoDb.getTable(sessionTableName);

        // query index for expired
        for (Item item : table.getIndex(INDEX_LAST_ACCESSED).scan(scanSpec)) {
            // launch task to delete each one -- enforcing condition at time of deletion too, to avoid race condition
            executorService.submit(() -> {
                String sessionId = item.getString(COLUMN_ID);

                DeleteItemOutcome deleteItemOutcome = table.deleteItem(new DeleteItemSpec()
                        .withPrimaryKey(COLUMN_ID, sessionId)
                        .withConditionExpression(conditionExpression)
                        .withNameMap(nameMap)
                        .withReturnValues(ReturnValue.ALL_OLD)
                        .withValueMap(valueMap));

                if (deleteItemOutcome.getItem() != null) {
                    log.finer("Repeaed session " + sessionId + " with last accessed " + deleteItemOutcome.getItem().getNumber(COLUMN_LAST_ACCESSED));
                }
            });
        }
    }

    public void afterRequest() {
        try {
            DynamoSession session = currentSession.get();
            if (session != null) {
                if (session.isValid()) {
                    if (log.isLoggable(Level.FINE)) {
                        log.fine("Request with session completed, saving session " + session.getId());
                    }
                    if (session.getSession() != null) {
                        if (log.isLoggable(Level.FINE)) {
                            log.fine("HTTP Session present, saving " + session.getId());
                        }
                        try {
                            save(session);
                        } catch (IOException e) {
                            log.severe("IOException while saving " + session.getIdInternal());
                            e.printStackTrace();
                        }
                    } else {
                        if (log.isLoggable(Level.FINE)) {
                            log.fine("No HTTP Session present, Not saving " + session.getId());
                        }
                    }
                } else {
                    if (log.isLoggable(Level.FINE)) {
                        log.fine("HTTP Session has been invalidated, removing :" + session.getId());
                    }
                    remove(session);
                }
            }
        } finally {
            currentSession.remove();
        }
    }

    @Override
    public void changeSessionId(Session session) {
        session.setId(UUID.randomUUID().toString());
    }

    @Override
    public Session createEmptySession() {
        DynamoSession session = new DynamoSession(this);
        session.setId(UUID.randomUUID().toString());
        session.setMaxInactiveInterval(maxInactiveInterval);
        session.setValid(true);
        session.setCreationTime(System.currentTimeMillis());
        session.setNew(true);
        setCurrentSession(session);
        log.fine("Created new empty session " + session.getIdInternal());
        return session;
    }

    @Override
    public org.apache.catalina.Session createSession(java.lang.String sessionId) {
        DynamoSession session = (DynamoSession) createEmptySession();

        if (sessionId != null) {
            session.setId(sessionId);
        }

        return session;
    }

    /**
     * There is no good way to return a list of sessions from dynamo without scanning the table.
     * It is a design goal of this project to avoid scanning the table, so this method just returns
     * an empty array.
     * @return an empty array
     */
    @Override
    public org.apache.catalina.Session[] findSessions() {
        return new Session[]{};
    }

    @Override
    public Session findSession(String id) throws IOException {
        long t0 = System.currentTimeMillis();
        if (id == null || id.length() == 0) {
            return createEmptySession();
        }

        DynamoSession session = currentSession.get();
        if (session != null) {
            if (id.equals(session.getId())) {
                return session;
            } else {
                currentSession.remove();
            }
        }

        try {
           if (log.isLoggable(Level.FINE)) {
                log.fine("Loading session " + id + " from Dynamo, current = " + sessionTableName);
            }

            GetItemSpec spec = new GetItemSpec()
                    .withPrimaryKey(COLUMN_ID, id)
                    .withConsistentRead(!eventualConsistency);

            Item item = dynamoDb.getTable(sessionTableName).getItem(spec);
            if (item == null) {
                log.info("Existing session " + id + " not found in Dynamo");
                return null;
            }

            ByteBuffer data = item.getByteBuffer(COLUMN_DATA);
            Long lastAccessed;
            {
                Number lastAccessedNumber = item.getNumber(COLUMN_LAST_ACCESSED);
                if (lastAccessedNumber != null) {
                    lastAccessed = lastAccessedNumber.longValue();
                } else {
                    lastAccessed = System.currentTimeMillis();
                }
            }
            if (log.isLoggable(Level.FINE)) {
                log.fine("Session " + id + " lastAccessed at " + lastAccessed);
            }

            session = (DynamoSession) createEmptySession();
            session.setId(id);
            session.setManager(this);
            long t2 = System.currentTimeMillis();
            serializer.deserializeInto(data, session);
            long t3 = System.currentTimeMillis();

            if (log.isLoggable(Level.FINE)) {
                log.fine("Deserialized session in " + (t3-t2) + "ms");
            }

            // assert active
            long now = System.currentTimeMillis();
            if (!isActive(lastAccessed, now, session.getMaxInactiveInterval())) {
                log.fine("Existing session " + id + " expired, so creating a new one: " +
                        "last accessed = " + lastAccessed +
                        ", now = " + now +
                        ", max inactive = " + session.getMaxInactiveInterval());
                session.expire(); // internal processing, whatever that means
                remove(session); // delete
                return null; // return null if the session is inactive
            }

            session.setValid(true);
            session.setNew(false);

            if (logSessionContents && log.isLoggable(Level.FINE)) {
                log.fine("Session Contents [" + session.getId() + "]:");
                for (Object name : Collections.list(session.getAttributeNames())) {
                    log.fine("  " + name.toString());
                }
            }

            // Set the lastAccessedTime according to the lastAccessedTime from the dynamo record,
            // since we don't save the serialized session itself if attributes haven't changed
            session.setLastAccessedTime(lastAccessed);

            long t1 = System.currentTimeMillis();

            if (log.isLoggable(Level.FINE)) {
                log.fine("Loaded session id " + id + " in " + (t1-t0) + "ms");
            }
            if (statsdClient != null) {
                statsdClient.time("session.load", t0, t1);
            }
            setCurrentSession(session);
            return session;
        } catch (IOException e) {
            log.severe(e.getMessage());
            throw e;
        } catch (ClassNotFoundException e) {
            log.severe("Unable to deserialize session (class not found)");
            e.printStackTrace();
            throw new IOException("Unable to deserializeInto session", e);
        } catch (Exception e) {
            log.severe("Unexpected Error in dynamo session manager: " + e.getMessage());
            e.printStackTrace();
            throw new IOException(e);
        }
    }

    private boolean isActive(long lastAccessedTs, long nowTs, int maxInactiveSeconds) {
        if (maxInactiveSeconds < 0) {
            return true;
        }
        long maxInactiveMilli = maxInactiveSeconds * 1000;
        long inactiveMilli = (nowTs - lastAccessedTs);
        return (inactiveMilli < maxInactiveMilli);
    }

    private int hashSession(DynamoSession session) {
        int prime = 31;
        int hash = 0;
        List<String> attrNames = Collections.list(session.getAttributeNames());
        Collections.sort(attrNames);
        for (String name : attrNames) {
            hash = prime * hash + session.getAttribute(name).hashCode();
        }
        return hash;
    }

    /**
     * Store the session and attributes at create or load time, for comparison later on.
     * @param session the session
     */
    protected void setCurrentSession(DynamoSession session) {
        currentSession.set(session);
        originalAttributeHash = hashSession(session);
    }

    public void save(DynamoSession dynamoSession) throws IOException {
        long t0 = System.currentTimeMillis();
        try {
            if (log.isLoggable(Level.FINE)) {
                log.fine("Saving session " + dynamoSession.getIdInternal() + " into Dynamo (" + sessionTableName + ")");
            }

            Map<String, AttributeValue> dbData = new HashMap<>();
            // We save lastAccessed on every access
            dbData.put(COLUMN_LAST_ACCESSED, new AttributeValue().withN(Long.toString(System.currentTimeMillis(), 10)));

            if (dynamoSession.isNew()) {
                putSessionInDynamo(dynamoSession); // new session, use PutItem
            } else {
                updateSessionInDynamo(dynamoSession); // existing session, use UpdateItem
            }

            long t1 = System.currentTimeMillis();
            if (log.isLoggable(Level.FINE)) {
                log.fine("Updated session with id " + dynamoSession.getIdInternal() + " in " + (t1 - t0) + "ms");
            }
            if (statsdClient != null) {
                statsdClient.time("session.save", t0, t1);
            }
        } catch (IOException e) {
            log.severe(e.getMessage());
            throw e;
        } finally {
            currentSession.remove();
            originalAttributeHash = 0;
            if (log.isLoggable(Level.FINE)) {
                log.fine("Session " + dynamoSession.getIdInternal() + " removed from ThreadLocal");
            }
        }
    }

    /**
     * Put a new session into Dynamo using PutItemRequest API
     * @param session the session
     * @return how many units were consumed
     * @throws IOException if something bad happens
     */
    protected void putSessionInDynamo(DynamoSession session) throws IOException {
        // New session, do PutItem
        if (log.isLoggable(Level.FINE)) {
            log.fine("Storing new session for " + session.getIdInternal());
        }

        Item item = new Item()
                .withPrimaryKey(COLUMN_ID, session.getIdInternal())
                .withNumber(COLUMN_LAST_ACCESSED, System.currentTimeMillis())
                .withBinary(COLUMN_DATA, serializer.serializeFrom(session));

        dynamoDb.getTable(sessionTableName).putItem(item);
    }

    /**
     * Update an existing session in Dynamo using UpdateItemRequest
     * @param session the session
     * @throws IOException if something bad happens
     */
    protected void updateSessionInDynamo(DynamoSession session) throws IOException {
        Map<String, String> expressionAttributeNames = new HashMap<>();
        Map<String, Object> expressionAttributeValues = new HashMap<>();

        String updateExpression = "set #T = :val1";
        expressionAttributeNames.put("#T", COLUMN_LAST_ACCESSED);
        expressionAttributeValues.put(":val1", System.currentTimeMillis());
        // Only set the session data if attributes have changed.
        if (haveAttributesChanged(session)) {
            if (log.isLoggable(Level.FINE)) {
                log.fine("Attributes have changed, saving session data for " + session.getIdInternal());
            }
            expressionAttributeNames.put("#B", COLUMN_DATA);
            expressionAttributeValues.put(":val2", serializer.serializeFrom(session));

            updateExpression = "set #T = :val1 set #B = :val2";
        } else if (log.isLoggable(Level.FINE)) {
            log.fine("Attributes have not changed, saving session data for " + session.getIdInternal());
        }

        dynamoDb.getTable(sessionTableName)
                .updateItem(COLUMN_ID, session.getIdInternal(), updateExpression, expressionAttributeNames, expressionAttributeValues);
    }


    /**
     * If the original session and the current session have the same attributes (and values) then return false,
     * otherwise return true.
     * @param session the session to check
     * @return whether attributes have changed
     */
    protected boolean haveAttributesChanged(DynamoSession session) {
        if (logSessionContents && log.isLoggable(Level.FINE)) {
            log.fine("Session Contents [" + session.getId() + "]:");
        }
        return hashSession(session) != originalAttributeHash;
    }

    @Override
    public void remove(Session session) {
        if (log.isLoggable(Level.FINE)) {
            log.fine("Removing session ID: " + session.getId());
        }
        try {
            dynamoDb.getTable(sessionTableName).deleteItem(COLUMN_ID, session.getIdInternal());
        } catch (Exception e) {
            log.log(Level.SEVERE, "Error removing session in Dynamo Session Store", e);
        } finally {
            currentSession.remove();
            originalAttributeHash = 0;
        }
    }

    @Override
    public void remove(Session session, boolean b) {
        remove(session);
    }

    /**
     * Decide whether to skip loading/saving this request based on
     * ignoreUri and ignoreHeader regexes in configuration.
     */

    private void updateLifecycleState(LifecycleState lifecycleState) {
        synchronized (lifecycleMonitor) {
            this.lifecycleState = lifecycleState;
            lifecycleMonitor.notifyAll();
        }
    }
}
