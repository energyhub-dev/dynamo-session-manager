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
import com.amazonaws.services.dynamodb.AmazonDynamoDB;
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodb.model.*;
import org.apache.catalina.*;
import org.apache.catalina.connector.Request;
import org.apache.catalina.session.StandardSession;

import java.beans.PropertyChangeListener;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class DynamoManager implements Manager, Lifecycle {
    private static Logger log = Logger.getLogger(DynamoManager.class.getName());

    protected static String awsAccessKey = "";  // Required for production environment
    protected static String awsSecretKey = "";  // Required for production environment
    protected static String dynamoEndpoint = ""; // used only for QA mock dynamo connections (not production)
    protected static String tableBaseName = "tomcat-sessions";
    protected static int maxInactiveInterval = 3600; // default in seconds
    protected static String ignoreUri = "";
    protected static String ignoreHeader = "";
    protected static boolean logSessionContents = false;
    protected static int requestsPerSecond = 10; // for provisioning
    protected static int sessionSize = 1; // in kB
    protected static boolean eventualConsistency = false;
    protected static String statsdHost = "";
    protected static int statsdPort = 8125;

    public static final String COLUMN_ID = "id";
    public static final String COLUMN_LAST_ACCESSED = "lastAccessed";
    public static final String COLUMN_DATA = "data";

    protected AmazonDynamoDB dynamo;
    protected DynamoTableRotator rotator;
    private DynamoSessionTrackerValve trackerValve;
    private ThreadLocal<StandardSession> currentSession = new ThreadLocal<StandardSession>();

    // maintain hash of attributes on load so we can compare against a
    // hash when deciding to update in dynamo
    private int originalAttributeHash = 0;
    private Serializer serializer;
    private StatsdClient statsdClient = null;

    //Either 'kryo' or 'java'
    private String serializationStrategyClass = "net.energyhub.session.JavaSerializer";

    private Container container;

    private Pattern ignoreUriPattern;
    private Pattern ignoreHeaderPattern;

    /////////////////////////////////////////////////////////////////
    //   Getters and Setters for Implementation Properties
    /////////////////////////////////////////////////////////////////
    public static String getDynamoEndpoint() {
        return dynamoEndpoint;
    }

    public static void setDynamoEndpoint(String endpoint) {
        DynamoManager.dynamoEndpoint = endpoint;
    }

    public static String getTableBaseName() {
        return tableBaseName;
    }

    public static void setTableBaseName(String tableBaseName) {
        DynamoManager.tableBaseName = tableBaseName;
    }

    public static String getAwsAccessKey() {
        return awsAccessKey;
    }

    public static void setAwsAccessKey(String awsAccessKey) {
        DynamoManager.awsAccessKey = awsAccessKey;
    }

    public static String getAwsSecretKey() {
        return awsSecretKey;
    }

    public static void setAwsSecretKey(String awsSecretKey) {
        DynamoManager.awsSecretKey = awsSecretKey;
    }

    public static String getIgnoreHeader() {
        return ignoreHeader;
    }

    public static void setIgnoreHeader(String ignoreHeader) {
        DynamoManager.ignoreHeader = ignoreHeader;
    }

    public static String getIgnoreUri() {
        return ignoreUri;
    }

    public static void setIgnoreUri(String ignoreUri) {
        DynamoManager.ignoreUri = ignoreUri;
    }

    public static boolean getLogSessionContents() {
        return logSessionContents;
    }

    public static void setLogSessionContents(boolean logSessionContents) {
        DynamoManager.logSessionContents = logSessionContents;
    }

    public static int getRequestsPerSecond() {
        return requestsPerSecond;
    }

    public static void setRequestsPerSecond(int requestsPerSecond) {
        DynamoManager.requestsPerSecond = requestsPerSecond;
    }

    public static int getSessionSize() {
        return sessionSize;
    }

    public static void setSessionSize(int sessionSize) {
        DynamoManager.sessionSize = sessionSize;
    }

    public static boolean getEventualConsistency() {
        return eventualConsistency;
    }

    public static void setEventualConsistency(boolean eventualConsistency) {
        DynamoManager.eventualConsistency = eventualConsistency;
    }

    public static void setStatsdHost(String statsdHost) {
        DynamoManager.statsdHost = statsdHost;
    }
    public String getStatsdHost() {
        return statsdHost;
    }

    public static void setStatsdPort(int statsdPort) {
        DynamoManager.statsdPort = statsdPort;
    }
    public int getStatsdPort() {
        return statsdPort;
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
    public void start() throws LifecycleException {
        log.info("Starting Dynamo Session Manager in container: " + this.getContainer().getName());
        for (Valve valve : getContainer().getPipeline().getValves()) {
            if (valve instanceof DynamoSessionTrackerValve) {
                trackerValve = (DynamoSessionTrackerValve) valve;
                trackerValve.setDynamoManager(this);
                log.info("Attached to Dynamo Tracker Valve");
                break;
            }
        }
        try {
            initSerializer();
        } catch (ClassNotFoundException e) {
            log.log(Level.SEVERE, "Unable to load serializer", e);
            throw new LifecycleException(e);
        } catch (InstantiationException e) {
            log.log(Level.SEVERE, "Unable to load serializer", e);
            throw new LifecycleException(e);
        } catch (IllegalAccessException e) {
            log.log(Level.SEVERE, "Unable to load serializer", e);
            throw new LifecycleException(e);
        }
        log.info("Will expire sessions after " + getMaxInactiveInterval() + " ms");
        initDbConnection();

        if (!getIgnoreUri().isEmpty()) {
            log.info("Setting URI ignore regex to: " + getIgnoreUri());
            this.ignoreUriPattern = Pattern.compile(getIgnoreUri());
        }
        if (!getIgnoreHeader().isEmpty()) {
            log.info("Setting header ignore regex to: " + getIgnoreHeader());
            this.ignoreHeaderPattern = Pattern.compile(getIgnoreHeader());
        }
        if (!getStatsdHost().isEmpty()) {
            log.info("Configuring statsd client on " + getStatsdHost() + ":" + getStatsdPort());
            this.statsdClient = new StatsdClient(getStatsdHost(), getStatsdPort());
        }
        log.info("Finished starting manager");
    }

    public void stop() throws LifecycleException {
        getDynamo().shutdown();
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
        this.container = container;
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
    public int getSessionCounter() {
        return 10000000;
    }

    @Override
    public void setSessionCounter(int i) {

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
    public int getExpiredSessions() {
        return 0;
    }

    @Override
    public void setExpiredSessions(int i) {

    }

    @Override
    public int getRejectedSessions() {
        return 0;
    }

    @Override
    public void setRejectedSessions(int i) {
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
    public void setSessionAverageAliveTime(int i) {

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
            save(session);
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
        if (rotator != null) {
            rotator.process();
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

    /**
     * @deprecated
     */
    @Override
    public org.apache.catalina.Session createSession() {
        return createEmptySession();
    }

    @Override
    public org.apache.catalina.Session createSession(java.lang.String sessionId) {
        StandardSession session = (DynamoSession) createEmptySession();

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
        org.apache.catalina.Session[] sessions = new Session[]{};
        return sessions;
    }

    @Override
    public Session findSession(String id) throws IOException {
        return loadSession(id);
    }

    public Session loadSession(String id) throws IOException {
        if (rotator == null) {
            log.severe("Processing requests but rotator is not initialized");
            return null;
        }
        if (rotator.getCurrentTableName() == null) {
            log.severe("No table is yet set to current");
            return null;
        }

        long t0 = System.currentTimeMillis();
        if (id == null || id.length() == 0) {
            return createEmptySession();
        }

        StandardSession session = currentSession.get();

        if (session != null) {
            if (id.equals(session.getId())) {
                return session;
            } else {
                currentSession.remove();
            }
        }

        String currentTable = "", previousTable = null;

        try {
            currentTable = rotator.getCurrentTableName();
            previousTable = rotator.getPreviousTableName();
            boolean sessionFoundInPreviousTable = false;
            if (log.isLoggable(Level.FINE)) {
                log.fine("Loading session " + id + " from Dynamo, current = " + currentTable);
            }
            GetItemRequest request = new GetItemRequest()
                    .withTableName(currentTable)
                    .withKey(new Key().withHashKeyElement(new AttributeValue().withS(id)));
            // set eventual consistency or fully consistent
            request = request.withConsistentRead(!eventualConsistency);

            GetItemResult result = getDynamo().getItem(request);

            // if not found in the current table, we look in the previous table
            if (result == null || result.getItem() == null && rotator.getPreviousTableName() != null) {
                try {
                    log.fine("Falling back to previous table: " + previousTable);
                    request = request.withTableName(previousTable);
                    result = getDynamo().getItem(request);
                    sessionFoundInPreviousTable = true;
                } catch (ResourceNotFoundException e) {
                    // Occasionally, the table we call 'previous' has actually been deleted by another process
                    // In that case we are *just about* to delete it anyway, PLUS, this session is not in our
                    // current active table, it is presumably a new session request.
                    log.warning("Tried to lookup session in deleted table (presumably): " + previousTable);
                }
            }

            if (result == null || result.getItem() == null) {
                log.info("Session " + id + " not found in Dynamo");
                StandardSession ret = (DynamoSession) createEmptySession();
                ret.setId(id);
                setCurrentSession(ret);
                return ret;
            }

            ByteBuffer data = result.getItem().get(COLUMN_DATA).getB();
            Long lastAccessed = System.currentTimeMillis();
            try {
                lastAccessed = Long.parseLong(result.getItem().get(COLUMN_LAST_ACCESSED).getN());
                if (log.isLoggable(Level.FINE)) {
                    log.fine("Session " + id + "lastAccessed at " + lastAccessed);
                }
            } catch (Exception e) {
                log.warning("Couldn't read lastAccessedTime for session " + id + ", using current time");
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

            session.setMaxInactiveInterval(-1);
            session.setValid(true);
            session.setNew(false);

            if (sessionFoundInPreviousTable) {
                session.setNew(true); // force the session to be saved using PutItem
            }


            if (logSessionContents && log.isLoggable(Level.FINE)) {
                log.fine("Session Contents [" + session.getId() + "]:");
                for (Object name : Collections.list(session.getAttributeNames())) {
                    log.fine("  " + name.toString());
                }
            }

            DynamoSession dynamoSession = (DynamoSession) session;
            // Set the lastAccessedTime according to the lastAccessedTime from the dynamo record,
            // since we don't save the serialized session itself if attributes haven't changed
            dynamoSession.setLastAccessedTime(lastAccessed);

            long t1 = System.currentTimeMillis();

            if (log.isLoggable(Level.FINE)) {
                log.fine("Loaded session id " + id + " in " + (t1-t0) + "ms, "
                        + result.getConsumedCapacityUnits() + " read units");
            }
            if (statsdClient != null) {
                statsdClient.time("session.load", t0, t1);
            }
            setCurrentSession(session);
            return session;
        } catch (IOException e) {
            log.severe(e.getMessage());
            throw e;
        } catch (ResourceNotFoundException e) {
            log.severe("Unable to deserialize session (table not found) " + currentTable);
            e.printStackTrace();
            log.info("Calling backgroundProcess again");
            backgroundProcess(); // try to speed up processing
            throw e;
        } catch (ClassNotFoundException ex) {
            log.severe("Unable to deserialize session (class not found)");
            ex.printStackTrace();
            throw new IOException("Unable to deserializeInto session", ex);
        } catch (Exception e) {
            log.severe("Unexpected Error in dynamo session manager: " + e.getMessage());
            e.printStackTrace();
            throw new IOException(e);
        }
    }

    private int hashSession(StandardSession session) {
        int prime = 31;
        int hash = 0;
        List<Object> attrs = Collections.list(session.getAttributeNames());
        List<String> attrNames = new ArrayList<String>();
        for (Object name : attrs) {
            attrNames.add(name.toString());
        }
        Collections.sort(attrNames);
        for (String name : attrNames) {
            hash = prime * hash + session.getAttribute(name).hashCode();
        }
        return hash;
    }

    /**
     * Store the session and attributes at create or load time, for comparison later on.
     * @param session
     */
    protected void setCurrentSession(StandardSession session) {
        currentSession.set(session);
        originalAttributeHash = hashSession(session);
    }

    public void save(Session session) throws IOException {
        long t0 = System.currentTimeMillis();
        try {
            String currentTable = rotator.getCurrentTableName();

            if (log.isLoggable(Level.FINE)) {
                log.fine("Saving session " + session + " into Dynamo (" + currentTable + ")");
            }

            DynamoSession dynamoSession = (DynamoSession) session;

            ByteBuffer data = serializer.serializeFrom(dynamoSession);
            Map<String, AttributeValue> dbData = new HashMap<String, AttributeValue>();
            // We save lastAccessed on every access
            dbData.put(COLUMN_LAST_ACCESSED, new AttributeValue().withN(Long.toString(System.currentTimeMillis(), 10)));

            double consumedCapacity = 0.0;
            if (((DynamoSession) session).isNew()) {
                consumedCapacity = putSessionInDynamo(currentTable, dynamoSession); // new session, use PutItem
            } else {
                consumedCapacity = updateSessionInDynamo(currentTable, dynamoSession); // existing session, use UpdateItem
            }


            long t1 = System.currentTimeMillis();
            if (log.isLoggable(Level.FINE)) {
                log.fine("Updated session with id " + session.getIdInternal() + " in " + (t1 - t0) + "ms, "
                        + consumedCapacity + " write units.");
            }
            if (statsdClient != null) {
                statsdClient.time("session.save", t0, t1);
                statsdClient.timing("session.size", Math.round(consumedCapacity*1000));
            }
        } catch (IOException e) {
            log.severe(e.getMessage());
            throw e;
        } finally {
            currentSession.remove();
            originalAttributeHash = 0;
            log.fine("Session removed from ThreadLocal :" + session.getIdInternal());
        }
    }

    /**
     * Put a new session into Dynamo using PutItemRequest API
     * @param currentTable
     * @param session
     * @return
     * @throws IOException
     */
    protected double putSessionInDynamo(String currentTable, DynamoSession session) throws IOException {
        // New session, do PutItem
        if (log.isLoggable(Level.FINE)) {
            log.fine("Storing new session for " + session.getIdInternal());
        }
        Map<String, AttributeValue> dbData = new HashMap<String, AttributeValue>();

        dbData.put(COLUMN_ID, new AttributeValue().withS(session.getIdInternal()));
        dbData.put(COLUMN_LAST_ACCESSED, new AttributeValue().withN(Long.toString(System.currentTimeMillis(), 10)));
        dbData.put(COLUMN_DATA, new AttributeValue().withB(serializer.serializeFrom(session)));

        PutItemRequest putRequest = new PutItemRequest().withTableName(currentTable).withItem(dbData);
        PutItemResult result = getDynamo().putItem(putRequest);
        return result.getConsumedCapacityUnits();
    }

    /**
     * Update an existing session in Dynamo using UpdateItemRequest
     * @param currentTable
     * @param session
     * @return
     * @throws IOException
     */
    protected double updateSessionInDynamo(String currentTable, DynamoSession session) throws IOException {

        Map<String, AttributeValueUpdate> dbData = new HashMap<String, AttributeValueUpdate>();
        // Only set the session data if attributes have changed.
        boolean attributesHaveChanged = haveAttributesChanged(session);
        if (attributesHaveChanged) {
            if (log.isLoggable(Level.FINE)) {
                log.fine("Attributes have changed, saving session data for " + session.getIdInternal());
            }
            dbData.put(COLUMN_DATA, new AttributeValueUpdate()
                    .withValue(new AttributeValue().withB(serializer.serializeFrom(session)))
                    .withAction(AttributeAction.PUT));

        } else if (log.isLoggable(Level.FINE)) {
            log.fine("Attributes have not changed, saving session data for " + session.getIdInternal());

        }
        // Always update the last accessed time
        dbData.put(COLUMN_LAST_ACCESSED, new AttributeValueUpdate()
                .withValue(new AttributeValue().withN(Long.toString(System.currentTimeMillis(), 10)))
                .withAction(AttributeAction.PUT));
        UpdateItemRequest updateRequest = new UpdateItemRequest()
                .withTableName(currentTable)
                .withKey(new Key().withHashKeyElement(new AttributeValue().withS(session.getIdInternal())))
                .withAttributeUpdates(dbData);
        UpdateItemResult result = getDynamo().updateItem(updateRequest);
        return result.getConsumedCapacityUnits();
    }


    /**
     * If the original session and the current session have the same attributes (and values) then return false,
     * otherwise return true.
     * @param originalAttributes
     * @param session
     * @return
     */
    protected boolean haveAttributesChanged(StandardSession session) {
        if (logSessionContents && log.isLoggable(Level.FINE)) {
            log.fine("Session Contents [" + session.getId() + "]:");
        }
        return hashSession(session) != originalAttributeHash;
    }

    @Override
    public void remove(Session session) {
        log.fine("Removing session ID : " + session.getId());
        Key key = new Key(new AttributeValue().withS(session.getId()));
        try {
            DeleteItemRequest deleteItemRequest = new DeleteItemRequest().withTableName(rotator.getCurrentTableName()).withKey(key);
            getDynamo().deleteItem(deleteItemRequest);
            if (rotator.getPreviousTableName() != null) {
                // TODO: this is something of an issue since we have provisioned the previous table to low-write-volume
                deleteItemRequest = deleteItemRequest.withTableName(rotator.getPreviousTableName());
                getDynamo().deleteItem(deleteItemRequest);
            }

        } catch (Exception e) {
            log.log(Level.SEVERE, "Error removing session in Dynamo Session Store", e);
        } finally {
            currentSession.remove();
            originalAttributeHash = 0;
        }
    }

    protected AmazonDynamoDB getDynamo() {
        if (this.dynamo != null) {
            return this.dynamo;
        }
        if (awsAccessKey.isEmpty() && dynamoEndpoint.isEmpty()) {
            log.severe("No connection properties specified for Dynamo");
            // FIXME try to connect anyway
            return null;
        }
        this.dynamo = new AmazonDynamoDBClient(new BasicAWSCredentials(awsAccessKey, awsSecretKey));
        log.info("Connecting to Dynamo with accessKey: " + awsAccessKey);
        if (!dynamoEndpoint.isEmpty()) {
            // Using some sort of mock connection for QA/testing (see ddbmock or Alternator)
            log.info("Setting dynamo endpoint: " + dynamoEndpoint);
            this.dynamo.setEndpoint(dynamoEndpoint);
        }
        return this.dynamo;
    }

    private void initDbConnection() throws LifecycleException {
        long nowSeconds = System.currentTimeMillis() / 1000;
        try {
            getDynamo();
            this.rotator = new DynamoTableRotator(getTableBaseName(), getMaxInactiveInterval(), getRequestsPerSecond(),
                    getSessionSize(), getEventualConsistency(), getDynamo());
            rotator.init(nowSeconds); // set current table, will wait for a table to come online if we need to create
                                      // a new one.

            log.info("Connected to Dynamo for session storage. Session live time = "
                    + (getMaxInactiveInterval()) + "s");
        } catch (Exception e) {
            throw new LifecycleException("Error Connecting to Dynamo", e);
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

    /**
     * Decide whether to skip loading/saving this request based on
     * ignoreUri and ignoreHeader regexes in configuration.
     */
    protected boolean isIgnorable(Request request) {
        if (this.ignoreUriPattern != null) {
            if (ignoreUriPattern.matcher(request.getRequestURI()).matches()) {
                if (log.isLoggable(Level.FINE)) {
                    log.fine("Session manager will ignore this session based on uri: " + request.getRequestURI());
                }
                return true;
            }
        }
        if (this.ignoreHeaderPattern != null) {
            for (Enumeration headers = request.getHeaderNames(); headers.hasMoreElements(); ) {
                String header = headers.nextElement().toString();
                if (ignoreHeaderPattern.matcher(header).matches()) {
                    if (log.isLoggable(Level.FINE)) {
                        log.fine("Session manager will ignore this session based on header: " + header);
                    }
                    return true;
                }

            }
        }
        return false;
    }

}
