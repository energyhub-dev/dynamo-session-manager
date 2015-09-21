package net.energyhub.session;

import static org.junit.Assert.*;

import com.amazonaws.services.dynamodb.model.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.dynamodb.AmazonDynamoDB;
import com.michelboudreau.alternator.AlternatorDB;
import com.michelboudreau.alternator.AlternatorDBClient;

import java.lang.String;
import java.util.List;

/**
 * User: oneill
 * Date: 3/22/13
 */
public class DynamoTableRotatorTest {
    private DynamoTableRotator rotator;
    private AmazonDynamoDB dynamo;
    private AlternatorDB db;
    private int maxInterval = 180;
    private long defaultReadCapacity = 20;
    private long defaultWriteCapacity = 5;
//
//    @Before
//    public void setUp() throws Exception {
//        this.dynamo = new AlternatorDBClient();
//        this.db = new AlternatorDB().start();
//        this.rotator = new DynamoTableRotator("testTables", maxInterval, defaultReadCapacity, defaultWriteCapacity, dynamo);
//    }
//
//    @After
//    public void tearDown() throws Exception {
//        this.db.stop();
//    }
//
//    @Test
//    public void testRotationRequired() throws Exception {
//        long startSeconds = (System.currentTimeMillis() / 1000) / maxInterval*maxInterval;
//        String staleName = rotator.createCurrentTableName(startSeconds -maxInterval - 1);
//        rotator.currentTableName = staleName;
//
//        // same time, table doesn't need to change
//        assertFalse(rotator.rotationRequired(startSeconds -maxInterval - 1));
//        // later, table does need to rotate
//        assertTrue(rotator.rotationRequired(startSeconds + maxInterval + 1));
//
//    }
//
//    @Test
//    public void testNextCurrentTableNames() {
//        long startSeconds = (System.currentTimeMillis() / 1000) / maxInterval * maxInterval;
//
//        String currentName = rotator.createCurrentTableName(startSeconds);
//        String previousName = rotator.createPreviousTableName(startSeconds);
//        assertEquals(currentName, rotator.createCurrentTableName(startSeconds + 1));
//        assertEquals(previousName, rotator.createPreviousTableName(startSeconds + 1));
//        assertFalse(currentName.equals(rotator.createCurrentTableName(startSeconds + maxInterval)));
//        assertFalse(previousName.equals(rotator.createPreviousTableName(startSeconds + maxInterval)));
//    }
//
//    @Test
//    public void testNextTableRequired() {
//        long startSeconds = System.currentTimeMillis()/1000;
//        long lastTableTime = startSeconds - startSeconds % maxInterval;
//        String currentTableName = rotator.createCurrentTableName(startSeconds);
//        rotator.currentTableName = currentTableName;
//
//        assertFalse(rotator.createTableRequired(lastTableTime + 1));
//        assertTrue(rotator.createTableRequired(lastTableTime + maxInterval - 1));
//
//    }
//
//    @Test
//    public void rotation() {
//        long startSeconds = 0;
//
//        List<String> tables = dynamo.listTables().getTableNames();
//        assertTrue(tables.isEmpty());
//
//        // just starting, create current
//        assertTrue(rotator.rotationRequired(startSeconds));
//        rotator.rotateTables(startSeconds);
//        tables = dynamo.listTables().getTableNames();
//        assertEquals(1, tables.size());
//
//        // table created with default throughput capacity
//        TableDescription firstTable = rotator.getTable(rotator.getCurrentTableName());
//        assertNotNull(firstTable);
//        assertEquals(defaultReadCapacity, firstTable.getProvisionedThroughput().getReadCapacityUnits().longValue());
//        assertEquals(defaultWriteCapacity, firstTable.getProvisionedThroughput().getWriteCapacityUnits().longValue());
//        assertNull(rotator.getPreviousTableName());
//
//        // just current
//        rotator.rotateTables(startSeconds + maxInterval  - 1);
//        tables = dynamo.listTables().getTableNames();
//        assertEquals(1, tables.size());
//        assertEquals(firstTable.getTableName(), rotator.getCurrentTableName());
//
//        // scale throughput capacity
//        UpdateTableRequest update = new UpdateTableRequest()
//                .withTableName(firstTable.getTableName())
//                .withProvisionedThroughput(new ProvisionedThroughput()
//                .withReadCapacityUnits(45l)
//                .withWriteCapacityUnits(15l));
//        dynamo.updateTable(update);
//
//        // previous + current
//        rotator.rotateTables(startSeconds + maxInterval + 1);
//        tables = dynamo.listTables().getTableNames();
//        assertEquals(2, tables.size());
//        assertEquals(firstTable.getTableName(), rotator.getPreviousTableName());
//
//        // table created with previous table's capacity
//        TableDescription secondTable = rotator.getTable(rotator.getCurrentTableName());
//        assertNotNull(secondTable);
//        assertEquals(45l, secondTable.getProvisionedThroughput().getReadCapacityUnits().longValue());
//        assertEquals(15l, secondTable.getProvisionedThroughput().getWriteCapacityUnits().longValue());
//
//        // previous + current
//        rotator.rotateTables(startSeconds + 120);
//        tables = dynamo.listTables().getTableNames();
//        assertEquals(2, tables.size());
//    }
//
//    @Test
//    public void createTable() throws Exception {
//        String testTableName = "test_table_" + System.currentTimeMillis();
//        rotator.ensureTable(testTableName, 10000);
//        assertTrue(dynamo.listTables().getTableNames().contains(testTableName));
//    }
//
//
//    @Test
//    public void ensureTableMakesWrite() throws Exception {
//        String testTableName = "test_table_" + System.currentTimeMillis();
//        rotator.ensureTable(testTableName, 10000);
//
//        GetItemRequest request = new GetItemRequest()
//                .withTableName(testTableName)
//                .withKey(new Key().withHashKeyElement(new AttributeValue().withS("test_id")));
//        // set eventual consistency or fully consistent
//        request = request.withConsistentRead(true);
//
//        GetItemResult result = dynamo.getItem(request);
//        assertNotNull(result);
//        assertEquals("test_id", result.getItem().get(DynamoManager.COLUMN_ID).getS());
//        assertEquals("test", result.getItem().get(DynamoManager.COLUMN_DATA).getS());
//    }
//
//    @Test
//    public void isActive() throws Exception {
//        // bit of a circular test!
//        String testTableName = rotator.createCurrentTableName(System.currentTimeMillis()/1000);
//        rotator.ensureTable(testTableName, 10000);
//        assertTrue(rotator.isActive(testTableName));
//    }
//
//    @Test
//    public void isWritable() throws Exception {
//        // bit of a circular test!
//        String testTableName = rotator.createCurrentTableName(System.currentTimeMillis()/1000);
//        rotator.ensureTable(testTableName, 10000);
//        assertTrue(rotator.isWritable(testTableName));
//    }
//
//    @Test
//    public void init_previousExists() throws Exception {
//        // Check that we pick up an active table that is from a previous time period
//        long nowSeconds = System.currentTimeMillis()/1000;
//        long twoTablesAgo = nowSeconds - 2*rotator.tableRotationSeconds;
//
//        String oldTableName = rotator.createCurrentTableName(twoTablesAgo);
//        rotator.ensureTable(oldTableName, 10000);
//
//        rotator.init(nowSeconds);
//        assertEquals(oldTableName, rotator.currentTableName);
//    }
//
//    @Test
//    public void init_currentExists() throws Exception {
//        // Check that we pick up an active table that is from the current time period
//        long nowSeconds = System.currentTimeMillis()/1000;
//
//        String tableName = rotator.createCurrentTableName(nowSeconds);
//        rotator.ensureTable(tableName, 10000);
//
//        rotator.init(nowSeconds);
//        assertEquals(tableName, rotator.currentTableName);
//    }
//
//    @Test
//    public void init_noneExists() throws Exception {
//        // Check that we create and wait for a new table if none exists
//        long nowSeconds = System.currentTimeMillis()/1000;
//
//        String tableName = rotator.createCurrentTableName(nowSeconds);
//
//        rotator.init(nowSeconds);
//        assertEquals(tableName, rotator.currentTableName);
//    }
//
//    @Test
//    public void isMyTable() {
//        long nowSeconds = System.currentTimeMillis()/1000;
//
//        String tableName = rotator.createCurrentTableName(nowSeconds);
//        String fakeTableName = "some-other-base_" + rotator.dateFormat.format(nowSeconds);
//        String almostTableName = rotator.tableBaseName + "-extra_" + rotator.dateFormat.format(nowSeconds);
//
//        assertTrue(rotator.isMyTable(tableName));
//        assertFalse(rotator.isMyTable(fakeTableName));
//        assertFalse(rotator.isMyTable(almostTableName));
//
//    }
}
