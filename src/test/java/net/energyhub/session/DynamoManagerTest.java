package net.energyhub.session;

import java.util.logging.Logger;

public class DynamoManagerTest {
    private static Logger log = Logger.getLogger(DynamoManagerTest.class.getName());
    private static org.apache.juli.logging.Log logger = org.apache.juli.logging.LogFactory.getLog(DynamoManagerTest.class);

    private static final String PORT = "10500"; //System.getProperty("dynamodb.port");
    private DynamoManager manager;
    private int maxInterval = 60;
//
//    @Before
//    public void setUp() throws Exception {
//        Container container = mock(Container.class, withSettings().extraInterfaces(org.apache.catalina.Context.class));
//        Pipeline pipeline = mock(Pipeline.class);
//        when(pipeline.getValves()).thenReturn(new Valve[]{});
//        when(container.getPipeline()).thenReturn(pipeline);
//
//        // and now your code
//        manager = new DynamoManager();
//        manager.setAwsAccessKey("");
//        manager.setAwsSecretKey("");
//        manager.setContainer(container);
//        manager.setMaxInactiveInterval(maxInterval);
//        manager.start();
//
//        when(container.getLogger()).thenReturn(logger);
//        log.setLevel(Level.FINE);
//        Logger.getLogger(DynamoManager.class.getName()).setLevel(Level.FINE);
//    }
//
//    @After
//    public void tearDown() throws Exception {
//        this.manager.stop();
//    }
//
//    @Test
//    public void testSaveLoad() throws Exception {
//        Session session = this.manager.createSession(null);
//        log.info("Created session:" + session.getId() + "; access = " + session.getLastAccessedTime());
//
//        this.manager.save((DynamoSession)session);
//        log.info("Saved session:" + session.getId() + "; access = " + session.getLastAccessedTime());
//        String id = session.getId();
//
//        Thread.sleep(1000l);
//
//        session = this.manager.findSession(id);
//        log.info("Loaded session:" + session.getId() + "; access = " + session.getLastAccessedTime());
//        // verify that loaded session still has the last accessed time we expect
//        assertEquals(id, session.getId());
//        long lastAccessed = session.getLastAccessedTime();
//
//        // Now save again and verify that the last accessed time has changed
//        this.manager.save((DynamoSession)session);
//        log.info("Saved session (again):" + session.getId());
//        session = this.manager.findSession(id);
//        log.info("Loaded session (again):" + session.getId() + "; access = " + session.getLastAccessedTime());
//        assertEquals(id, session.getId());
//        log.info("Old access time: " + lastAccessed + ";  current access time: " + session.getLastAccessedTime());
//        assertTrue(lastAccessed < session.getLastAccessedTime()); // should be 10ms apart
//
//    }
//
//    @Test
//    public void testHaveAttributesChanged() throws Exception {
//        Map<String, Object> originalAttributes = new HashMap<>();
//        originalAttributes.put("FOO", "BAR");
//        originalAttributes.put("FOO2", "BAR2");
//
//        // start off with a session with the same attributes
//        StandardSession session = setUpSession(originalAttributes);
//        // modify the session attributes
//        session.getSession().setAttribute("FOO", "BAZ");
//        // attribute has changed
//        assertTrue(this.manager.haveAttributesChanged((DynamoSession)session));
//
//        // Get another instance of session and add new attribute
//        session = setUpSession(originalAttributes);
//        session.getSession().setAttribute("FOO3", "QUX");
//        // attributes have changed
//        assertTrue(this.manager.haveAttributesChanged((DynamoSession)session));
//
//        // Get another instance of session and remove attribute
//        session = setUpSession(originalAttributes);
//        session.getSession().removeAttribute("FOO2");
//        // attributes have changed
//        assertTrue(this.manager.haveAttributesChanged((DynamoSession)session));
//
//        // Get a new instance of this session and do not modify it
//        session = setUpSession(originalAttributes);
//        // attribute has not changed
//        assertFalse(this.manager.haveAttributesChanged((DynamoSession)session));
//
//        // Get a new instance of this session and set the same attrib
//        session = setUpSession(originalAttributes);
//        session.getSession().setAttribute("FOO", "BAR");
//        // attribute has not changed
//        assertFalse(this.manager.haveAttributesChanged((DynamoSession)session));
//    }
//
//    @Test
//    public void reaps() throws Exception {
//        Session session = manager.createSession(null);
//        log.info("Created session:" + session.getId() + "; access = " + session.getLastAccessedTime());
//        log.info("Saved session:" + session.getId() + "; access = " + session.getLastAccessedTime());
//        manager.afterRequest(); // swap out
//
//        String id = session.getId();
//        assertNotNull(manager.findSession(id)); // must load from DB
//        manager.afterRequest(); // swap out
//
//        manager.setMaxInactiveInterval(0);
//        Thread.sleep(1000l);
//        manager.backgroundProcess();
//        Thread.sleep(1000l);
//
//        Session lastSession = manager.findSession(id);
//        assertNull(lastSession); // should be deleted
//    }
//
//
//    protected StandardSession setUpSession(Map<String, Object> attributes) {
//        StandardSession originalSession = (StandardSession) this.manager.createSession(null);
//        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
//            originalSession.getSession().setAttribute(entry.getKey(), entry.getValue());
//        }
//        this.manager.setCurrentSession((DynamoSession)originalSession);
//        return originalSession;
//    }
}
