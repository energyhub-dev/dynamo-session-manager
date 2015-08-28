package net.energyhub.session;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import org.apache.catalina.Container;
import org.apache.catalina.Context;
import org.apache.catalina.Pipeline;
import org.apache.catalina.Valve;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;


/** Test dynamo manager to inject alternator
 */
public class TestManager extends DynamoManager {
    private static Log logger = LogFactory.getLog(TestManager.class);
    private AmazonDynamoDB dbClient;

    public TestManager(AmazonDynamoDB dbClient) {
        this.dbClient = dbClient;
    }

    @Override
    protected AmazonDynamoDB initDynamoClient() {
        return dbClient;
    }

    /*
    Stub and mock enough to make the container work in tests
     */
    @Override
    public Container getContainer() {
        Container container = mock(Container.class, withSettings().extraInterfaces(Context.class));
        Pipeline pipeline = mock(Pipeline.class);
        when(pipeline.getValves()).thenReturn(new Valve[]{});
        when(container.getPipeline()).thenReturn(pipeline);

        when(container.getLogger()).thenReturn(logger);
        return container;
    }

}
