package apache.ignite.futures.testobjects;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;

import java.util.Arrays;

/**
 * Ignite cluster consisting of one server and one client.
 */
public class Cluster implements AutoCloseable {
    private final Ignite server;
    private final Ignite client;

    /**
     * Constructor: start the server and client.
     */
    public Cluster() {
        Ignite[] futs = Arrays.asList("ignite-server.xml", "ignite-client.xml")
            .parallelStream()
            .map(Ignition::start)
            .toArray(Ignite[]::new);

        server = futs[0];
        client = futs[1];
    }

    /**
     * {@inheritDoc}
     */
    @Override public void close() {
        Arrays.asList(client, server).parallelStream().forEach(Ignite::close);
    }

    /**
     * @return Ignite client connected to the cluster.
     */
    public Ignite client() {
        return client;
    }
}
