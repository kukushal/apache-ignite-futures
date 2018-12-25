package apache.ignite.futures.testobjects;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;

import java.util.stream.Stream;

/**
 * Ignite cluster consisting of one server and one client.
 */
public class Cluster implements AutoCloseable {
    /** Server. */
    private final Ignite srv;

    /** Client. */
    private final Ignite client;

    /**
     * Constructor: start the server and client.
     */
    public Cluster() {
        Ignite[] futs = Stream.of("ignite-server.xml", "ignite-client.xml")
            .map(Ignition::start)
            .toArray(Ignite[]::new);

        srv = futs[0];
        client = futs[1];

        assert(client.cluster().active());
    }

    /**
     * {@inheritDoc}
     */
    @Override public void close() {
        Stream.of(client, srv).forEach(Ignite::close);
    }

    /**
     * @return Ignite client connected to the cluster.
     */
    public Ignite client() {
        return client;
    }
}
