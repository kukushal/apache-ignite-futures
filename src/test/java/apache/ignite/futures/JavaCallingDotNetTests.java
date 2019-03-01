package apache.ignite.futures;

import apache.ignite.futures.testobjects.Calculator;
import apache.ignite.futures.testobjects.DotNetServer;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.lang.IgniteFuture;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Java client calls Ignite.NET service.
 */
public class JavaCallingDotNetTests {
    @Test
    public void getResultBeforeOperationCompletes() throws Exception {
        try (DotNetServer ignored = new DotNetServer(); Ignite ignite = Ignition.start("ignite-client.xml")) {
            IgniteFuture<Integer> calcFut = asyncSum(ignite, 1, 2, 2000);

            int actual = calcFut.get();

            assertEquals(1 + 2, actual);
            assertTrue(calcFut.isDone());
        }
    }

    /**
     * @return {@link IgniteFuture} from {@link Calculator#sum(int, int, int)}.
     */
    private static IgniteFuture<Integer> asyncSum(Ignite ignite, int a, int b, int duration) {
        return serviceProxy(ignite).sum(a, b, duration).setIgnite(ignite);
    }

    /**
     * @return {@link Calculator} non-sticky service proxy.
     */
    private static Calculator serviceProxy(Ignite ignite) {
        return ignite.services().serviceProxy("Calculator", Calculator.class, false);
    }
}
