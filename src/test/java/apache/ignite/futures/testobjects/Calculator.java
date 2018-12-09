package apache.ignite.futures.testobjects;

import apache.ignite.futures.TopicMessageFuture;

/**
 * Calculator interface.
 */
public interface Calculator {
    /**
     * A very long running operation to add two integers.
     */
    TopicMessageFuture<Integer> sum(int n1, int n2);
}