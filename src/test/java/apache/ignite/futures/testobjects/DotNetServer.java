package apache.ignite.futures.testobjects;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Ignite.NET Server Node with .NET calculator service deployed.
 */
public class DotNetServer implements AutoCloseable {
    /** Ignite output after successful start. */
    private static final String SUCCESS_OUTPUT = "Ignite node started OK";

    /** Process. */
    private Process proc;

    /** Process output reader. */
    private BufferedReader procReader;

    /**
     * Constructor: starts .NET server node.
     */
    public DotNetServer() throws Exception {
        Path dir = Paths.get("Apache.Ignite.Futures.Tests", "bin", "Debug").toAbsolutePath();
        Path path = Paths.get(dir.toString(), "Apache.Ignite.Futures.Tests.exe");

        if (!Files.exists(path))
            throw new Exception("Ignite.NET server executable does not exist: " + path);

        proc = new ProcessBuilder(path.toString())
            .directory(new File(dir.toString()))
            .redirectError(ProcessBuilder.Redirect.INHERIT)
            .redirectOutput(ProcessBuilder.Redirect.INHERIT)
            .start();

        procReader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
        StringBuilder output = new StringBuilder();
        String line;

        while ((line = procReader.readLine()) != null &&
            !line.contains(SUCCESS_OUTPUT) &&
            output.lastIndexOf(SUCCESS_OUTPUT) < 0)
            output.append(line);
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        if (proc != null) {
            try (PrintWriter procWriter = new PrintWriter(proc.getOutputStream())) {
                procWriter.println();
            }

            procReader.close();
        }
    }
}
