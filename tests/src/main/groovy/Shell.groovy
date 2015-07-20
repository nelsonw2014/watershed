import java.util.concurrent.TimeUnit

/**
 * @author pmogren
 */
class Shell {
    /**
     * Runs a process shell-style, pipes its output, and terminates it after a timeout.
     *
     * @param mixedOutputWriter A sink for both stdout and stderr.
     * @param timeout How long to wait for the command to terminate, in {@code timeoutUnit}.
     * @param timeoutUnit Unit of {@code timeout}.
     * @param command An array representing the command line to execute.
     *
     * @return process exit code, or {@code null} if the process did not terminate within the timeout.
     */
    Integer execute(PrintStream output, int timeout, TimeUnit timeoutUnit, String... command) {
        println "Running command ${Arrays.asList(command)}"
        def process = new ProcessBuilder(command).redirectErrorStream(true).start()
        process.inputStream.eachLine { output.println(it) }
        boolean terminated = process.waitFor(timeout, timeoutUnit)
        if (!terminated) {
            process.destroy()
        }
        return terminated ? process.exitValue() : null
    }

    Process executeInBackground(PrintStream output, String... command) {
        println "Running command ${Arrays.asList(command)}"
        def process = new ProcessBuilder(command).redirectErrorStream(true).start()
        new Thread({ process.inputStream.eachLine { output.println(it) } }, "Process output tailer").start()
        return process
    }

}
