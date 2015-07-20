import spock.lang.Specification

import java.util.concurrent.TimeUnit


/**
 * Requires the 'aws' command to be on the system PATH.
 *
 * @author pmogren
 */
class LaunchAndQueryClusterSpec extends Specification {
    String clusterId

    def "launch cluster and issue queries"() {
        //TODO upload-resources or assert it has been done
        //TODO stream and archives need data, can that be generated now?
        //TODO generate config file that won't conflict with anything in actual use? Meh, we need to be able to test a real config; can create a dummy one as a special case or in a harness.
        //TODO in order to run query, need SSH proxy
        //TODO Doesn't this really belong in OSS? Let user configure queries?

        given: "Test configuration"
        String launcherHome = System.getProperty("launcherHome")
        if (!launcherHome || !new File(launcherHome).isDirectory()) {
            throw new IllegalArgumentException("You must set the system property launcherHome to a git repo containing commercehub-oss/flux-capacitor. It was: ${launcherHome}")
        }

        String launcherConfig = System.getProperty("launcherConfig")
        if (!launcherConfig || !new File(launcherConfig).isFile()) {
            throw new IllegalArgumentException("You must set the system property launcherConfig to a configuration file for commercehub-oss/flux-capacitor. It was: ${launcherConfig}")
        }

        String privateKeyFile = System.getProperty("privateKeyFile")
        if (!privateKeyFile || !new File(privateKeyFile).isFile()) {
            throw new IllegalArgumentException("You must set the system property privateKeyFile to the local file containing the private key for the SSH key pair used by the EC2 nodes in the cluster.")
        }

        when: "Launching a cluster"
        def capturedOutputStream = new ByteArrayOutputStream()
        Integer exitCode = new Shell().execute(new PrintStream(capturedOutputStream), 1, TimeUnit.MINUTES,
                "${launcherHome}/launch-cluster", "${launcherConfig}")
        String capturedOutput = capturedOutputStream.toString()
        System.out.print(capturedOutput)
        if (capturedOutput.matches(/j-\w+\r?\n/)) {
            this.clusterId = capturedOutput.split("\r?\n")[0]
        }

        then: "Cluster launched"
        exitCode == 0
        clusterId

        when: "Waiting for cluster to be ready"
        exitCode = new Shell().execute(System.out, 20, TimeUnit.MINUTES,
                "${launcherHome}/wait-until-ready", "${clusterId}")

        then: "Cluster is ready"
        exitCode == 0

        when: "Forward local ports to cluster with SSH"
        Process forwardingProcess = new Shell().executeInBackground(System.out,
                "${launcherHome}/forward-local-ports", "${clusterId}", "${privateKeyFile}", "-T",
                "-o", "StrictHostKeyChecking=no", "-o", "BatchMode=yes")

        then: "Port forwarding is active"
        Thread.sleep(1000)
        forwardingProcess.alive
        Socket socket = new Socket((String) null, 31010)
        socket.connected
        socket.close()

/*
        when:
        //TODO configure a stream archive storage plugin?
        //TODO issue stream archive query

        then:
        //TODO assert stream archive query success

        when:
        //TODO configure a stream storage plugin?
        //TODO issue stream query

        then:
        //TODO assert stream query success

        when:
        //TODO issue unified-view query

        then:
        //TODO assert unified-view query success
*/

        cleanup:
        if (forwardingProcess?.alive) {
            println "Killing port-forwarding process."
            forwardingProcess.destroy()
        }
        if (clusterId) {
            println "Terminating cluster."
            new Shell().execute(System.out, 1, TimeUnit.MINUTES,
                    "${launcherHome}/terminate-clusters", "${clusterId}")
        }
        if (socket?.connected) {
            socket.close()
        }
    }

}