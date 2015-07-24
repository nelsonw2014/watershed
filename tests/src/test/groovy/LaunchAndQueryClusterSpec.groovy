import groovy.sql.Sql
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spock.lang.Specification

import java.util.concurrent.TimeUnit


/**
 * Requires the 'aws' command to be on the system PATH.
 *
 * @author pmogren
 */
class LaunchAndQueryClusterSpec extends Specification {
    Logger logger = LoggerFactory.getLogger("LaunchAndQueryClusterSpec")
    String clusterId

    def "launch cluster and issue queries"() {
        given: "Test configuration"
        String launcherHome = System.getProperty("launcherHome")
        if (!launcherHome || !new File(launcherHome).isDirectory()) {
            throw new IllegalArgumentException("You must set the system property launcherHome to a git repo containing commercehub-oss/watershed. It was: ${launcherHome}")
        }

        String launcherConfig = System.getProperty("launcherConfig")
        if (!launcherConfig || !new File(launcherConfig).isFile()) {
            throw new IllegalArgumentException("You must set the system property launcherConfig to a configuration file for commercehub-oss/watershed. It was: ${launcherConfig}")
        }

        String privateKeyFile = System.getProperty("privateKeyFile")
        if (!privateKeyFile || !new File(privateKeyFile).isFile()) {
            throw new IllegalArgumentException("You must set the system property privateKeyFile to the local file containing the private key for the SSH key pair used by the EC2 nodes in the cluster.")
        }

        when: "Uploading resources"
        Integer exitCode = new Shell().execute(System.out, 1, TimeUnit.MINUTES,
            "${launcherHome}/upload-resources", "${launcherConfig}")

        then: "Resources uploaded"
        exitCode == 0

        when: "Launching a cluster"
        def capturedOutputStream = new ByteArrayOutputStream()
        exitCode = new Shell().execute(new PrintStream(capturedOutputStream), 1, TimeUnit.MINUTES,
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

        then:
        noExceptionThrown()

        then: "Port forwarding is active"
        Thread.sleep(1000)
        forwardingProcess.alive

        when: "Test socket connectivity on Drill user port"
        def maxTries = 60
        def socket
        for (i in 1..maxTries) {
            try {
                socket = new Socket("localhost", 31010)
            } catch (ConnectException ce) {
                if (i == maxTries) {
                    throw new IllegalStateException("Repeatedly failed to open a socket to Drill", ce)
                }
                Thread.sleep(1000)
            }
        }

        then:
        noExceptionThrown()

        then:
        socket.connected

        when: "Connect to database and query a built-in resource"
        def db = [url: "jdbc:drill:drillbit=localhost:31010", user: "admin", password: "admin", driver: "org.apache.drill.jdbc.Driver"]
        def sql = Sql.newInstance(db.url, db.user, db.password, db.driver)
        def rs = sql.rows("SELECT COUNT(employee_id) cnt FROM cp.`employee.json`")

        then:
        noExceptionThrown()

        then: "Query result is correct"
        rs.size() == 1
        rs[0].getProperty("cnt") == 1155

        cleanup:
        logger.info("Cleaning up.")
        if (forwardingProcess?.alive) {
            logger.info("Killing port-forwarding process.")
            forwardingProcess.destroy()
        }
        if (clusterId) {
            logger.info("Terminating cluster ${clusterId}.")
            new Shell().execute(System.out, 1, TimeUnit.MINUTES,
                    "${launcherHome}/terminate-clusters", "${clusterId}")
        }
        if (socket?.connected) {
            socket.close()
        }
    }

}
