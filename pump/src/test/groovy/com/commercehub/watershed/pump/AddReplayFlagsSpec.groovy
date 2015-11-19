package com.commercehub.watershed.pump

import com.commercehub.watershed.pump.processing.JobRunnable
import org.skyscreamer.jsonassert.JSONAssert
import org.skyscreamer.jsonassert.JSONCompareMode
import spock.lang.Ignore
import spock.lang.Specification


/**
 * @author pmogren
 */
@Ignore
class AddReplayFlagsSpec extends Specification {
    def "replay flags added correctly when not present"() {
        String inputJson = """{"foo": 1, "bar": "two"}"""
        String expectedOutputJson = """{"foo": 1, "bar": "two", "replay": true, "overwrite": true}"""
        when:
        String outputJson = new String(JobRunnable.addReplayFlags().apply(inputJson.getBytes("UTF-8")), "UTF-8")
        then:
        println("Output: ${outputJson}")
        JSONAssert.assertEquals(expectedOutputJson, outputJson, JSONCompareMode.STRICT)
    }

    def "replay flags set correctly when already set false at arbitrary position"() {
        String inputJson = """{"foo": 1, "replay": false, "bar": "two", "overwrite": true}""".toString()
        String expectedOutputJson = """{"foo": 1, "bar": "two", "replay": true, "overwrite": true}""".toString()
        when:
        String outputJson = new String(JobRunnable.addReplayFlags().apply(inputJson.getBytes("UTF-8")), "UTF-8")
        then:
        println("Output: ${outputJson}")
        JSONAssert.assertEquals(expectedOutputJson, outputJson, JSONCompareMode.STRICT)
    }
}