package com.commercehub.watershed.pump

import org.skyscreamer.jsonassert.JSONAssert
import org.skyscreamer.jsonassert.JSONCompareMode
import spock.lang.Specification


/**
 * @author pmogren
 */
class AddReplayFlagsSpec extends Specification {
    def "replay flags added correctly when not present"() {
        String inputJson = """{"foo": 1, "bar": "two"}""".toString()
        String expectedOutputJson = """{"foo": 1, "bar": "two", "replay": true, "overwrite": true}""".toString()
        when:
        String outputJson = new String(WatershedPumpMain.addReplayFlags().apply(inputJson.getBytes("UTF-8")), "UTF-8")
        then:
        println("Output: ${outputJson}")
        JSONAssert.assertEquals(expectedOutputJson, outputJson, JSONCompareMode.STRICT)
    }

    def "replay flags set correctly when already set false at arbitrary position"() {
        String inputJson = """{"foo": 1, "replay": false, "bar": "two", "overwrite": true}""".toString()
        String expectedOutputJson = """{"foo": 1, "bar": "two", "replay": true, "overwrite": true}""".toString()
        when:
        String outputJson = new String(WatershedPumpMain.addReplayFlags().apply(inputJson.getBytes("UTF-8")), "UTF-8")
        then:
        println("Output: ${outputJson}")
        JSONAssert.assertEquals(expectedOutputJson, outputJson, JSONCompareMode.STRICT)
    }
}