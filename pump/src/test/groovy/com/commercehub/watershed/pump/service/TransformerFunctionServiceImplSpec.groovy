package com.commercehub.watershed.pump.service

import com.commercehub.watershed.pump.application.GuiceBridge
import com.commercehub.watershed.pump.application.PumpGuiceModule
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.inject.Guice
import org.skyscreamer.jsonassert.JSONAssert
import org.skyscreamer.jsonassert.JSONCompareMode
import spock.lang.Specification

class TransformerFunctionServiceImplSpec extends Specification {

    TransformerFunctionService transformerFunctionServiceService

    def setup() {
        GuiceBridge.setOverrideInjector(Guice.createInjector(new PumpGuiceModule()))
        ObjectMapper objectMapper = GuiceBridge.getInjector().getInstance(ObjectMapper)

        transformerFunctionServiceService = new TransformerFunctionServiceImpl(objectMapper: objectMapper)
    }

    def "replay flags added correctly when not present"(Boolean replayEnabled, Boolean overwriteEnabled) {
        setup:
        String inputJson = """{"foo": 1, "bar": "two"}"""
        String expectedOutputJson = String.format("""{"foo": 1, "bar": "two", "replay": %b, "overwrite": %b}""", replayEnabled, overwriteEnabled)

        when:
        String outputJson = new String(transformerFunctionServiceService.addReplayFlags(replayEnabled, overwriteEnabled).apply(inputJson.getBytes("UTF-8")), "UTF-8")

        then:
        JSONAssert.assertEquals(expectedOutputJson, outputJson, JSONCompareMode.STRICT)

        where:
        replayEnabled | overwriteEnabled
        true          | true
        false         | true
        true          | false
        false         | false
    }

    def "replay flags set correctly when already set false at arbitrary position"(Boolean replayEnabled, Boolean overwriteEnabled) {
        setup:
        String inputJson = """{"foo": 1, "replay": false, "bar": "two", "overwrite": true}""".toString()
        String expectedOutputJson = String.format("""{"foo": 1, "bar": "two", "replay": %b, "overwrite": %b}""", replayEnabled, overwriteEnabled)

        when:
        String outputJson = new String(transformerFunctionServiceService.addReplayFlags(replayEnabled, overwriteEnabled).apply(inputJson.getBytes("UTF-8")), "UTF-8")

        then:
        JSONAssert.assertEquals(expectedOutputJson, outputJson, JSONCompareMode.STRICT)

        where:
        replayEnabled | overwriteEnabled
        true          | true
        false         | true
        true          | false
        false         | false
    }
}
