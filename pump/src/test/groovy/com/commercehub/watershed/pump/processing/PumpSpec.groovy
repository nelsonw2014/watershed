package com.commercehub.watershed.pump.processing

import com.amazonaws.AmazonServiceException
import com.amazonaws.services.kinesis.producer.KinesisProducer
import com.amazonaws.services.kinesis.producer.UserRecordResult
import com.commercehub.watershed.pump.model.Job
import com.commercehub.watershed.pump.model.PumpSettings
import com.commercehub.watershed.pump.service.KinesisService
import com.google.common.util.concurrent.Futures
import rx.Observable
import rx.observers.TestSubscriber
import spock.lang.Specification

import java.nio.ByteBuffer
import java.sql.Connection
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.sql.Statement
import java.util.concurrent.ExecutionException


public class PumpSpec extends Specification {
    Job job
    Pump pump
    Connection connection
    KinesisProducer kinesisProducer
    KinesisService kinesisService
    PumpSettings pumpSettings
    TestSubscriber<UserRecordResult> testSubscriber
    Statement statement
    ResultSet resultSet
    ResultSetMetaData resultSetMetaData
    UserRecordResult userRecordResult

    String partitonKeyColumnName = "partition_key"
    String rawDataColumnName = "raw_data"

    def setup(){
        job = Mock(Job)
        connection = Mock(Connection)
        statement = Mock(Statement)
        resultSet = Mock(ResultSet)
        resultSetMetaData = Mock(ResultSetMetaData)
        userRecordResult = Mock(UserRecordResult)

        pumpSettings = Mock(PumpSettings)
        kinesisProducer = Mock(KinesisProducer)
        kinesisService = Mock(KinesisService)
        testSubscriber = new TestSubscriber<>()

        connection.createStatement(_, _) >> statement
        resultSet.getMetaData() >> resultSetMetaData

        pumpSettings.getPartitionKeyColumn() >> partitonKeyColumnName
        pumpSettings.getRawDataColumn() >> rawDataColumnName
        pumpSettings.getStreamOut() >> "stream"

        pumpSettings.getQueryIn() >> "select * from foo"
        statement.executeQuery("select * from foo") >> resultSet

        resultSet.next() >> true
        resultSet.getString(partitonKeyColumnName) >> "key"
        resultSet.getBytes(rawDataColumnName) >> "data".getBytes()


        pump = new Pump(connection, kinesisProducer, kinesisService, 5, 1).with(pumpSettings)
    }


    def "pump queries database when subscriber subscribes"(){
        setup:
        Observable<UserRecordResult> results = pump.build()

        when:
        results.subscribe(testSubscriber)
        testSubscriber.awaitTerminalEvent()

        then:
        1 * resultSet.next() >> false
        testSubscriber.assertNoErrors()
        2 * pumpSettings.getQueryIn() >> "select * from foo"
        1 * statement.executeQuery("select * from foo") >> resultSet
    }

    def "error when pump queries database"(){
        setup:
        Observable<UserRecordResult> results = pump.build()

        when:
        results.subscribe(testSubscriber)
        testSubscriber.awaitTerminalEvent()

        then:
        2 * pumpSettings.getQueryIn() >> "select * from foo"
        1 * statement.executeQuery("select * from foo") >> { throw new SQLException("sql error") }

        then:
        testSubscriber.assertNotCompleted()
        testSubscriber.assertError(SQLException.class)
        testSubscriber.assertTerminalEvent()
        testSubscriber.assertNoValues()
    }

    def "subscriber receives onNext from producer"(){
        setup:
        //subscribe for one record
        testSubscriber = TestSubscriber.create(1)
        Observable<UserRecordResult> results = pump.build()

        when:
        results.subscribe(testSubscriber)
        testSubscriber.awaitTerminalEvent()

        then:
        1 * resultSet.next() >> true

        then:
        //kinesis produces a record
        1 * kinesisProducer.addUserRecord("stream", "key", ByteBuffer.wrap("data".getBytes())) >> new Futures.ImmediateSuccessfulFuture(userRecordResult)
        1 * resultSet.next() >> false

        then:
        //subscriber is told about the record
        testSubscriber.assertNoErrors()
        testSubscriber.assertReceivedOnNext([userRecordResult])
        testSubscriber.assertValue(userRecordResult)
        testSubscriber.assertCompleted()
    }

    def "subscriber receives onErorr when producer errors"(){
        setup:
        //subscribe for one record
        testSubscriber = TestSubscriber.create(1)
        Observable<UserRecordResult> results = pump.build()

        when:
        results.subscribe(testSubscriber)
        testSubscriber.awaitTerminalEvent()

        then:
        1 * resultSet.next() >> true

        then:
        //kinesis errors on a record
        1 * kinesisProducer.addUserRecord("stream", "key", ByteBuffer.wrap("data".getBytes())) >> new Futures.ImmediateFailedFuture(new AmazonServiceException("amazon error"))
        1 * resultSet.next() >> false

        then:
        //subscriber is told about the error
        testSubscriber.assertError(ExecutionException.class) //from the Future
        testSubscriber.assertNoValues()
        testSubscriber.assertTerminalEvent()
        testSubscriber.assertNotCompleted()
    }
}
