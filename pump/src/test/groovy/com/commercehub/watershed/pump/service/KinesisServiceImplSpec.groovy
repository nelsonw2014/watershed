package com.commercehub.watershed.pump.service

import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model.DescribeStreamResult
import com.amazonaws.services.kinesis.model.Shard
import com.amazonaws.services.kinesis.model.StreamDescription
import spock.lang.Specification

class KinesisServiceImplSpec extends Specification{
    AmazonKinesisClient kinesisClient
    StreamDescription streamDescription
    KinesisService kinesisService
    DescribeStreamResult describeStreamResult

    def setup(){
        kinesisClient = Mock(AmazonKinesisClient)
        streamDescription = Mock(StreamDescription)
        describeStreamResult = Mock(DescribeStreamResult)
        describeStreamResult.getStreamDescription() >> streamDescription
        kinesisService = new KinesisServiceImpl(kinesisClient: kinesisClient)
    }

    def "countShardsInStream gets initial shard count"(){
        when:
        int numShards = kinesisService.countShardsInStream("my_stream")

        then:
        1 * kinesisClient.describeStream("my_stream") >> describeStreamResult
        1 * streamDescription.isHasMoreShards() >> false
        1 * streamDescription.getShards() >> [Mock(Shard), Mock(Shard)]
        numShards == 2
    }

    def "countShardsInStream gets additional shard counts"(){
        setup:
        List<Shard> shards = [Mock(Shard), Mock(Shard)]
        shards.get(1).getShardId() >> "my_shard_id"

        when:
        int numShards = kinesisService.countShardsInStream("my_stream")

        then:
        //make a call to get shards
        1 * kinesisClient.describeStream("my_stream") >> describeStreamResult
        1 * streamDescription.getShards() >> shards
        1 * streamDescription.isHasMoreShards() >> true

        then:
        //make another call to get a describeStreamResult with shardId of last in list
        1 * streamDescription.getShards() >> shards
        1 * kinesisClient.describeStream("my_stream", "my_shard_id") >> describeStreamResult

        then:
        //get shards again
        1 * streamDescription.getShards() >> [Mock(Shard)]
        1 * streamDescription.isHasMoreShards() >> false
        numShards == 3
    }
}
