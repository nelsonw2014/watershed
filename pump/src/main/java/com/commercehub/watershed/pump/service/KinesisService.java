package com.commercehub.watershed.pump.service;

public interface KinesisService {
    int countShardsInStream(String stream);
}
