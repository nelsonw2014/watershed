package com.commercehub.watershed.pump.service;

import com.google.common.base.Function;

public interface RecordTransformerService {
    Function<byte[], byte[]> addReplayFlags(Boolean replayEnabled, Boolean overwriteEnabled);
}
