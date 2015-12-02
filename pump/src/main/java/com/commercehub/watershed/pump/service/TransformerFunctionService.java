package com.commercehub.watershed.pump.service;

import com.google.common.base.Function;

public interface TransformerFunctionService {
    Function<byte[], byte[]> getReplayFlagTransformFunction(Boolean replayEnabled, Boolean overwriteEnabled);
}
