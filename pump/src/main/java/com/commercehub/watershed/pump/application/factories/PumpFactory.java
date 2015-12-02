package com.commercehub.watershed.pump.application.factories;

import com.commercehub.watershed.pump.model.PumpSettings;
import com.commercehub.watershed.pump.processing.Pump;
import com.google.common.base.Function;

public interface PumpFactory {
    Pump create(PumpSettings pumpSettings, Function<byte[], byte[]> recordTransformer);
}
