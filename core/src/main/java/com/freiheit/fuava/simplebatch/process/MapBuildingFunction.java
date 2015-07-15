package com.freiheit.fuava.simplebatch.process;

import java.util.List;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;

public class MapBuildingFunction<Input, Output> implements Function<List<Input>, Map<Input, Output>> {
	private final Function<List<Input>, List<Output>> delegee;

	/**
	 * The Function must produce on output item for each input item and keep the order.
	 * Additionally, it must only be used for data where each input item is distinct and 
	 * no duplicates exist within one input.
	 * 
	 * @param delegee
	 */
	public MapBuildingFunction(Function<List<Input>, List<Output>> delegee) {
		this.delegee = delegee;
	}
	
	@Override
	public Map<Input, Output> apply(List<Input> input) {
		List<Output> output = delegee.apply(input);
		if (input.size() != output.size()) {
			throw new IllegalStateException("result size must match input size. Got " + output.size() + " outputs for " + input.size() + " inputs");
		}
		// Note that it is intentional to throw an exception if there the input instances are not unique
		ImmutableMap.Builder<Input, Output> b = ImmutableMap.builder();
		for (int i = 0; i < input.size(); i++) {
			Output out = output.get(i);
			Input in = input.get(i);
			b.put(in, out);
		}
		return b.build();
	}
	
}