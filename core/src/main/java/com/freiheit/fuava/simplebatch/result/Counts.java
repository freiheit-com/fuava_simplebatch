package com.freiheit.fuava.simplebatch.result;


public final class Counts {
	public static final class Builder {
		private int success;
		private int error;
		
		public Counts.Builder success() {
			success++;
			return this;
		}
		
		public Counts.Builder failed() {
			error++;
			return this;
		}
		
		public Counts build() {
			return new Counts(success, error);
		}
		
		public void count(Result<?, ?> result) {
			if (result.isFailed()) {
				failed();
			} else {
				success();
			}
		}
		

	}
	
	private final int success;
	private final int error;
	
	public Counts(int success, int error) {
		this.success = success;
		this.error = error;
	}
	
	
	public int getSuccess() {
		return success;
	}
	
	public int getError() {
		return error;
	}
	
	public static final Counts.Builder builder() {
		return new Builder();
	}
	
	
}