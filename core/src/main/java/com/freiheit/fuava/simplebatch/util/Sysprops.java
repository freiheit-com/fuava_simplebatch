package com.freiheit.fuava.simplebatch.util;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.google.common.collect.ImmutableList;

public class Sysprops {
    
    private static final Iterable<Prop<?>> ALL_PROPS = new ConcurrentLinkedQueue<Prop<?>>();
    
    public abstract static class Prop<T> {
        private final String key;
        
        public Prop(final String name) {
            this.key = "fdc.simplebatch." + name;
        }
        
        protected abstract T parse(String v);
        
        protected String toString(final T value) {
            return value == null ? null : value.toString();
        }
        
        public T get() {
            return parse( System.getProperty( key ) );
        }
        
        public T getOrDefault(final T defaultValue) {
            return parse( System.getProperty( key, toString( defaultValue ) ) );
        }

    }
    
    public static final class BooleanProp extends Prop<Boolean> {

        public BooleanProp( final String name ) {
            super( name );
        }

        @Override
        protected Boolean parse( final String v ) {
            return v == null ? null : "true".equals( v );
        }

        @Override
        protected String toString( final Boolean value ) {
            return value != null && value.booleanValue() ? "true" : "false";
        }
    }

    public static final class StringProp extends Prop<String> {

        public StringProp( final String name ) {
            super( name );
        }

        @Override
        protected String parse( final String v ) {
            return v;
        }

        @Override
        protected String toString( final String value ) {
            return value;
        }
    }

    public static final Prop<Boolean> ATOMIC_MOVE = new BooleanProp( "" );
    
    public static final List<Prop<?>> properties() {
        return ImmutableList.copyOf( Sysprops.ALL_PROPS );
    }
}
