package com.freiheit.fuava.simplebatch.util;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

import subdirs.StandardSubdirStrategies;
import subdirs.SubdirStrategy;

public class Sysprops {
    
    private static final ConcurrentLinkedQueue<Prop<?>> ALL_PROPS = new ConcurrentLinkedQueue<Prop<?>>();
    
    public abstract static class Prop<T> {
        private final String key;
        
        public Prop(final String name) {
            this.key = "fdc.simplebatch." + name;
            ALL_PROPS.add(this);
        }
        
        public String getKey() {
            return this.key;
        }
        
        public abstract String getDefaultValueString();
        
        public abstract List<String> getPossibleStringValues();
        
        protected T getOrDefault( final Function<String, T> parse, final T defaultValue ) {
            final String v = System.getProperty( key );
            return v == null ? defaultValue : parse.apply( v );
        }

    }
    
    public static final class BooleanProp extends Prop<Boolean> {

        private final boolean defaultValue;

        public BooleanProp( final String name, final boolean defaultValue ) {
            super( name );
            this.defaultValue = defaultValue;
        }

        @Override
        public List<String> getPossibleStringValues() {
            return ImmutableList.of( "true", "false" );
        }
        
        @Override
        public String getDefaultValueString() {
            return Boolean.toString( defaultValue );
        }
        
        public boolean is() {
            return getOrDefault( v -> "true".equals( v ), defaultValue );
        }
    }

    public static final class StringProp extends Prop<String> {

        private final String defaultValue;

        public StringProp( final String name, final String defaultValue ) {
            super( name );
            this.defaultValue = defaultValue;
        }
        
        @Override
        public List<String> getPossibleStringValues() {
            return ImmutableList.of("<any>");
        }
        
        @Override
        public String getDefaultValueString() {
            return defaultValue;
        }
        
        public String get() {
            return getOrDefault( v -> v, defaultValue );
        }
    }

    public static final class SubdirStrategyProp extends Prop<SubdirStrategy> {

        private final StandardSubdirStrategies defaultValue;

        public SubdirStrategyProp( final String name, final StandardSubdirStrategies defaultValue ) {
            super( name );
            this.defaultValue = defaultValue;
        }
        
        @Override
        public String getDefaultValueString() {
            return defaultValue.name();
        }
        
        @Override
        public List<String> getPossibleStringValues() {
            return FluentIterable.of( StandardSubdirStrategies.values() ).transform( e -> e.name() ).toList();
        }
        
        public SubdirStrategy get() {
            return getOrDefault( v -> StandardSubdirStrategies.getInstance( v ), defaultValue );
        }
    }

    public static final boolean ATOMIC_MOVE = new BooleanProp( "fsjobs.atomicmove", false ).is();
    public static final boolean FILE_PROCESSING_PARALLEL = new BooleanProp( "fsjobs.files.parallel", false ).is();
    public static final boolean CONTENT_PROCESSING_PARALLEL = new BooleanProp( "fsjobs.content.parallel", false ).is();
    public static final String INSTANCE_NAME = new StringProp( "fsjobs.instance", "inst_01" ).get();
    public static final SubdirStrategy SUBDIR_STRATEGY = new SubdirStrategyProp( "fsjobs.files.subdirstrategy", StandardSubdirStrategies.MD5_ONE_LETTER_TWO_DIRS ).get();
    public static final SubdirStrategy SFTP_SUBDIR_STRATEGY = new SubdirStrategyProp( "sftp.files.subdirstrategy", StandardSubdirStrategies.NONE ).get();
    
    private static final Logger LOG = LoggerFactory.getLogger( Sysprops.class );
    
    static {
        if ( LOG.isInfoEnabled() ) {
            final StringBuilder sb = new StringBuilder();
            sb.append( "Simplebatch Java Properties. Use -Dpropname=propvalue to control the parameter" ).append( '\n' );
            for (final Prop<?> p: properties()) {
                sb
                .append( '\n' )
                .append( "# Possible Values: " ).append( Joiner.on( ", " ).join( p.getPossibleStringValues() ) ).append( '\n' )
                .append( "-D").append( p.getKey() ).append( "=" )
                .append( p.getDefaultValueString() ).append( '\n' );
            }
            LOG.info( sb.toString() );
        }
    }
    static final List<Prop<?>> properties() {
        return ImmutableList.copyOf( Sysprops.ALL_PROPS );
    }
}
