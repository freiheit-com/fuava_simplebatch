package com.freiheit.fuava.simplebatch.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import subdirs.StandardSubdirStrategies;
import subdirs.SubdirStrategy;

public class Sysprops {
    private static final ConcurrentLinkedQueue<Prop<?>> ALL_PROPS = new ConcurrentLinkedQueue<>();
    
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
            return Arrays.asList( "true", "false" );
        }
        
        @Override
        public String getDefaultValueString() {
            return Boolean.toString( defaultValue );
        }
        
        public boolean is() {
            return getOrDefault( "true"::equals, defaultValue );
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
            return Collections.singletonList( "<any>" );
        }
        
        @Override
        public String getDefaultValueString() {
            return defaultValue;
        }
        
        public String get() {
            return getOrDefault( v -> v, defaultValue );
        }
    }
    public static final class IntegerProp extends Prop<Integer> {
        
        private final Integer defaultValue;
        
        public IntegerProp( final String name, final Integer defaultValue ) {
            super( name );
            this.defaultValue = defaultValue;
        }
        
        @Override
        public List<String> getPossibleStringValues() {
            return Collections.singletonList("1, 2, 3, ... ");
        }
        
        @Override
        public String getDefaultValueString() {
            return defaultValue == null ? "<not set>" : defaultValue.toString();
        }
        
        public Integer get() {
            return getOrDefault( Integer::parseInt, defaultValue );
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
            return Arrays.stream( StandardSubdirStrategies.values() ).map( Enum::name ).collect( Collectors.toList() );
        }
        
        public SubdirStrategy get() {
            return getOrDefault( StandardSubdirStrategies::getInstance, defaultValue );
        }
    }

    public static final boolean ATOMIC_MOVE = new BooleanProp( "fsjobs.atomicmove", false ).is();
    public static final boolean FILE_PROCESSING_PARALLEL = new BooleanProp( "fsjobs.files.parallel", false ).is();
    public static final boolean CONTENT_PROCESSING_PARALLEL = new BooleanProp( "fsjobs.content.parallel", false ).is();
    public static final Integer FILE_PROCESSING_NUM_THREADS = new IntegerProp( "fsjobs.files.threads", null ).get();
    public static final Integer CONTENT_PROCESSING_NUM_THREADS = new IntegerProp( "fsjobs.content.threads", null ).get();
    public static final String INSTANCE_NAME = new StringProp( "fsjobs.instance", "inst_01" ).get();
    public static final SubdirStrategy SUBDIR_STRATEGY = new SubdirStrategyProp( "fsjobs.files.subdirstrategy", StandardSubdirStrategies.MD5_ONE_LETTER_TWO_DIRS ).get();
    public static final SubdirStrategy SFTP_SUBDIR_STRATEGY = new SubdirStrategyProp( "sftp.files.subdirstrategy", StandardSubdirStrategies.NONE ).get();
    
    private static final Logger LOG = LoggerFactory.getLogger( Sysprops.class );
    
    static {
        if ( LOG.isInfoEnabled() ) {
            final StringBuilder sb = new StringBuilder();
            sb.append( "Simplebatch Java Properties. Use -Dpropname=propvalue to control the parameter" ).append( '\n' );
            for ( final Prop<?> p: properties() ) {
                sb
                .append( '\n' )
                .append( "# Possible Values: " ).append( String.join( ", ", p.getPossibleStringValues() ) ).append( '\n' )
                .append( "-D" ).append( p.getKey() ).append( "=" )
                .append( p.getDefaultValueString() ).append( '\n' );
            }
            LOG.info( sb.toString() );
        }
    }
    static List<Prop<?>> properties() {
        return Collections.unmodifiableList( new ArrayList<>( ALL_PROPS ) );
    }
}
