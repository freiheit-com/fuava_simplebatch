package com.freiheit.fuava.simplebatch.persist;

import com.freiheit.fuava.simplebatch.result.Result;

public class Persistences {

    private static final class ComposedPersistence<A, B, C, D> implements Persistence<A, B, D> {
        private final Persistence<A, B, C> f;
        private final Persistence<A, C, D> g;

        private ComposedPersistence( final Persistence<A, C, D> g, final Persistence<A, B, C> f ) {
            this.g = g;
            this.f = f;
        }

        @Override
        public Iterable<Result<A, D>> persist( final Iterable<Result<A, B>> toPersist ) {
            final Iterable<Result<A, C>> fResults = this.f.persist( toPersist );
            final Iterable<Result<A, D>> gResults = this.g.persist( fResults );
            return gResults;
        }

    }

    /**
     * Compose two processors. Note that the input of g will be a set of the
     * successful and failed output values from f. Also note that f must not
     * return null outputs for successfully processed items!
     */
    public static <A, B, C, D> Persistence<A, B, D> compose( Persistence<A, C, D> g, Persistence<A, B, C> f ) {
        return new ComposedPersistence<A, B, C, D>( g, f );
    }

}
