/*
 * Copyright 2015 freiheit.com technologies gmbh
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.freiheit.fuava.sftp.util;

import com.freiheit.fuava.sftp.RemoteFileStatus;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.text.ParseException;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Util class for any operations on filenames.
 *
 * @author Jochen Oekonomopulos (jochen.oekonomopulos@freiheit.com)
 *
 */
@ParametersAreNonnullByDefault
public class FilenameUtil {
    public static final String DATE_TIME_PATTERN =
            "((19|20)\\d\\d)(0?[1-9]|1[012])(0?[1-9]|[12][0-9]|3[01])_((0[0-9]|1[0123456789]|2[0123])([0-5][0-9])([0-5][0-9]))";

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger( FilenameUtil.class );

    private static final String FILENAME_DELIMITER = "_";

    private FilenameUtil() {
        // Util class.
    }

    /**
     * Returns the filename from the given entryList which matches the search
     * conditions.
     */
    @CheckForNull
    public static String getFilename( final String requestedTimestamp,
            final FileType fileType, final List<String> entryList, @Nullable final RemoteFileStatus status ) {
        final List<String> result = getAllMatchingFilenames( requestedTimestamp, fileType, entryList, status );

        if ( result == null || result.isEmpty() ) {
            return null;
        }
        return result.get( 0 );
    }

    /**
     * Returns the filename of the data file for given ok file.
     */
    @CheckForNull
    public static String getDataFileOfOkFile( @Nonnull final FileType type, @Nonnull final String filename ) {
        if ( filename.endsWith( type.getOkFileExtention() ) ) {
            final StringBuilder b = new StringBuilder( filename );
            b.replace( filename.lastIndexOf( type.getOkFileExtention() ), filename.lastIndexOf( type.getOkFileExtention() )
                    + type.getOkFileExtention().length(), type.getExtention() );
            return b.toString();
        }

        return null;
    }

    /**
     * Returns the filename of the data file for given ok file.
     */
    @CheckForNull
    public static String getOkFileForDataFile( @Nonnull final FileType type, @Nonnull final String filename ) {
        if ( filename.endsWith( type.getExtention() ) ) {
            final StringBuilder b = new StringBuilder( filename );
            b.replace( filename.lastIndexOf( type.getExtention() ), filename.lastIndexOf( type.getExtention() )
                    + type.getExtention().length(), type.getOkFileExtention() );
            return b.toString();
        }

        return null;
    }


    /**
     * Returns all filenames from the given entryList which match the search
     * conditions.
     */
    public static List<String> getAllMatchingFilenames( final String requestedTimestamp,
            final FileType fileType, final List<String> entryList, @Nullable final RemoteFileStatus status ) {
        return entryList.stream().filter( p -> p != null )
                .filter( f -> FilenameUtil.matchesSearchedFile( f, fileType, requestedTimestamp, status ) )
                .collect( Collectors.toList() );

    }

    /**
     * Returns the lastest date/time from a list of filenames.
     */
    @CheckForNull
    public static String extractLatestDateFromFilenames( final List<String> entryList, final FileType fileType,
            @Nullable final RemoteFileStatus status ) throws ParseException {
        final Optional<String> maxDate =
                entryList.stream()
                        .filter( p -> p != null )
                        .filter( f -> FilenameUtil.matchesSearchedFile( f, fileType, null,
                                status ) )
                        .max( ( a, b ) -> Long.compare( getDateFromFilename( a ), getDateFromFilename( b ) ) );

        if ( !maxDate.isPresent() ) {
            return null; // no timestamp found
        }
        final String max = maxDate.get();

        return String.valueOf( getDateFromFilenameWithDelimiter( max ) );
    }

    /**
     * Extracts timestamp as long.
     *
     * @return timestamp as long.
     */
    public static long timestampToLong( final String dateTimeWithDelimiter ) {
        try {
            return Long.parseLong( dateTimeWithDelimiter.replace( FILENAME_DELIMITER, "" ) );
        } catch ( final NumberFormatException e ) {
            LOG.error( "Could not extract date and time from string: " + dateTimeWithDelimiter );
            return -1;
        }
    }

    /**
     * Compares two timestamps extracted from the names of the product group
     * data.
     *
     * @param timestamp1
     *            first timestamp to compare
     * @param timestamp2
     *            second timestamp to compare
     * @return returns -1 if timestamp1 < timestamp2, 0 if they are equal and 1
     *         if timestamp1 > timestamp2
     */
    public static int compareTimestamps( final String timestamp1, final String timestamp2 ) {
        return Long.compare( timestampToLong( timestamp1 ), timestampToLong( timestamp2 ) );
    }

    /**
     * Cuts the date from a filename and returns it as a string:
     * YYYYMMDD_hhmmss.
     */
    @CheckForNull
    private static String getDateFromFilenameWithDelimiter( final String filename ) throws ParseException {

        final String[] split = filename.replace( ".ok", "" ).replace( ".zip", "" ).replace( ".csv", "" ).split( FILENAME_DELIMITER );

        // the assumption is, that the date and time is the second last and last part of the filename.

        try {
            return split[split.length - 2] + FILENAME_DELIMITER + split[split.length - 1];
        } catch ( final IndexOutOfBoundsException e ) {
            LOG.error( "Could not extract date and time from line: " + filename );
            throw new ParseException( "Could not extract date and time from line: " + filename, 0 );
        }
    }

    /**
     * Checks, whether a filename matches the pattern of a file on the server.
     */
    public static boolean matchesSearchedFile( final String filename,
            final FileType fileType, @Nullable final String lastDateTime, @Nullable final RemoteFileStatus status ) {

        return getFilenamePattern( fileType, lastDateTime, status ).matcher( filename ).find();
    }

    /**
     * Cuts the date from a filename and returns it as an int: YYYYMMDDhhmmss.
     *
     * Returns -1 in case of an error so that the file still can be used for
     * comparison.
     */
    public static long getDateFromFilename( final String filename ) {

        final String dateTimeWithDelimiter;
        try {
            dateTimeWithDelimiter = getDateFromFilenameWithDelimiter( filename );
        } catch ( final ParseException e ) {
            return -1;
        }
        return timestampToLong( dateTimeWithDelimiter );
    }

    /**
     * Returns a pattern, that matches a file corresponding to the parameter
     * fileType.
     */
    @Nonnull
    private static Pattern getFilenamePattern( final FileType type,
            @Nullable final String lastDateTime,
            @Nullable final RemoteFileStatus status ) {

        return Pattern.compile( ".*" + ( type == FileType.ALL_FILES
            ? ""
            : type.getFileIdentifier() ) + ".*_"
                + getDateTimePattern( lastDateTime )
                + getFileExtension( status, type ) );
    }

    /**
     * If status is not null, a statusfile extension will be created. Otherwise
     * just .zip.
     * <p>
     * Example: if status=Status.OK, the extension will be ".csv.ok"
     */
    private static String getFileExtension( @Nullable final RemoteFileStatus status, final FileType fileType ) {
        return status == null
            ? fileType.getExtention()
            : fileType.getOkFileExtention();
    }

    /**
     * Returns a pattern, that allows for all syntactical correct date-times, or
     * if the parameter lastDateTime is not null, this exactly date-time is
     * returned.
     */
    private static String getDateTimePattern( @Nullable final String lastDateTime ) {
        return lastDateTime == null || lastDateTime.isEmpty()
            ? DATE_TIME_PATTERN
            : lastDateTime;
    }
}
