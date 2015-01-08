/**
 * DatabaseInfo.java
 *
 * Copyright (C) 2003 MaxMind LLC.  All Rights Reserved.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

package com.maxmind.geoip;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Encapsulates metadata about the GeoIP database. The database has a date, is a
 * premium or standard version, and is one of the following types:
 *
 * <ul>
 * <li>Country edition -- this is the most common version of the database. It
 * includes the name of the country and it's ISO country code given an IP
 * address.
 * <li>Region edition -- includes the country information as well as what U.S.
 * state or Canadian province the IP address is from if the IP address is from
 * the U.S. or Canada.
 * <li>City edition -- includes country, region, city, postal code, latitude,
 * and longitude information.
 * <li>Org edition -- includes netblock owner.
 * <li>ISP edition -- ISP information.
 * </ul>
 *
 * @see com.maxmind.geoip.LookupService#getDatabaseInfo()
 * @author Matt Tucker
 */
public class DatabaseInfo {

    public final static int COUNTRY_EDITION = 1;
    public final static int REGION_EDITION_REV0 = 7;
    public final static int REGION_EDITION_REV1 = 3;
    public final static int CITY_EDITION_REV0 = 6;
    public final static int CITY_EDITION_REV1 = 2;
    public final static int ORG_EDITION = 5;
    public final static int ISP_EDITION = 4;
    public final static int PROXY_EDITION = 8;
    public final static int ASNUM_EDITION = 9;
    public final static int NETSPEED_EDITION = 10;
    public final static int DOMAIN_EDITION = 11;
    public final static int COUNTRY_EDITION_V6 = 12;
    public final static int ASNUM_EDITION_V6 = 21;
    public final static int ISP_EDITION_V6 = 22;
    public final static int ORG_EDITION_V6 = 23;
    public final static int DOMAIN_EDITION_V6 = 24;
    public final static int CITY_EDITION_REV1_V6 = 30;
    public final static int CITY_EDITION_REV0_V6 = 31;
    public final static int NETSPEED_EDITION_REV1 = 32;
    public final static int NETSPEED_EDITION_REV1_V6 = 33;

    final static int STRUCTURE_INFO_MAX_SIZE = 20;
    final static int DATABASE_INFO_MAX_SIZE = 100;


    final static int STANDARD_RECORD_LENGTH = 3;
    final static int ORG_RECORD_LENGTH = 4;
    final static int SEGMENT_RECORD_LENGTH = 3;


    public final Path path;
    //    private final String info;
    public final int databaseType;
    public final int recordLength;
    public final int databaseSegment;

    private final boolean premium;
    private final Date date;

    /**
     * Creates a new DatabaseInfo object given the database info String.
     *
     * @param path
     */
    public DatabaseInfo(Path path) throws IOException {
        if (path == null) throw new IllegalArgumentException("Path is null!");

        this.path = path;
        ByteBuffer header = getHeader(path);
        byte dbType = header.hasRemaining() ?header.get() : COUNTRY_EDITION;
        databaseType = dbType >= 106 ? (byte)(dbType - 105) : dbType;
        switch (databaseType) {
            case CITY_EDITION_REV0:
            case CITY_EDITION_REV1:
            case ASNUM_EDITION_V6:
            case NETSPEED_EDITION_REV1:
            case NETSPEED_EDITION_REV1_V6:
            case CITY_EDITION_REV0_V6:
            case CITY_EDITION_REV1_V6:
            case ASNUM_EDITION: {
                recordLength = STANDARD_RECORD_LENGTH;
                databaseSegment = databaseSegment(header);
            } break;
            case ORG_EDITION:
            case ORG_EDITION_V6:
            case ISP_EDITION:
            case ISP_EDITION_V6:
            case DOMAIN_EDITION:
            case DOMAIN_EDITION_V6: {
                recordLength = ORG_RECORD_LENGTH;
                databaseSegment = databaseSegment(header);
            } break;
            case COUNTRY_EDITION:
            case COUNTRY_EDITION_V6:
            case PROXY_EDITION:
            case NETSPEED_EDITION: {
                recordLength = STANDARD_RECORD_LENGTH;
                databaseSegment = LookupService.COUNTRY_BEGIN;
            } break;
            case REGION_EDITION_REV0: {
                recordLength = STANDARD_RECORD_LENGTH;
                databaseSegment = LookupService.STATE_BEGIN_REV0;
            } break;
            case REGION_EDITION_REV1: {
                recordLength = STANDARD_RECORD_LENGTH;
                databaseSegment = LookupService.STATE_BEGIN_REV1;
            } break;
            default:
                throw new InternalError("Cannot detect DB type!");
        }

        String info = header.asCharBuffer().toString();
        date = parseDate(info);
        premium = !info.contains("FREE");

    }

    public int getType() {
        return databaseType;
    }

    /**
     * Returns true if the database is the premium version.
     *
     * @return true if the premium version of the database.
     */
    public boolean isPremium() {
        return premium;
    }

    /**
     * Returns the date of the database.
     *
     * @return the date of the database.
     */
    public Date getDate() {
        return date;
    }

    private Date parseDate(String info) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        for (int i = 0; i < info.length() - 9; i++) {
            if (Character.isWhitespace(info.charAt(i))) {
                String dateString = info.substring(i + 1, i + 9);
                try {
                    return formatter.parse(dateString);
                } catch (ParseException pe) {}
                break;
            }
        }
        return null;
    }

//    @Override
//    public String toString() {
//        return info;
//    }


    private ByteBuffer getHeader(Path path) throws IOException {
        try(FileChannel fc = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE); FileLock lock = fc.lock()) {

            byte[] header = new byte[STRUCTURE_INFO_MAX_SIZE + 3];
            long filePos = fc.size() - header.length;
            fc.read(ByteBuffer.wrap(header), filePos);
            int i;
            for (i = 0; i < STRUCTURE_INFO_MAX_SIZE; i++) {
                if (header[STRUCTURE_INFO_MAX_SIZE - i] == -1 && header[STRUCTURE_INFO_MAX_SIZE - i + 1] == -1 && header[STRUCTURE_INFO_MAX_SIZE - i + 2] == -1)
                    return ByteBuffer.wrap(header, header.length - i, i);
            }
            return ByteBuffer.allocate(0);
        }
    }

    private static int databaseSegment(ByteBuffer header) throws IOException {
        int dbSegment = 0;
        for (int i = 0; i < SEGMENT_RECORD_LENGTH; i++)
            dbSegment += (unsignedByteToInt(header.get()) << (i * 8));
        return dbSegment;
    }

    private static int unsignedByteToInt(final byte b) { return (int) b & 0xFF; }

}
