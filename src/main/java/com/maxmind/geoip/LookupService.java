/*
 * LookupService.java
 *
 * Copyright (C) 2003 MaxMind LLC. All Rights Reserved.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option) any
 * later version.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation, Inc.,
 * 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */

package com.maxmind.geoip;

import static java.lang.System.arraycopy;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.*;

/**
 * Provides a lookup service for information based on an IP address. The
 * location of a database file is supplied when creating a lookup service
 * instance. The edition of the database determines what information is
 * available about an IP address. See the DatabaseInfo class for further
 * details.
 * <p>
 *
 * The following code snippet demonstrates looking up the country that an IP
 * address is from:
 *
 * <pre>
 * // First, create a LookupService instance with the location of the database.
 * LookupService lookupService = new LookupService(&quot;c:\\geoip.dat&quot;);
 * // Assume we have a String ipAddress (in dot-decimal form).
 * Country country = lookupService.getCountry(ipAddress);
 * System.out.println(&quot;The country is: &quot; + country.getName());
 * System.out.println(&quot;The country code is: &quot; + country.getCode());
 * </pre>
 *
 * In general, a single LookupService instance should be created and then reused
 * repeatedly.
 * <p>
 *
 * <i>Tip:</i> Those deploying the GeoIP API as part of a web application may
 * find it difficult to pass in a File to create the lookup service, as the
 * location of the database may vary per deployment or may even be part of the
 * web-application. In this case, the database should be added to the classpath
 * of the web-app. For example, by putting it into the WEB-INF/classes directory
 * of the web application. The following code snippet demonstrates how to create
 * a LookupService using a database that can be found on the classpath:
 *
 * <pre>
 * String fileName = getClass().getResource(&quot;/GeoIP.dat&quot;).toExternalForm()
 *         .substring(6);
 * LookupService lookupService = new LookupService(fileName);
 * </pre>
 *
 * @author Matt Tucker (matt@jivesoftware.com)
 */
public class LookupService {

    final static int US_OFFSET = 1;
    final static int CANADA_OFFSET = 677;
    final static int WORLD_OFFSET = 1353;
    final static int FIPS_RANGE = 360;
    public final static int COUNTRY_BEGIN = 16776960;
    public final static int STATE_BEGIN_REV0 = 16700000;
    public final static int STATE_BEGIN_REV1 = 16000000;

    final static int MAX_RECORD_LENGTH = 4;

    final static int MAX_ORG_RECORD_LENGTH = 300;
    final static int FULL_RECORD_LENGTH = 60;
    final static Country UNKNOWN_COUNTRY = new Country("--", "N/A");

    final static String[] countryCode = { "--", "AP", "EU", "AD", "AE",
            "AF", "AG", "AI", "AL", "AM", "CW", "AO", "AQ", "AR", "AS", "AT",
            "AU", "AW", "AZ", "BA", "BB", "BD", "BE", "BF", "BG", "BH", "BI",
            "BJ", "BM", "BN", "BO", "BR", "BS", "BT", "BV", "BW", "BY", "BZ",
            "CA", "CC", "CD", "CF", "CG", "CH", "CI", "CK", "CL", "CM", "CN",
            "CO", "CR", "CU", "CV", "CX", "CY", "CZ", "DE", "DJ", "DK", "DM",
            "DO", "DZ", "EC", "EE", "EG", "EH", "ER", "ES", "ET", "FI", "FJ",
            "FK", "FM", "FO", "FR", "SX", "GA", "GB", "GD", "GE", "GF", "GH",
            "GI", "GL", "GM", "GN", "GP", "GQ", "GR", "GS", "GT", "GU", "GW",
            "GY", "HK", "HM", "HN", "HR", "HT", "HU", "ID", "IE", "IL", "IN",
            "IO", "IQ", "IR", "IS", "IT", "JM", "JO", "JP", "KE", "KG", "KH",
            "KI", "KM", "KN", "KP", "KR", "KW", "KY", "KZ", "LA", "LB", "LC",
            "LI", "LK", "LR", "LS", "LT", "LU", "LV", "LY", "MA", "MC", "MD",
            "MG", "MH", "MK", "ML", "MM", "MN", "MO", "MP", "MQ", "MR", "MS",
            "MT", "MU", "MV", "MW", "MX", "MY", "MZ", "NA", "NC", "NE", "NF",
            "NG", "NI", "NL", "NO", "NP", "NR", "NU", "NZ", "OM", "PA", "PE",
            "PF", "PG", "PH", "PK", "PL", "PM", "PN", "PR", "PS", "PT", "PW",
            "PY", "QA", "RE", "RO", "RU", "RW", "SA", "SB", "SC", "SD", "SE",
            "SG", "SH", "SI", "SJ", "SK", "SL", "SM", "SN", "SO", "SR", "ST",
            "SV", "SY", "SZ", "TC", "TD", "TF", "TG", "TH", "TJ", "TK", "TM",
            "TN", "TO", "TL", "TR", "TT", "TV", "TW", "TZ", "UA", "UG", "UM",
            "US", "UY", "UZ", "VA", "VC", "VE", "VG", "VI", "VN", "VU", "WF",
            "WS", "YE", "YT", "RS", "ZA", "ZM", "ME", "ZW", "A1", "A2", "O1",
            "AX", "GG", "IM", "JE", "BL", "MF", "BQ", "SS", "O1" };

    final static String[] countryName = { "N/A", "Asia/Pacific Region",
            "Europe", "Andorra", "United Arab Emirates", "Afghanistan",
            "Antigua and Barbuda", "Anguilla", "Albania", "Armenia", "Curacao",
            "Angola", "Antarctica", "Argentina", "American Samoa", "Austria",
            "Australia", "Aruba", "Azerbaijan", "Bosnia and Herzegovina",
            "Barbados", "Bangladesh", "Belgium", "Burkina Faso", "Bulgaria",
            "Bahrain", "Burundi", "Benin", "Bermuda", "Brunei Darussalam",
            "Bolivia", "Brazil", "Bahamas", "Bhutan", "Bouvet Island",
            "Botswana", "Belarus", "Belize", "Canada",
            "Cocos (Keeling) Islands", "Congo, The Democratic Republic of the",
            "Central African Republic", "Congo", "Switzerland",
            "Cote D'Ivoire", "Cook Islands", "Chile", "Cameroon", "China",
            "Colombia", "Costa Rica", "Cuba", "Cape Verde", "Christmas Island",
            "Cyprus", "Czech Republic", "Germany", "Djibouti", "Denmark",
            "Dominica", "Dominican Republic", "Algeria", "Ecuador", "Estonia",
            "Egypt", "Western Sahara", "Eritrea", "Spain", "Ethiopia",
            "Finland", "Fiji", "Falkland Islands (Malvinas)",
            "Micronesia, Federated States of", "Faroe Islands", "France",
            "Sint Maarten (Dutch part)", "Gabon", "United Kingdom", "Grenada",
            "Georgia", "French Guiana", "Ghana", "Gibraltar", "Greenland",
            "Gambia", "Guinea", "Guadeloupe", "Equatorial Guinea", "Greece",
            "South Georgia and the South Sandwich Islands", "Guatemala",
            "Guam", "Guinea-Bissau", "Guyana", "Hong Kong",
            "Heard Island and McDonald Islands", "Honduras", "Croatia",
            "Haiti", "Hungary", "Indonesia", "Ireland", "Israel", "India",
            "British Indian Ocean Territory", "Iraq",
            "Iran, Islamic Republic of", "Iceland", "Italy", "Jamaica",
            "Jordan", "Japan", "Kenya", "Kyrgyzstan", "Cambodia", "Kiribati",
            "Comoros", "Saint Kitts and Nevis",
            "Korea, Democratic People's Republic of", "Korea, Republic of",
            "Kuwait", "Cayman Islands", "Kazakhstan",
            "Lao People's Democratic Republic", "Lebanon", "Saint Lucia",
            "Liechtenstein", "Sri Lanka", "Liberia", "Lesotho", "Lithuania",
            "Luxembourg", "Latvia", "Libya", "Morocco", "Monaco",
            "Moldova, Republic of", "Madagascar", "Marshall Islands",
            "Macedonia", "Mali", "Myanmar", "Mongolia", "Macau",
            "Northern Mariana Islands", "Martinique", "Mauritania",
            "Montserrat", "Malta", "Mauritius", "Maldives", "Malawi", "Mexico",
            "Malaysia", "Mozambique", "Namibia", "New Caledonia", "Niger",
            "Norfolk Island", "Nigeria", "Nicaragua", "Netherlands", "Norway",
            "Nepal", "Nauru", "Niue", "New Zealand", "Oman", "Panama", "Peru",
            "French Polynesia", "Papua New Guinea", "Philippines", "Pakistan",
            "Poland", "Saint Pierre and Miquelon", "Pitcairn Islands",
            "Puerto Rico", "Palestinian Territory", "Portugal", "Palau",
            "Paraguay", "Qatar", "Reunion", "Romania", "Russian Federation",
            "Rwanda", "Saudi Arabia", "Solomon Islands", "Seychelles", "Sudan",
            "Sweden", "Singapore", "Saint Helena", "Slovenia",
            "Svalbard and Jan Mayen", "Slovakia", "Sierra Leone", "San Marino",
            "Senegal", "Somalia", "Suriname", "Sao Tome and Principe",
            "El Salvador", "Syrian Arab Republic", "Swaziland",
            "Turks and Caicos Islands", "Chad", "French Southern Territories",
            "Togo", "Thailand", "Tajikistan", "Tokelau", "Turkmenistan",
            "Tunisia", "Tonga", "Timor-Leste", "Turkey", "Trinidad and Tobago",
            "Tuvalu", "Taiwan", "Tanzania, United Republic of", "Ukraine",
            "Uganda", "United States Minor Outlying Islands", "United States",
            "Uruguay", "Uzbekistan", "Holy See (Vatican City State)",
            "Saint Vincent and the Grenadines", "Venezuela",
            "Virgin Islands, British", "Virgin Islands, U.S.", "Vietnam",
            "Vanuatu", "Wallis and Futuna", "Samoa", "Yemen", "Mayotte",
            "Serbia", "South Africa", "Zambia", "Montenegro", "Zimbabwe",
            "Anonymous Proxy", "Satellite Provider", "Other", "Aland Islands",
            "Guernsey", "Isle of Man", "Jersey", "Saint Barthelemy",
            "Saint Martin", "Bonaire, Saint Eustatius and Saba", "South Sudan",
            "Other" };

    /* check the hashmap once at startup time */
    static {
        if (countryCode.length != countryName.length)
            throw new AssertionError("countryCode.length!=countryName.length");
    }

    protected Thread watchThread = null;

    public interface UpdateCallback {
        /**
         * Will be called when the LookupService can be replaced with the updated service.
         * @param updatedService
         */
        void update(com.maxmind.geoip.LookupService updatedService);
    }


    private interface Reader {
        void readBuffer(byte[] buffer, int offset, int length);
        void close();
    }

    private static class FileReader implements Reader {
        final FileChannel fileChannel;

        public FileReader(DatabaseInfo dbInfo) throws IOException {
            fileChannel = FileChannel.open(dbInfo.path, StandardOpenOption.READ);
        }

        @Override
        public void readBuffer(byte[] buffer, int offset, int length) {
            try {
                fileChannel.read(ByteBuffer.wrap(buffer), offset);
            } catch (IOException e) {
                System.out.println("IO Exception");
            }
        }

        @Override
        public void close() {
            try {
                fileChannel.close();
            } catch (IOException e) {
                // Here for backward compatibility.
            }
        }
    }

    private static class MemoryReader implements Reader {
        final byte[] data;

        public MemoryReader(DatabaseInfo dbInfo) throws IOException {
            //Lock file so it's not modified while reading.
            try(FileChannel fileChannel = FileChannel.open(dbInfo.path, StandardOpenOption.READ, StandardOpenOption.WRITE); FileLock lock = fileChannel.lock()){
                data = new byte[(int) fileChannel.size()];
                fileChannel.read(ByteBuffer.wrap(data), 0);
            }
        }

        @Override
        public void readBuffer(byte[] buffer, int offset, int length) {
            int maxLen = data.length - offset;
            arraycopy(data, offset, buffer, 0, maxLen < length ? maxLen : length);
        }

        public void close() {}
    }

    private static class IndexReader extends FileReader {
        final byte[] index;

        public IndexReader(DatabaseInfo dbInfo) throws IOException {
            super(dbInfo);
            index = new byte[dbInfo.databaseSegment * dbInfo.recordLength * 2];
            fileChannel.read(ByteBuffer.wrap(index), 0);
        }

        @Override
        public void readBuffer(byte[] buffer, int offset, int length) {
            if((offset + length) <= index.length) arraycopy(index, offset, buffer, 0, length);
            else super.readBuffer(buffer, offset, length);
        }

    }

    public enum DBType {
        File { @Override Reader getReader(DatabaseInfo dbInfo) throws IOException { return new FileReader(dbInfo); } },
        INDEX_CACHE { @Override Reader getReader(DatabaseInfo dbInfo) throws IOException { return new IndexReader(dbInfo); } },
        MEMORY_CACHE { @Override Reader getReader(DatabaseInfo dbInfo) throws IOException { return new MemoryReader(dbInfo); } };

        abstract Reader getReader(DatabaseInfo dbInfo) throws IOException;
    }


    private final DBType dbType;
    protected final DatabaseInfo dbInfo;
    protected final Reader reader;


    /**
     * Create a new lookup service using the specified database file.
     *
     * @param databaseFile
     *            String representation of the database file.
     * @param dbType
     *            database flags to use when opening the database GEOIP_STANDARD
     *            read database from disk GEOIP_MEMORY_CACHE cache the database
     *            in RAM and read it from RAM
     * @throws java.io.IOException
     *             if an error occured creating the lookup service from the
     *             database file.
     */
    public LookupService(String databaseFile, DBType dbType) throws IOException {
        this(Paths.get(databaseFile), dbType);
    }

    /**
     * Create a new lookup service using the specified database file.
     *
     * @param databaseFile
     *            the database file.
     * @param dbType
     *            database flags to use when opening the database GEOIP_STANDARD
     *            read database from disk GEOIP_MEMORY_CACHE cache the database
     *            in RAM and read it from RAM
     * @throws java.io.IOException
     *             if an error occured creating the lookup service from the
     *             database file.
     */
    public LookupService(File databaseFile, DBType dbType) throws IOException {
        this(databaseFile.toPath(), dbType);
    }

    /**
     * Create a new lookup service using the specified database file.
     *
     * @param databasePath
     *            the database path.
     * @param dbType
     *            database flags to use when opening the database GEOIP_STANDARD
     *            read database from disk GEOIP_MEMORY_CACHE cache the database
     *            in RAM and read it from RAM
     * @throws java.io.IOException
     *             if an error occured creating the lookup service from the
     *             database file.
     */
    public LookupService(Path databasePath, DBType dbType) throws IOException {
        this.dbInfo = new DatabaseInfo(databasePath);
        this.reader = dbType.getReader(dbInfo);
        this.dbType = dbType;
    }


    public void close() {
        reader.close();
    }

    private int seekCountry(long ipAddress) {
        byte[] buf = new byte[2 * MAX_RECORD_LENGTH];
        int offset = 0;
        for (int depth = 31; depth >= 0; depth--) {
            reader.readBuffer(buf, 2 * dbInfo.recordLength * offset, buf.length);
            int x1 = calcX(dbInfo.recordLength, buf);
            if ((ipAddress & (1 << depth)) > 0) {
                if (x1 >= dbInfo.databaseSegment) {
//                        last_netmask = 32 - depth;
                    return x1;
                }
                offset = x1;
            } else {
                int x0 = calcX(0, buf);
                if (x0 >= dbInfo.databaseSegment) {
//                        last_netmask = 32 - depth;
                    return x0;
                }
                offset = x0;
            }
        }

        // shouldn't reach here
        System.err.println("Error seeking country while seeking " + ipAddress);
        return 0;
    }

    /**
     * Finds the country index value given an IPv6 address.
     *
     * @param addr
     *            the ip address to find in long format.
     * @return the country index.
     */
    private int seekCountryV6(InetAddress addr) {
        byte[] v6vec = addr.getAddress();

        if (v6vec.length == 4) {
            // sometimes java returns an ipv4 address for IPv6 input
            // we have to work around that feature
            // It happens for ::ffff:24.24.24.24
            byte[] t = new byte[16];
            System.arraycopy(v6vec, 0, t, 12, 4);
            v6vec = t;
        }

        byte[] buf = new byte[2 * MAX_RECORD_LENGTH];
        int offset = 0;
        for (int depth = 127; depth >= 0; depth--) {
            reader.readBuffer(buf, 2 * dbInfo.recordLength * offset, buf.length);
            int bnum = 127 - depth;
            int idx = bnum >> 3;
            int b_mask = 1 << (bnum & 7 ^ 7);
            if ((v6vec[idx] & b_mask) > 0) {
                int x1 = calcX(dbInfo.recordLength, buf);
                if (x1 >= dbInfo.databaseSegment) {
//                        last_netmask = 128 - depth;
                    return x1;
                }
                offset = x1;
            } else {
                int x0 = calcX(0, buf);
                if (x0 >= dbInfo.databaseSegment) {
//                        last_netmask = 128 - depth;
                    return x0;
                }
                offset = x0;
            }
        }

        // shouldn't reach here
        System.err.println("Error seeking country while seeking " + addr.getHostAddress());
        return 0;
    }


    /* TODO: Better name..
     */
    private int calcX(final int offset, final byte[] buf) {
        int result = 0;
        for (int j = 0; j < dbInfo.recordLength; j++) {
            int y = buf[offset + j];
            if (y < 0) y += 256;
            result += (y << (j * 8));
        }
        return result;
    }
    /**
     * Returns the country the IP address is in.
     *
     * @param ipAddress
     *            String version of an IPv6 address, i.e. "::127.0.0.1"
     * @return the country the IP address is from.
     */
    public Country getCountryV6(String ipAddress) {
        InetAddress addr;
        try {
            addr = InetAddress.getByName(ipAddress);
        } catch (UnknownHostException e) {
            return UNKNOWN_COUNTRY;
        }
        return getCountryV6(addr);
    }

    /**
     * Returns the country the IP address is in.
     *
     * @param ipAddress
     *            String version of an IP address, i.e. "127.0.0.1"
     * @return the country the IP address is from.
     */
    public Country getCountry(String ipAddress) {
        InetAddress addr;
        try {
            addr = InetAddress.getByName(ipAddress);
        } catch (UnknownHostException e) {
            return UNKNOWN_COUNTRY;
        }
        return getCountry(bytesToLong(addr.getAddress()));
    }

    /**
     * Returns the country the IP address is in.
     *
     * @param ipAddress
     *            the IP address.
     * @return the country the IP address is from.
     */
    public Country getCountry(InetAddress ipAddress) {
        return getCountry(bytesToLong(ipAddress.getAddress()));
    }

    /**
     * Returns the country the IP address is in.
     *
     * @param addr
     *            the IP address as Inet6Address.
     * @return the country the IP address is from.
     */
    public Country getCountryV6(InetAddress addr) {
        int ret = seekCountryV6(addr) - COUNTRY_BEGIN;
        return ret == 0 ? UNKNOWN_COUNTRY : new Country(countryCode[ret], countryName[ret]);
    }

    /**
     * Returns the country the IP address is in.
     *
     * @param ipAddress
     *            the IP address in long format.
     * @return the country the IP address is from.
     */
    public Country getCountry(long ipAddress) {
        int ret = seekCountry(ipAddress) - COUNTRY_BEGIN;
        return ret == 0 ? UNKNOWN_COUNTRY : new Country(countryCode[ret], countryName[ret]);
    }

    public int getID(String ipAddress) {
        InetAddress addr;
        try {
            addr = InetAddress.getByName(ipAddress);
        } catch (UnknownHostException e) {
            return 0;
        }
        return getID(bytesToLong(addr.getAddress()));
    }

    public int getID(InetAddress ipAddress) {
        return getID(bytesToLong(ipAddress.getAddress()));
    }

    public int getID(long ipAddress) {
        return  seekCountry(ipAddress) - dbInfo.databaseSegment;
    }

    // for GeoIP City only
    public Location getLocationV6(String str) {
        InetAddress addr;
        try {
            addr = InetAddress.getByName(str);
        } catch (UnknownHostException e) {
            return null;
        }

        return getLocationV6(addr);
    }

    // for GeoIP City only
    public Location getLocation(InetAddress addr) {
        return getLocation(bytesToLong(addr.getAddress()));
    }

    // for GeoIP City only
    public Location getLocation(String str) {
        InetAddress addr;
        try {
            addr = InetAddress.getByName(str);
        } catch (UnknownHostException e) {
            return null;
        }

        return getLocation(addr);
    }

    public Region getRegion(String str) {
        InetAddress addr;
        try {
            addr = InetAddress.getByName(str);
        } catch (UnknownHostException e) {
            return null;
        }

        return getRegion(bytesToLong(addr.getAddress()));
    }

    public Region getRegion(long ipnum) {
        Region record = new Region();

        if (dbInfo.databaseType == DatabaseInfo.REGION_EDITION_REV0) {
            int seek_region = seekCountry(ipnum) - STATE_BEGIN_REV0;
            char ch[] = new char[2];
            if (seek_region >= 1000) {
                record.countryCode = "US";
                record.countryName = "United States";
                ch[0] = (char) (((seek_region - 1000) / 26) + 65);
                ch[1] = (char) (((seek_region - 1000) % 26) + 65);
                record.region = new String(ch);
            } else {
                record.countryCode = countryCode[seek_region];
                record.countryName = countryName[seek_region];
                record.region = "";
            }
        } else if (dbInfo.databaseType == DatabaseInfo.REGION_EDITION_REV1) {
            int seek_region = seekCountry(ipnum) - STATE_BEGIN_REV1;
            char ch[] = new char[2];
            if (seek_region < US_OFFSET) {
                record.countryCode = "";
                record.countryName = "";
                record.region = "";
            } else if (seek_region < CANADA_OFFSET) {
                record.countryCode = "US";
                record.countryName = "United States";
                ch[0] = (char) (((seek_region - US_OFFSET) / 26) + 65);
                ch[1] = (char) (((seek_region - US_OFFSET) % 26) + 65);
                record.region = new String(ch);
            } else if (seek_region < WORLD_OFFSET) {
                record.countryCode = "CA";
                record.countryName = "Canada";
                ch[0] = (char) (((seek_region - CANADA_OFFSET) / 26) + 65);
                ch[1] = (char) (((seek_region - CANADA_OFFSET) % 26) + 65);
                record.region = new String(ch);
            } else {
                record.countryCode = countryCode[(seek_region - WORLD_OFFSET) / FIPS_RANGE];
                record.countryName = countryName[(seek_region - WORLD_OFFSET) / FIPS_RANGE];
                record.region = "";
            }
        }
        return record;
    }

    public Location getLocationV6(InetAddress addr) {
        Location record = new Location();
        try {
            int seek_country = seekCountryV6(addr);
            if (seek_country == dbInfo.databaseSegment) return null;
            int record_pointer = seek_country + (2 * dbInfo.recordLength - 1) * dbInfo.databaseSegment;

            byte record_buf[] = new byte[FULL_RECORD_LENGTH];
            reader.readBuffer(record_buf, record_pointer, FULL_RECORD_LENGTH);
            // get country
            record.countryCode = countryCode[unsignedByteToInt(record_buf[0])];
            record.countryName = countryName[unsignedByteToInt(record_buf[0])];

            int record_buf_offset = 1;

            // get region
            int str_length = stringScan(record_buf, record_buf_offset);
            if (str_length > 0) {
                record.region = new String(record_buf, record_buf_offset, str_length);
            }
            record_buf_offset += str_length + 1;

            // get city
            str_length = stringScan(record_buf, record_buf_offset);
            if (str_length > 0) {
                record.city = new String(record_buf, record_buf_offset, str_length, "ISO-8859-1");
            }
            record_buf_offset += str_length + 1;

            // get postal code
            str_length = stringScan(record_buf, record_buf_offset);
            if (str_length > 0) {
                record.postalCode = new String(record_buf, record_buf_offset, str_length);
            }
            record_buf_offset += str_length + 1;

            record.latitude = (float) extractCoordValue(record_buf, record_buf_offset);
            record_buf_offset += 3;
            record.longitude = (float) extractCoordValue(record_buf, record_buf_offset);

            record.dma_code = record.metro_code = 0;
            record.area_code = 0;
            if (dbInfo.databaseType == dbInfo.CITY_EDITION_REV1) {
                // get DMA code
                int metroarea_combo = 0;
                if ("US".equals(record.countryCode)) {
                    record_buf_offset += 3;
                    for (int j = 0; j < 3; j++)
                        metroarea_combo += (unsignedByteToInt(record_buf[record_buf_offset + j]) << (j * 8));
                    record.metro_code = record.dma_code = metroarea_combo / 1000;
                    record.area_code = metroarea_combo % 1000;
                }
            }
        } catch (IOException e) {
            System.err.println("IO Exception while seting up segments");
        }
        return record;
    }

    public Location getLocation(long ipnum) {
        byte record_buf[] = new byte[FULL_RECORD_LENGTH];
        int record_buf_offset = 0;
        Location record = new Location();
        try {
            int seek_country = seekCountry(ipnum);
            if (seek_country == dbInfo.databaseSegment) {
                return null;
            }
            int record_pointer = seek_country + (2 * dbInfo.recordLength - 1) * dbInfo.databaseSegment;
            reader.readBuffer(record_buf, record_pointer, FULL_RECORD_LENGTH);
            // get country
            record.countryCode = countryCode[unsignedByteToInt(record_buf[0])];
            record.countryName = countryName[unsignedByteToInt(record_buf[0])];
            record_buf_offset++;

            // get region
            int  str_length = stringScan(record_buf, record_buf_offset);
            if (str_length > 0) {
                record.region = new String(record_buf, record_buf_offset, str_length);
            }
            record_buf_offset += str_length + 1;
            // get city
            str_length = stringScan(record_buf, record_buf_offset);
            if (str_length > 0) {
                record.city = new String(record_buf, record_buf_offset, str_length, "ISO-8859-1");
            }
            record_buf_offset += str_length + 1;

            // get postal code
            str_length = stringScan(record_buf, record_buf_offset);
            if (str_length > 0) {
                record.postalCode = new String(record_buf, record_buf_offset, str_length);
            }
            record_buf_offset += str_length + 1;
            record.latitude = (float) extractCoordValue(record_buf, record_buf_offset);
            record_buf_offset += 3;
            record.longitude = (float) extractCoordValue(record_buf, record_buf_offset);

            record.dma_code = record.metro_code = 0;
            record.area_code = 0;
            if (dbInfo.databaseType == dbInfo.CITY_EDITION_REV1) {
                // get DMA code
                int metroarea_combo = 0;
                if ("US".equals(record.countryCode)) {
                    record_buf_offset += 3;
                    for (int j = 0; j < 3; j++)
                        metroarea_combo += (unsignedByteToInt(record_buf[record_buf_offset + j]) << (j * 8));
                    record.metro_code = record.dma_code = metroarea_combo / 1000;
                    record.area_code = metroarea_combo % 1000;
                }
            }
        } catch (IOException e) {
            System.err.println("IO Exception while seting up segments");
        }
        return record;
    }

    private static double extractCoordValue(final byte[] buffer, final int offset) {
        double value = 0;
        for (int j = 0; j < 3; j++)
            value += (unsignedByteToInt(buffer[offset + j]) << (j * 8));
        return value / 10000 - 180;
    }

    public String getOrg(InetAddress addr) {
        return getOrg(bytesToLong(addr.getAddress()));
    }

    public String getOrg(String str) {
        try {
            return getOrg(InetAddress.getByName(str));
        } catch (UnknownHostException e) {
            return null;
        }
    }

    // GeoIP Organization and ISP Edition methods
    public String getOrg(long ipnum) {
        int seek_org = seekCountry(ipnum);
        return getOrg(seek_org);
    }

    public String getOrgV6(String str) {
        try {
            return getOrgV6(InetAddress.getByName(str));
        } catch (UnknownHostException e) {
            return null;
        }
    }

    // GeoIP Organization and ISP Edition methods
    public String getOrgV6(InetAddress addr) {
        int seek_org = seekCountryV6(addr);
        return getOrg(seek_org);
    }


    private String getOrg(int seek_org) {
        if (seek_org == dbInfo.databaseSegment) return null;
        int record_pointer = seek_org + (2 * dbInfo.recordLength - 1) * dbInfo.databaseSegment;
        try {
            byte[] buf = new byte[MAX_ORG_RECORD_LENGTH];
            reader.readBuffer(buf, record_pointer, buf.length);
            int strLength = stringScan(buf, 0);
            return new String(buf, 0, strLength, "ISO-8859-1");
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("IO Exception");
            return null;
        }
    }

    private static int stringScan(final byte[] buffer, final int offset) {
        int strLength;
        //noinspection StatementWithEmptyBody
        for (strLength = offset; strLength < buffer.length && buffer[strLength] != 0; strLength++);
        return strLength - offset;
    }

    /**
     * Returns the long version of an IP address given an InetAddress object.
     *
     * @param address
     *            the InetAddress.
     * @return the long form of the IP address.
     */
    private static long bytesToLong(final byte[] address) {
        long ipnum = 0;
        for (int i = 0; i < 4; ++i) {
            long y = address[i];
            if (y < 0) y += 256;
            ipnum += y << ((3 - i) * 8);
        }
        return ipnum;
    }

    private static int unsignedByteToInt(final byte b) { return (int) b & 0xFF; }

    public DatabaseInfo getDatabaseInfo() {
        return dbInfo;
    }

    public synchronized void watch(final UpdateCallback updateCallback) throws IOException {
        if(watchThread != null) {
            watchThread.interrupt();
            watchThread = null;
        }
        if(updateCallback == null) return;
        watchThread = new Thread("GeoAPI FileWatcher") {
            @Override
            public void run() {
                final Path dirPath = dbInfo.path.getParent();
                final Path filePath = dbInfo.path.getFileName();
                try(WatchService watchService = dirPath.getFileSystem().newWatchService()) {
                    WatchKey key = dirPath.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);
                    while(true) {
                        WatchKey wk = watchService.take();
                        boolean doUpdate = false;
                        for (WatchEvent<?> event : wk.pollEvents())
                            doUpdate = doUpdate || filePath.equals(event.context());
                        key.reset();
                        if(doUpdate) updateCallback.update(new LookupService(dbInfo.path, dbType));
                    }
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };

        watchThread.setPriority(Thread.NORM_PRIORITY);
        watchThread.setDaemon(true);
        watchThread.start();
    }
}
