package com.maxmind.geoip;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

public class CountryLookupV6Test {
	@Test
	public void testCountryLookup() throws IOException {

		String dbfile = "src/test/resources/GeoIP/GeoIPv6.dat";
		LookupService cl = new LookupService(dbfile, LookupService.DBType.MEMORY_CACHE);

        Country countryV6 = cl.getCountryV6("64.17.254.216");
        assertEquals("US", countryV6.getCode());
		assertEquals("United States", countryV6
                .getName());
		assertEquals("US", cl.getCountryV6("::64.17.254.216").getCode());
		assertEquals("US", cl.getCountryV6("::ffff:64.17.254.216").getCode());

		assertEquals("JP", cl.getCountry("2001:200::").getCode());
		assertEquals("Japan", cl.getCountry("2001:200::").getName());
		cl.close();

	}
}
