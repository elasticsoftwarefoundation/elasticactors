/*
 * Copyright 2013 the original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsoftware.elasticactors.geoevents;

import ch.hsr.geohash.GeoHash;
import org.elasticsoftware.elasticactors.geoevents.util.GeoHashUtils;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * @author Joost van de Wijgerd
 */
public class GeoHashSelectionTest {
    @Test
    public void testBoundingBoxPrecision3() {
        // 52.370216d,4.895168d

        List<GeoHash> geoHashes = GeoHashUtils.getAllGeoHashesWithinRadius(52.370216d, 4.895168d, 10000d, LengthUnit.METRES, 3);
        assertNotNull(geoHashes);
        assertEquals(geoHashes.size(), 1);
        assertEquals(geoHashes.get(0).toBase32(), "u17");
    }

    @Test
    public void testBoundingBoxPrecision4() {
        // 52.370216d,4.895168d

        List<GeoHash> geoHashes = GeoHashUtils.getAllGeoHashesWithinRadius(52.370216d, 4.895168d, 10000d, LengthUnit.METRES, 4);
        assertNotNull(geoHashes);
        assertEquals(geoHashes.size(), 4);
        assertEquals(geoHashes.get(0).toBase32(), "u176");
        assertEquals(geoHashes.get(1).toBase32(), "u17d");
        assertEquals(geoHashes.get(2).toBase32(), "u173");
        assertEquals(geoHashes.get(3).toBase32(), "u179");
    }

    @Test
    public void testBoundingBoxPrecision5() {
        // 52.370216d,4.895168d

        List<GeoHash> geoHashes = GeoHashUtils.getAllGeoHashesWithinRadius(52.370216d, 4.895168d, 10000d, LengthUnit.METRES, 5);
        assertNotNull(geoHashes);
        assertEquals(geoHashes.size(), 35);

    }
}
