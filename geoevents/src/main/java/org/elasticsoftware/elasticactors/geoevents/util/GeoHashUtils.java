/*
 * Copyright 2013 Joost van de Wijgerd
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

package org.elasticsoftware.elasticactors.geoevents.util;

import ch.hsr.geohash.GeoHash;
import org.elasticsoftware.elasticactors.geoevents.LengthUnit;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Joost van de Wijgerd
 */
public class GeoHashUtils {
    public static List<GeoHash> getAllGeoHashesWithinRadius(final double latitude,
                                                                  final double longitude,
                                                                  final double radius,
                                                                  final LengthUnit unit,
                                                                  final int characterPrecision) {
        GeoLocation loc = GeoLocation.fromDegrees(latitude, longitude);
        GeoLocation[] bbox = loc.boundingCoordinates(LengthUnit.METRES.convert(radius, unit), GeoLocation.EARTH_SPHERE_RADIUS_METRES);
        GeoHash leftBottom = GeoHash.withCharacterPrecision(bbox[0].getLatitudeInDegrees(), bbox[0].getLongitudeInDegrees(), characterPrecision);
        GeoHash rightTop = GeoHash.withCharacterPrecision(bbox[1].getLatitudeInDegrees(), bbox[1].getLongitudeInDegrees(), characterPrecision);
        GeoHash leftTop = GeoHash.withCharacterPrecision(bbox[1].getLatitudeInDegrees(), bbox[0].getLongitudeInDegrees(), characterPrecision);


        List<GeoHash> matrix = new LinkedList<GeoHash>();
        int rows = calculateRows(leftTop, leftBottom);
        int cols = calculateColumns(leftTop, rightTop);

        GeoHash currentHash = leftTop;
        for (int i = 0; i < rows; i++) {

            matrix.add(currentHash);
            GeoHash nextHash = currentHash;
            for (int j = 1; j < cols; j++) {
                nextHash = nextHash.getEasternNeighbour();
                matrix.add(nextHash);
            }
            currentHash = currentHash.getSouthernNeighbour();
        }
        return matrix;
    }

    private static int calculateColumns(GeoHash leftTop, GeoHash rightTop) {
        int cols = 1;
        GeoHash currentHash = leftTop;
        while (!currentHash.equals(rightTop)) {
            cols += 1;
            currentHash = currentHash.getEasternNeighbour();
        }
        return cols;
    }

    private static int calculateRows(GeoHash leftTop, GeoHash leftBottom) {
        int rows = 1;
        GeoHash currentHash = leftTop;
        while (!currentHash.equals(leftBottom)) {
            rows += 1;
            currentHash = currentHash.getSouthernNeighbour();
        }
        return rows;
    }
}

