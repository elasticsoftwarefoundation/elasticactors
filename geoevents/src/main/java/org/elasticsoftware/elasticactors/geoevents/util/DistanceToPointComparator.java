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

package org.elasticsoftware.elasticactors.geoevents.util;

import ch.hsr.geohash.GeoHash;
import org.elasticsoftware.elasticactors.geoevents.Coordinate;
import org.elasticsoftware.elasticactors.geoevents.LengthUnit;

import java.util.Comparator;

/**
 * @author Joost van de Wijgerd
 */
public class DistanceToPointComparator implements Comparator<GeoHash> {
    private final Coordinate center;

    public DistanceToPointComparator(Coordinate center) {
        this.center = center;
    }

    @Override
    public int compare(GeoHash one, GeoHash two) {
        double oneDistance = center.distance(one.getBoundingBoxCenterPoint().getLatitude(),
                                             one.getBoundingBoxCenterPoint().getLongitude(), LengthUnit.METRES);
        double twoDistance = center.distance(two.getBoundingBoxCenterPoint().getLatitude(),
                                             two.getBoundingBoxCenterPoint().getLongitude(), LengthUnit.METRES);
        if(oneDistance == twoDistance) {
            return 0;
        } else if(oneDistance < twoDistance) {
            return -1;
        } else {
            return 1;
        }
    }
}
