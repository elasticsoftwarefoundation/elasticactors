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
