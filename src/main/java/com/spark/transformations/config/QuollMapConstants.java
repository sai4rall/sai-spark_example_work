package com.spark.transformations.config;

import java.util.Map;

public class QuollMapConstants {
    //    # Map the Quell cell_status to the EAI status
//# Note: The status picklist for wireless deployments; OCN WirelessDeployment StatusPicklist and arcollDeployment StatusPacklist
    public static final Map cellStatusMapDict = Map.of(
            "Commissioning", "IN CONSTRUCTION",
            "Concept", "IN CONCEPT",
            "Inactive", "PENDING DECOMMISSION",
            "In Service", "IN SERVICE",
            "In Service - Not Accepted", "NOT ACCEPTED",
            "Planned Create", "IN DESIGN");
}
