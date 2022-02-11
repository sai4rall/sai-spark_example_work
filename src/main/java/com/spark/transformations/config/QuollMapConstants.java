package com.spark.transformations.config;

import java.util.Map;

public class QuollMapConstants {
    //    # Map the Quell cell_status to the EAI status
//# Note: The status picklist for wireless deployments; OCN WirelessDeployment StatusPicklist and arcollDeployment StatusPacklist
    public static Map<String, String> cellStatusMapDict = Map.of(
            "Commissioning", "IN CONSTRUCTION",
            "Concept", "IN CONCEPT",
            "Inactive", "PENDING DECOMMISSION",
            "In Service", "IN SERVICE",
            "In Service - Not Accepted", "NOT ACCEPTED",
            "Planned Create", "IN DESIGN");

    public static Map<String, String> statusMapDict = Map.ofEntries(
            Map.entry("Commissioning", "PENDING"),
            Map.entry("Concept", "PENDING"),
            Map.entry("Erroneous entry", "PENDING DECOMMISSION"),
            Map.entry("Inactive", "PENDING DECOMMISSION"),
            Map.entry("In Service", "ACTIVE"),
            Map.entry("In Service - Not Accepted", "PENDING"),
            Map.entry("Planned Create", "PENDING"),
            Map.entry("Removed", "DECOMMISSIONED"),
            Map.entry("Not Allocated", "PENDING"),
            Map.entry("Pending Delete", "PENDING DECOMMISSION"),
            Map.entry("Planned", "PENDING"),
            Map.entry("Unknown", "PENDING"),
            Map.entry("Working", "ACTIVE"));


    //# Map Quoll base_station_type to the EAI-collType
//        # Note: TBA is mapped to Macro as there should not be TBA type in the solution
//        # Note: A number of these cellTypes will need to be created in ocw:telstraCellTypePicklist
    public static Map<String, String> cellTypeMapDict = Map.ofEntries(
            Map.entry("Dedicated Donor", "DEDICATED DONOR"),
            Map.entry("IBC", "IN BUILDING CELL"),
            Map.entry("Macro", "MACRO"),
            Map.entry("Micro", "MICRO"),
            Map.entry("Mixed", "MIXED"),
            Map.entry("Remote Macro", "REMOTE MACRO"),
            Map.entry("Remote Micro", "REMOTE MICRO"),
            Map.entry("Remote Transceiver", "REMOTE TRANSCEIVER"),
            Map.entry("TBA", "MACRO"),
            Map.entry("Temporary", "TEMPORARY"),
            Map.entry("Test", "TEST"));

    //# Map Quoll cell function to EAT cellFunction (New field: OCW/rfCell/cellFunction)
    public static Map<Integer, String> cellFunctionbict = Map.of(
            1, "Coverage",
            2, "Capacity",
            3, "Other");

    //    # Only the following MSC_NODE values are valid. All others are to be ignored. # Note: Implementing this as a Dict in case the names need to be mapped in the future. That's my excuse and I'm sticking to it!
    public static Map<String, String> validiscNodeDict = Map.ofEntries(
            Map.entry("NSW-ACT-HSCPOOL", "NSW-ACT-HSCPOOL"),
            Map.entry("NSW-PAR-MSCPOOL", "NSW-PAR-HSCPOOL"),
            Map.entry("QLD-NSCPOOL", "OLD-HSCPOOL"),
            Map.entry("SA-NT-NS.CPOOL", "SA-NT-HSCPOOL"),
            Map.entry("VIC-TAS-NSCPOOL", "VIC-TAS-HSCPOOL"),
            Map.entry("WA-NSCPOOL", "NA-HSCPOOL"));


    public static Map<Integer, String> areaCodeDict = Map.of(
            2, "02",
            3, "03",
            7, "07",
            8, "08",
            9, "09");

    public static Map<String, String> ranNumberingDict = Map.ofEntries(
            Map.entry("10", "A"),
            Map.entry("11", "B"),
            Map.entry("12", "C"),
            Map.entry("13", "D"),
            Map.entry("14", "E"),
            Map.entry("15", "F"),
            Map.entry("16", "G"),
            Map.entry("17", "H"),
            Map.entry("18", "J"),
            Map.entry("19", "K"),
            Map.entry("20", "L"),
            Map.entry("21", "M"),
            Map.entry("22", "N"),
            Map.entry("23", "P"),
            Map.entry("24", "Q"),
            Map.entry("25", "R"),
            Map.entry("26", "S"),
            Map.entry("27", "T"),
            Map.entry("28", "U"),
            Map.entry("29", "V"),
            Map.entry("30", "W"),
            Map.entry("31", "X"),
            Map.entry("32", "Y"),
            Map.entry("33", "Z"));

}
