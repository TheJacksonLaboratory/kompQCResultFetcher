-- IMPC Paramater codes that are files.
SELECT
    DISTINCT ExternalID AS ImpcCode
FROM
    Output
WHERE
ExternalID IS NOT NULL AND CHAR_LENGTH(ExternalID) > 0 AND
    _dataType_key = 7;
-- Stock numbers as JR numbers for complete KOMP lines.
SELECT
    CONCAT('JR', RIGHT(StockNumber, 5)) AS JR
FROM
    Line
WHERE
    _LineStatus_key = 13;
-- Get mouse ids as well as JR and IMPC code for procedure
SELECT
    DISTINCT OrganismID, CONCAT('JR', RIGHT(StockNumber, 5)) AS JR, ProcedureDefinition.ExternalID AS TestImpcCode
FROM
    rslims.Organism
        INNER JOIN
    rslims.Line USING (_Line_key)
        INNER JOIN
    rslims.ProcedureInstanceOrganism USING (_Organism_key)
        INNER JOIN
    rslims.ProcedureInstance USING (_ProcedureInstance_key)
        INNER JOIN
    rslims.ProcedureDefinitionVersion USING (_ProcedureDefinitionVersion_key)
        INNER JOIN
    rslims.ProcedureDefinition USING (_ProcedureDefinition_key)
WHERE
    _LineStatus_key = 13  -- i.e. KOMP completed
        AND _ProcedureDefinitionVersion_key IN (231 , 233, 275)  -- SHIRPA, EYE, EKG
ORDER BY ProcedureDefinition.ExternalID , StockNumber;
SELECT
    DISTINCT OrganismID, Output.ExternalID AS IMPCCODE, OutputValue AS FileName, _ProcedureInstance_key, url  -- url is at the DCC
FROM
    Organism
        INNER JOIN
        OrganismStudy USING (_Organism_key)
        INNER JOIN
    ProcedureInstanceOrganism USING (_Organism_key)
        INNER JOIN
    ProcedureInstance USING (_ProcedureInstance_key)
        INNER JOIN
    OutputInstanceSet USING (_ProcedureInstance_key)
        INNER JOIN
    OutputInstance USING (_OutputInstanceSet_key)
        INNER JOIN
    Output USING (_Output_key)
        LEFT OUTER JOIN
    komp.dccimages ON (OrganismID = komp.dccimages.animalName AND komp.dccimages.parameterKey = Output.ExternalID)
WHERE
    ProcedureInstance._ProcedureDefinitionVersion_key NOT IN ( 197 ) AND
    _ProcedureStatus_key = 5 AND
    (_LevelTwoReviewAction_key=14 ) -- OR _LevelTwoReviewAction_key=13)
    AND _DataType_key=7
    AND (OutputValue IS NOT NULL AND CHAR_LENGTH(OutputValue) > 0)
    AND Output.ExternalID = {parameterKey}
    AND _Study_key IN (27,57,28)
        AND url IS NULL;



