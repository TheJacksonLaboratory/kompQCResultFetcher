-- IMPC Paramater codes that are files.
SELECT
    DISTINCT ExternalID AS ImpcCode
FROM
    Output
WHERE
ExternalID IS NOT NULL AND CHAR_LENGTH(ExternalID) > 0 AND
    _dataType_key = 7;
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
-- IMPC Parameter codes of outputs that are files.
SELECT
    DISTINCT ExternalID AS ImpcCode
FROM
    rslims.Output
WHERE
ExternalID IS NOT NULL AND
CHAR_LENGTH(ExternalID) > 0 AND
    _dataType_key = 7;  -- File type
-- Stock numbers as JR numbers for complete KOMP lines.
SELECT
    CONCAT('JR', RIGHT(StockNumber, 5)) AS JR
FROM
    Line
WHERE
    _LineStatus_key = 13;

-- IMPC Paramater codes that are files.
SELECT
    DISTINCT ExternalID AS ImpcCode
FROM
    Output
WHERE
ExternalID IS NOT NULL AND CHAR_LENGTH(ExternalID) > 0 AND
    _dataType_key = 7;
