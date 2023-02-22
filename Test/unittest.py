import src.db_init.db_init as db
import pytest

user = "dba"
pwd = "rsdba"
server = "rslims-dev.jax.org"
database = "komp"


@pytest.fixture()
def test_komp_connection():
    conn = db.init("komp")
    query1 = "SELECT * FROM komp.dccImages;"
    query2 = "SELECT * FROM komp.ebiImages"
    cursor = conn.cursor(buffered=True, dictionary=True)
    cursor.execute(query1)
    res1 = cursor.fetchall()
    cursor.execute(query2)
    res2 = cursor.fetchall()

    assert len(res1) > 0 and len(res2) > 0


@pytest.fixture()
def test_getParameterkey():
    conn = db.init("rslims")
    query = "SELECT DISTINCT ExternalID AS ImpcCode FROM Output WHERE ExternalID IS NOT NULL AND CHAR_LENGTH(" \
            "ExternalID) > 0 AND _dataType_key = 7;"
    parameterKeys = db.queryParameterKey(conn, query)
    result = sorted(parameterKeys)
    assert "IMPC_XRY_034_001" in result and "IMPC_XRY_051_001" in result and "JAX_SLW_016_001" in result and \
           "IMPC_EYE_050_001" in result


@pytest.fixture()
def test_getColonyId():
    conn = db.init("rslims")
    query = "SELECT CONCAT('JR', RIGHT(StockNumber, 5)) AS JR FROM Line WHERE _LineStatus_key = 13;"
    colonyIds = db.queryColonyId(conn, query)
    assert len(colonyIds) > 0
