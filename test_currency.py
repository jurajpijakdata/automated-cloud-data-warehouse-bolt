import pytest
from decimal import Decimal
# Import actual extracted conversion logic from pure currency parser
from currency_parser import clean_and_convert_currency_live

TEST_RATES = {"EUR": Decimal("1.0"), "CZK": Decimal("25.20"), "USD": Decimal("1.09")}

@pytest.mark.parametrize("raw_price, currency, expected_output", [
    ("-25.00", "EUR", -25.00),         # Preserves the refund minus-sign bug completely
    ("12,50", "CZK", 0.50),            # Checks standard EU format conversions
    ("  $150.50 ", "USD", 138.07),     # Checks grouping strip anomalies
    ("", "EUR", None),                 # Missing elements yield SQL NULL mappings
    ("UNKNOWN_NOISE", "EUR", None)     # Intercepts alphanumeric logging intrusions
])
def test_clean_and_convert_currency_live_logic_vectors(raw_price, currency, expected_output):
    """
    Parametrizovaná testovacia suita pre projekt Bolt Drive.
    Verifikuje samoopravné spracovanie a zachovanie záporných hodnôt (vrátenie peňazí).
    """
    mock_row = {'raw_price': raw_price, 'currency': currency}
    result = clean_and_convert_currency_live(mock_row, rates_matrix=TEST_RATES)
    assert result == expected_output

