import re
import pandas as pd
from decimal import Decimal, InvalidOperation

def clean_and_convert_currency_live(row, rates_matrix):
    """
    Pure Financial Token Parser completely isolated from environment footprints.
    Strictly verifies formatting rules, preserves negative flags, and scales Decimals.
    """
    raw_price = row['raw_price']
    currency = row['currency']
    
    if pd.isna(currency) or str(currency).strip() == '':
        currency = 'EUR'
    else:
        currency = str(currency).strip().upper()
        
    rate_to_use = rates_matrix.get(currency, Decimal("1.0"))
    if rate_to_use <= 0:
        rate_to_use = Decimal("1.0")

    if pd.isna(raw_price) or str(raw_price).strip() == '':
        return None

    price_str = str(raw_price).strip()

    # Self-Healing text grouping corrections
    if ',' in price_str and '.' in price_str:
        if price_str.find(',') < price_str.find('.'):
            price_str = price_str.replace(',', '')
        else:
            price_str = price_str.replace('.', '').replace(',', '.')
    elif ',' in price_str and '.' not in price_str:
        price_str = price_str.replace(',', '.')

    price_str = price_str.replace('€', '').replace('$', '').strip()

    # Strict regex shield verification
    match = re.match(r"^-?\d+(?:\.\d+)?$", price_str)
    if not match:
        return None

    try:
        parsed_decimal = Decimal(price_str)
        price_eur = parsed_decimal / rate_to_use
        return float(price_eur.quantize(Decimal("0.01")))
    except (InvalidOperation, ValueError):
        return None
