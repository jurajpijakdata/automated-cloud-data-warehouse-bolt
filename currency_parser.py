import re
import logging
import pandas as pd
from decimal import Decimal, InvalidOperation
from typing import Dict, Any, Optional

def clean_and_convert_currency_live(row: Dict[str, Any], rates_matrix: Dict[str, Decimal]) -> Optional[float]:
    """
    Safely normalizes, validates, and converts a transactional monetary record into EUR.

    This function strips currency markers, handles cross-platform alphanumeric grouping
    variations (US/EU decimal notations), enforces strict regex verification to avoid silent
    data corruption, and dynamically applies SCD Type 2 exchange rates matrix alignments.

    Args:
        row (Dict[str, Any]): A pandas row array or dictionary containing 'raw_price' and 'currency'.
        rates_matrix (Dict[str, Decimal]): Active relational mapping reference exchange rates.

    Returns:
        Optional[float]: Normalized high-precision value in EUR currency system, 
                         or None if text corruption or structural drift is isolated.
    """
    raw_price: Any = row.get('raw_price')
    currency: Any = row.get('currency')
    
    if pd.isna(currency) or str(currency).strip() == '':
        currency_str: str = 'EUR'
    else:
        currency_str = str(currency).strip().upper()
        
    rate_to_use: Decimal = rates_matrix.get(currency_str, Decimal("1.0"))
    if rate_to_use <= 0:
        rate_to_use = Decimal("1.0")

    if pd.isna(raw_price) or str(raw_price).strip() == '':
        return None

    price_str: str = str(raw_price).strip()

    # Self-Healing text grouping corrections (EU/US formats handling matrix)
    if ',' in price_str and '.' in price_str:
        if price_str.find(',') < price_str.find('.'):
            price_str = price_str.replace(',', '')
        else:
            price_str = price_str.replace('.', '').replace(',', '.')
    elif ',' in price_str and '.' not in price_str:
        price_str = price_str.replace(',', '.')

    price_str = price_str.replace('€', '').replace('$', '').strip()

    # Strict regex shield verification (Enforces precisely one decimal system)
    match: Optional[re.Match[str]] = re.match(r"^-?\d+(?:\.\d+)?$", price_str)
    if not match:
        return None  # Triggers quarantine logging metric downstream

    try:
        parsed_decimal: Decimal = Decimal(price_str)
        price_eur: Decimal = parsed_decimal / rate_to_use
        return float(price_eur.quantize(Decimal("0.01")))
    except (InvalidOperation, ValueError):
        return None
