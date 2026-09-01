import pandas as pd

# Cached exchange rates for local execution testing
current_rates = {"EUR": 1.0, "CZK": 25.20, "USD": 1.09}

def clean_and_convert_currency_live(row):
    """
    Isolated production function with the applied negative value fix.
    """
    try:
        price = row['raw_price']
        if isinstance(price, str):
            # The exact fix applied with the hyphen (,-) to preserve negative numbers
            price = ''.join(c for c in price if c.isdigit() or c in '.,-')
            price = float(price.replace(',', '.'))
        else:
            price = float(price)
            
        currency = row['currency']
        if pd.isna(currency) or str(currency).strip() == '':
            currency = 'EUR'
        else:
            currency = str(currency).strip().upper()
            
        rate_to_use = current_rates.get(currency, 1.0)
        if float(rate_to_use) <= 0:
            rate_to_use = 1.0
            
        return round(price / float(rate_to_use), 2)
    except Exception as e:
        return 0.0

def test_negative_price_stays_negative():
    """
    Verify that negative currency values (refunds) are correctly preserved 
    and not accidentally normalized to absolute positive values.
    """
    test_row = {"raw_price": "-25.00", "currency": "EUR"}
    result = clean_and_convert_currency_live(test_row)
    
    print("\n" + "="*50)
    print(f"PIPELINE TEST OUTPUT: {result}")
    print("="*50)
    
    assert result < 0, f"Critical Bug: Negative transaction turned positive ({result})"
    print("🟢 SUCCESS: Negative sign preserved. Financial integrity verified.\n")

if __name__ == "__main__":
    test_negative_price_stays_negative()

