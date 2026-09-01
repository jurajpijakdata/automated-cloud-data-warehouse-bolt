# 1. Ručne zadefinujeme to, čo funkcia potrebuje k životu v pamäti, aby nemusela liezť do cloudu
current_rates = {"EUR": 1.0, "CZK": 25.20, "USD": 1.09}

# 2. Sem skopírujeme čistú izolovanú funkciu z tvojho ETL, aby sme ju otestovali lokálne
def clean_and_convert_currency_live(row):
    try:
        price = row['raw_price']
        if isinstance(price, str):
            # Tu je tvoja opravená podmienka aj s mínuskom (,-)
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

# 3. Vykonáme samotný test v pamäti tvojho počítača
import pandas as pd

vysledok = clean_and_convert_currency_live({"raw_price": "-25.00", "currency": "EUR"})

print("\n=========================================")
print(f"VÝSLEDOK TVOJHO KÓDU JE: {vysledok}")
print("=========================================")

if vysledok < 0:
    print("🟢 USPECH: Mínusko zostalo zachované! Kód drží finančnú pravdu.")
else:
    print("🔴 CHYBA: Mínusko zmizlo! Z vratky sa stala tržba.")
print("=========================================\n")
