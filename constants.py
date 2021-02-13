import talib as ta
from forex_python.converter import CurrencyRates

def get_pip_value(symbol, account_currency):
    symbol_1 = symbol[0:3]
    symbol_2 = symbol[3:6]
    c = CurrencyRates()
    return c.convert(symbol_2, account_currency, c.convert(symbol_1, symbol_2, 1))

movingAveragesFunctions = {
    'SMA' : lambda close, timeP: ta.SMA(close, timeP),
    'EMA' : lambda close, timeP: ta.EMA(close, timeP),
    'WMA' : lambda close, timeP: ta.WMA(close, timeP),
    'linearReg' : lambda close, timeP: ta.LINEARREG(close, timeP),
    'TRIMA' : lambda close, timeP: ta.TRIMA(close, timeP),
    'DEMA' : lambda close, timeP: ta.DEMA(close, timeP),
    'HT_TRENDLINE' : lambda close, timeP: ta.HT_TRENDLINE(close, timeP),
    'TSF' : lambda close, timeP: ta.TSF(close, timeP)
}
