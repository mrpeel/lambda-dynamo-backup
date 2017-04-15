'use strict';

const exportTable = require('./index');

let context = {
  succeed: function(msg) {
    console.log('context.succeed(', msg || '', ')');
  },
  fail: function(msg) {
    console.log('context.fail(', msg || '', ')');
  },
};

let columns = ['12WeekBollingerBandLower', '12WeekBollingerBandUpper', '12WeekBollingerPrediction', '12WeekBollingerType',
  '12WeekVolatility', '1WeekFutureDividend', '1WeekFuturePrice', '1WeekFutureReturn', '1WeekFutureRiskAdjustedReturn',
  '1WeekVolatility', '26WeekVolatility', '2WeekFutureDividend', '2WeekFuturePrice', '2WeekFutureReturn',
  '2WeekFutureRiskAdjustedReturn', '2WeekVolatility', '4WeekBollingerBandLower', '4WeekBollingerBandUpper',
  '4WeekBollingerPrediction', '4WeekBollingerType', '4WeekVolatility', '52WeekHigh', '52WeekLow', '52WeekVolatility',
  '640106_A3597525W', '8WeekVolatility', 'AINTCOV', 'AverageVolume', 'Beta', 'BookValuePerShareYear',
  'CashPerShareYear', 'DPSRecentYear', 'DividendPerShare', 'DividendRecentQuarter', 'DividendYield',
  'EBITDMargin', 'EPS', 'EPSGrowthRate10Years', 'EPSGrowthRate5Years', 'FIRMMCRT', 'FXRUSD', 'Float',
  'Future12WeekDividend', 'Future12WeekPrice', 'Future12WeekReturn', 'Future12WeekRiskAdjustedReturn',
  'Future1WeekDividend', 'Future1WeekPrice', 'Future1WeekReturn', 'Future1WeekRiskAdjustedReturn', 'Future26WeekDividend',
  'Future26WeekPrice', 'Future26WeekReturn', 'Future26WeekRiskAdjustedReturn', 'Future2WeekDividend', 'Future2WeekPrice',
  'Future2WeekReturn', 'Future2WeekRiskAdjustedReturn', 'Future4WeekDividend', 'Future4WeekPrice', 'Future4WeekReturn',
  'Future4WeekRiskAdjustedReturn', 'Future52WeekDividend', 'Future52WeekPrice', 'Future52WeekReturn',
  'Future52WeekRiskAdjustedReturn', 'Future8WeekDividend', 'Future8WeekPrice', 'Future8WeekReturn',
  'Future8WeekRiskAdjustedReturn', 'GRCPAIAD', 'GRCPAISAD', 'GRCPBCAD', 'GRCPBCSAD', 'GRCPBMAD', 'GRCPNRAD', 'GRCPRCAD',
  'H01_GGDPCVGDP', 'H01_GGDPCVGDPFY', 'H05_GLFSEPTPOP', 'IAD', 'LTDebtToEquityQuarter', 'LTDebtToEquityYear', 'MarketCap',
  'NetIncomeGrowthRate5Years', 'NetProfitMarginPercent', 'OperatingMargin', 'PE', 'Price200DayAverage', 'Price52WeekPercChange',
  'PriceToBook', 'QuoteLast', 'ReturnOnAssets5Years', 'ReturnOnAssetsTTM', 'ReturnOnAssetsYear', 'ReturnOnEquity5Years',
  'ReturnOnEquityTTM', 'ReturnOnEquityYear', 'RevenueGrowthRate10Years', 'RevenueGrowthRate5Years', 'TotalDebtToAssetsQuarter',
  'TotalDebtToAssetsYear', 'TotalDebtToEquityQuarter', 'TotalDebtToEquityYear', 'Volume', 'adjustedPrice', 'allordchange',
  'allorddayshigh', 'allorddayslow', 'allordpercebtChangeFrom52WeekHigh', 'allordpercentChangeFrom52WeekLow',
  'allordpreviousclose', 'asxchange', 'asxdayshigh', 'asxdayslow', 'asxpercebtChangeFrom52WeekHigh',
  'asxpercentChangeFrom52WeekLow', 'asxpreviousclose', 'bookValue', 'change', 'changeFrom52WeekHigh',
  'changeFrom52WeekLow', 'changeInPercent', 'created', 'daysHigh', 'daysLow', 'dividendPerShare', 'dividendYield',
  'earningsPerShare', 'ebitda', 'epsEstimateCurrentYear', 'exDividendDate', 'exDividendPayout', 'lastTradePriceOnly',
  'marketCapitalization', 'name', 'peRatio', 'pegRatio', 'percebtChangeFrom52WeekHigh', 'percentChangeFrom52WeekLow',
  'previousClose', 'pricePerBook', 'pricePerEpsEstimateCurrentYear', 'pricePerEpsEstimateNextYear', 'pricePerSales',
  'quoteDate', 'shortRatio', 'stockExchange', 'symbol', 'volume'];
let filterExpression = 'symbol = \'ANZ\' and quoteDate BETWEEN \'2016-01-01\' AND 2016-12-31';

exportTable.writeTableToCsv({
  table: 'companyQuotes',
  columns: columns,
  filterExpression: filterExpression,
}, context);
