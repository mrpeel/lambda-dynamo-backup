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

let columns = ['symbol', 'quoteDate', 'lastTradePriceOnly', 'adjustedPrice', 'volume', 'daysHigh', 'daysLow',
  'previousClose', 'change', 'changeInPercent', '52WeekHigh', '52WeekLow',
  'changeFrom52WeekHigh', 'changeFrom52WeekLow', 'percebtChangeFrom52WeekHigh', 'percentChangeFrom52WeekLow',
  'Price200DayAverage', 'Price52WeekPercChange',
  '1WeekVolatility', '2WeekVolatility', '4WeekVolatility', '8WeekVolatility', '12WeekVolatility', '26WeekVolatility',
  '52WeekVolatility',
  '4WeekBollingerBandLower', '4WeekBollingerBandUpper', '4WeekBollingerPrediction', '4WeekBollingerType',
  '12WeekBollingerBandLower', '12WeekBollingerBandUpper', '12WeekBollingerPrediction', '12WeekBollingerType',
  'allordpreviousclose', 'allordchange', 'allorddayshigh', 'allorddayslow',
  'allordpercebtChangeFrom52WeekHigh', 'allordpercentChangeFrom52WeekLow',
  'asxpreviousclose', 'asxchange', 'asxdayshigh', 'asxdayslow', 'asxpercebtChangeFrom52WeekHigh',
  'asxpercentChangeFrom52WeekLow', 'exDividendDate', 'exDividendPayout',
  '640106_A3597525W', 'AINTCOV', 'AverageVolume', 'Beta', 'BookValuePerShareYear', 'CashPerShareYear', 'DPSRecentYear',
  'EBITDMargin', 'EPS', 'EPSGrowthRate10Years', 'EPSGrowthRate5Years', 'FIRMMCRT', 'FXRUSD', 'Float',
  'GRCPAIAD', 'GRCPAISAD', 'GRCPBCAD', 'GRCPBCSAD', 'GRCPBMAD', 'GRCPNRAD', 'GRCPRCAD',
  'H01_GGDPCVGDP', 'H01_GGDPCVGDPFY', 'H05_GLFSEPTPOP', 'IAD', 'LTDebtToEquityQuarter', 'LTDebtToEquityYear', 'MarketCap',
  'NetIncomeGrowthRate5Years', 'NetProfitMarginPercent', 'OperatingMargin', 'PE',
  'PriceToBook', 'QuoteLast', 'ReturnOnAssets5Years', 'ReturnOnAssetsTTM', 'ReturnOnAssetsYear', 'ReturnOnEquity5Years',
  'ReturnOnEquityTTM', 'ReturnOnEquityYear', 'RevenueGrowthRate10Years', 'RevenueGrowthRate5Years', 'TotalDebtToAssetsQuarter',
  'TotalDebtToAssetsYear', 'TotalDebtToEquityQuarter', 'TotalDebtToEquityYear', 'bookValue',
  'earningsPerShare', 'ebitda', 'epsEstimateCurrentYear',
  'marketCapitalization', 'peRatio', 'pegRatio', 'pricePerBook', 'pricePerEpsEstimateCurrentYear',
  'pricePerEpsEstimateNextYear', 'pricePerSales',
  'Future1WeekDividend', 'Future1WeekPrice', 'Future1WeekReturn', 'Future1WeekRiskAdjustedReturn',
  'Future2WeekDividend', 'Future2WeekPrice', 'Future2WeekReturn', 'Future2WeekRiskAdjustedReturn',
  'Future4WeekDividend', 'Future4WeekPrice', 'Future4WeekReturn', 'Future4WeekRiskAdjustedReturn',
  'Future8WeekDividend', 'Future8WeekPrice', 'Future8WeekReturn', 'Future8WeekRiskAdjustedReturn',
  'Future12WeekDividend', 'Future12WeekPrice', 'Future12WeekReturn', 'Future12WeekRiskAdjustedReturn',
  'Future26WeekDividend', 'Future26WeekPrice', 'Future26WeekReturn', 'Future26WeekRiskAdjustedReturn',
  'Future52WeekDividend', 'Future52WeekPrice', 'Future52WeekReturn', 'Future52WeekRiskAdjustedReturn'];

let filterExpression = 'symbol BETWEEN :startSymbol AND :endSymbol ' +
  'and quoteDate BETWEEN :startDate AND :endDate';
let expressionAttributeValues = {
  ':startSymbol': '1AD',
  ':endSymbol': 'CXM',
  ':startDate': '2016-01-01',
  ':endDate': '2016-12-31',
};


exportTable.writeTableToCsv({
  table: 'companyQuotes',
  columns: columns,
  compressed: true,
  filterExpression: filterExpression,
  expressionAttributeValues: expressionAttributeValues,
}, context);
