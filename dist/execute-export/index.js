'use strict';

const aws = require('aws-sdk');
const sns = require('../libs/publish-sns');
const asyncify = require('asyncawait/async');
const awaitify = require('asyncawait/await');
const snsArn = 'arn:aws:sns:ap-southeast-2:815588223950:lambda-activity';

const lambda = new aws.Lambda({
  region: 'ap-southeast-2',
});

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

let filterExpression = 'quoteDate >= :startDate ' +
  'and volume >= :volumeMin';

let expressionAttributeValues = {
  ':startDate': '2007-07-01',
  ':volumeMin': 1,
};

let invokeLambda = function(lambdaName, event) {
  return new Promise(function(resolve, reject) {
    lambda.invoke({
      FunctionName: lambdaName,
      InvocationType: 'Event',
      Payload: JSON.stringify(event, null, 2),
    }, function(err, data) {
      if (err) {
        reject(err);
      } else {
        console.log(`Function ${lambdaName} executed with event: `,
          `${JSON.stringify(event)}`);
        resolve(true);
      }
    });
  });
};

let executeExportHandler = asyncify(function(event, context) {
  try {
    awaitify(invokeLambda('exportCsv', {
      table: 'companyQuotes',
      columns: columns,
      filterExpression: filterExpression,
      expressionAttributeValues: expressionAttributeValues,
      s3Bucket: 'sharecast-exports',
      s3Path: 'csv',
    }));
    console.log('exportCsv successfully invoked');
    context.succeed();
  } catch (err) {
    console.error(err, err.stack);
    try {
      awaitify(sns.publishMsg(snsArn,
        err,
        'Lambda executeExportHandler failed'));
    } catch (err) {}
    context.fail('executeExportHandler failed.  ', err);
  }
});


module.exports.executeExportHandler = executeExportHandler;
