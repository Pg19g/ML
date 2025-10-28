
import { useState, useEffect } from 'react';
import { BacktestReportData } from '../types';

const generatePnlData = () => {
  const data = [];
  let value = 1;
  const startDate = new Date('2018-01-01');
  const endDate = new Date('2023-12-31');
  for (let d = startDate; d <= endDate; d.setDate(d.getDate() + 7)) {
    data.push({
      date: d.toISOString().split('T')[0],
      pnl: value,
    });
    value *= (1 + (Math.random() - 0.49) * 0.02);
  }
  return data;
};

const mockData: BacktestReportData = {
  pnlData: generatePnlData(),
  metrics: [
    { label: 'CAGR', value: '12.3%', change: '+1.2%', changeType: 'positive' },
    { label: 'Sharpe Ratio', value: '1.25', change: '+0.1', changeType: 'positive' },
    { label: 'Max Drawdown', value: '-15.8%', change: '-0.5%', changeType: 'negative' },
    { label: 'Turnover (Weekly)', value: '85%', change: '+5%', changeType: 'negative' },
  ],
  turnover: Array.from({ length: 52 }, (_, i) => ({ date: `W${i + 1}`, value: 70 + Math.random() * 30 })),
  shapImageUrl: 'https://picsum.photos/seed/shap/800/450',
  factorReturns: [
      { factor: 'Value', returns: 0.08 },
      { factor: 'Quality', returns: 0.05 },
      { factor: 'Momentum', returns: 0.12 },
      { factor: 'ST Reversion', returns: -0.02 },
      { factor: 'Buyback Yield', returns: 0.03 }
  ]
};

export const useMockBacktestData = (shouldFetch: boolean) => {
  const [data, setData] = useState<BacktestReportData | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (shouldFetch) {
      setLoading(true);
      setData(null);
      const timer = setTimeout(() => {
        setData(mockData);
        setLoading(false);
      }, 2500);

      return () => clearTimeout(timer);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [shouldFetch]);

  return { data, loading };
};
