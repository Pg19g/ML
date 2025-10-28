
import React from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, AreaChart, Area, BarChart, Bar } from 'recharts';
import { BacktestReportData, Metric } from '../types';
import Card from './ui/Card';
import { useMockBacktestData } from '../hooks/useMockBacktestData';

const MetricCard: React.FC<{ metric: Metric }> = ({ metric }) => (
  <div className="bg-gray-800 p-4 rounded-lg">
    <p className="text-sm text-gray-400">{metric.label}</p>
    <p className="text-2xl font-bold text-gray-100">{metric.value}</p>
    {metric.change && (
      <p className={`text-sm font-medium ${metric.changeType === 'positive' ? 'text-green-400' : 'text-red-400'}`}>
        {metric.change}
      </p>
    )}
  </div>
);

const CustomTooltip = ({ active, payload, label }: any) => {
  if (active && payload && payload.length) {
    return (
      <div className="bg-gray-700/80 backdrop-blur-sm p-2 border border-gray-600 rounded-md shadow-lg">
        <p className="label text-sm text-gray-200">{`${label}`}</p>
        <p className="intro text-sm text-cyan-400">{`Value : ${payload[0].value.toFixed(2)}`}</p>
      </div>
    );
  }
  return null;
};

const BacktestReport: React.FC<{ isVisible: boolean }> = ({ isVisible }) => {
  const { data, loading } = useMockBacktestData(isVisible);

  if (!isVisible) {
    return (
      <Card title="Backtest Report">
        <div className="h-[70vh] flex items-center justify-center text-gray-500">
          <p>Run the 'Generate Report' job to see results.</p>
        </div>
      </Card>
    );
  }

  if (loading || !data) {
    return (
      <Card title="Backtest Report">
        <div className="h-[70vh] flex items-center justify-center">
          <div className="animate-spin rounded-full h-16 w-16 border-t-2 border-b-2 border-cyan-500"></div>
        </div>
      </Card>
    );
  }
  
  return (
    <Card title="Backtest Report" subtitle={`2018-01-01 to 2023-12-31`}>
      <div className="space-y-6">
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          {data.metrics.map(metric => <MetricCard key={metric.label} metric={metric} />)}
        </div>

        <div>
          <h3 className="text-lg font-semibold mb-2 text-gray-100">Portfolio P&L</h3>
          <div className="h-80 w-full">
            <ResponsiveContainer>
              <AreaChart data={data.pnlData} margin={{ top: 5, right: 20, left: -10, bottom: 5 }}>
                <defs>
                  <linearGradient id="colorPnl" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#22d3ee" stopOpacity={0.6}/>
                    <stop offset="95%" stopColor="#22d3ee" stopOpacity={0}/>
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="#3c3c3c" />
                <XAxis dataKey="date" stroke="#888888" fontSize={12} tickLine={false} axisLine={false} />
                <YAxis stroke="#888888" fontSize={12} tickLine={false} axisLine={false} />
                <Tooltip content={<CustomTooltip />} />
                <Area type="monotone" dataKey="pnl" stroke="#22d3ee" strokeWidth={2} fillOpacity={1} fill="url(#colorPnl)" />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </div>
        
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div>
              <h3 className="text-lg font-semibold mb-2 text-gray-100">Factor Returns</h3>
               <div className="h-72 w-full">
                  <ResponsiveContainer>
                    <BarChart data={data.factorReturns} layout="vertical" margin={{ top: 5, right: 20, left: 10, bottom: 5 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#3c3c3c" horizontal={false} />
                      <XAxis type="number" stroke="#888888" fontSize={12} />
                      <YAxis type="category" dataKey="factor" stroke="#888888" fontSize={12} width={80} />
                      <Tooltip cursor={{fill: '#2d2d2d'}} content={<CustomTooltip />} />
                      <Bar dataKey="returns" fill="#06b6d4" />
                    </BarChart>
                  </ResponsiveContainer>
               </div>
            </div>
             <div>
              <h3 className="text-lg font-semibold mb-2 text-gray-100">SHAP Summary</h3>
              <div className="bg-gray-800 rounded-lg p-2 aspect-video flex items-center justify-center">
                 <img src={data.shapImageUrl} alt="SHAP Summary Plot" className="max-w-full max-h-full object-contain rounded" />
              </div>
            </div>
        </div>

      </div>
    </Card>
  );
};

export default BacktestReport;
