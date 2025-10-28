
import React, { useState } from 'react';
import Card from './ui/Card';

const initialConfig = `
# config/example_nasdaq100.yaml
universe: NASDAQ100
rebalance_freq: weekly
costs_bps_per_side: 5.0
min_median_dollar_vol: 10000000.0
min_price: 5.0
pit_lag_days: 2
start_date: '2018-01-01'
end_date: '2023-12-31'
long_short_quantiles: 0.2
sector_max_weight: 0.3
single_name_max_weight: 0.05
gross_leverage: 1.0
model:
  type: "lightgbm_regressor"
  params:
    objective: "regression_l1"
    metric: "rmse"
    n_estimators: 200
    learning_rate: 0.05
    num_leaves: 31
targets:
  - next_21d_excess_vs_sector
`;

const ConfigEditor: React.FC = () => {
  const [config, setConfig] = useState(initialConfig.trim());

  return (
    <Card title="Strategy Configuration" subtitle="config/example_nasdaq100.yaml">
      <div className="h-96 bg-gray-900 rounded-md p-1">
        <textarea
          value={config}
          onChange={(e) => setConfig(e.target.value)}
          className="w-full h-full bg-transparent text-gray-300 font-mono text-sm border-0 resize-none focus:ring-0 p-3"
          spellCheck="false"
        />
      </div>
       <div className="flex justify-end mt-4">
          <button className="px-4 py-2 bg-cyan-600 hover:bg-cyan-700 rounded-md text-white font-semibold text-sm transition-colors focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-offset-gray-800 focus:ring-cyan-500">
            Save Changes
          </button>
        </div>
    </Card>
  );
};

// FIX: Removed multiple default exports.
export default ConfigEditor;
