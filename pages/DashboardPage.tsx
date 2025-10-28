
import React, { useState } from 'react';
import ApiConfig from '../components/ApiConfig';
import ConfigEditor from '../components/ConfigEditor';
import PipelineRunner from '../components/PipelineRunner';
import BacktestReport from '../components/BacktestReport';
import { PipelineJob, PipelineStatus } from '../types';

const DashboardPage: React.FC = () => {
  const [status, setStatus] = useState<PipelineStatus>({
    ingest: 'idle',
    train: 'idle',
    backtest: 'idle',
    report: 'idle',
  });
  const [showReport, setShowReport] = useState(false);
  const [isKeySet, setIsKeySet] = useState(false);

  const handleRun = (job: PipelineJob) => {
    setStatus(prev => ({ ...prev, [job]: 'running' }));
    
    // Simulate job execution with more deterministic logic
    setTimeout(() => {
      let success = false;
      switch(job) {
        case 'ingest':
          success = isKeySet; // Ingest only succeeds if the key is set
          break;
        case 'train':
          // Train has a chance of failure to demonstrate the error state
          success = status.ingest === 'completed' && Math.random() > 0.2; // 80% success
          break;
        case 'backtest':
          success = status.train === 'completed';
          break;
        case 'report':
          success = status.backtest === 'completed';
          break;
      }

      setStatus(prev => ({ ...prev, [job]: success ? 'completed' : 'failed' }));

      if (job === 'report' && success) {
        setShowReport(true);
      }
    }, 1500 + Math.random() * 1500);
  };

  return (
    <div className="space-y-8">
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
        <div className="lg:col-span-1 space-y-8">
          <ApiConfig isKeySet={isKeySet} onSetKey={() => setIsKeySet(true)} />
          <ConfigEditor />
          <PipelineRunner status={status} onRun={handleRun} isKeySet={isKeySet} />
        </div>
        <div className="lg:col-span-2">
          <BacktestReport isVisible={showReport} />
        </div>
      </div>
    </div>
  );
};

export default DashboardPage;
