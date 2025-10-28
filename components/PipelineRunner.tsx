
import React from 'react';
import { PipelineStatus, PipelineJob } from '../types';
import Card from './ui/Card';

const PlayIcon = () => <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" className="w-5 h-5"><path d="M6.3 2.841A1.5 1.5 0 004 4.11V15.89a1.5 1.5 0 002.3 1.269l9.344-5.89a1.5 1.5 0 000-2.538L6.3 2.84z" /></svg>;
const CheckCircleIcon = () => <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" className="w-5 h-5 text-green-400"><path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.857-9.809a.75.75 0 00-1.214-.882l-3.483 4.79-1.88-1.88a.75.75 0 10-1.06 1.061l2.5 2.5a.75.75 0 001.137-.089l4-5.5z" clipRule="evenodd" /></svg>;
const XCircleIcon = () => <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" className="w-5 h-5 text-red-400"><path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.28 7.22a.75.75 0 00-1.06 1.06L8.94 10l-1.72 1.72a.75.75 0 101.06 1.06L10 11.06l1.72 1.72a.75.75 0 101.06-1.06L11.06 10l1.72-1.72a.75.75 0 00-1.06-1.06L10 8.94 8.28 7.22z" clipRule="evenodd" /></svg>;
const Spinner = () => <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-gray-100"></div>;

const JobStatus: React.FC<{ status: 'idle' | 'running' | 'completed' | 'failed' }> = ({ status }) => {
  switch (status) {
    case 'completed': return <CheckCircleIcon />;
    case 'failed': return <XCircleIcon />;
    case 'running': return <Spinner />;
    default: return null;
  }
};

interface PipelineRunnerProps {
  status: PipelineStatus;
  onRun: (job: PipelineJob) => void;
  isKeySet: boolean;
}

const PipelineRunner: React.FC<PipelineRunnerProps> = ({ status, onRun, isKeySet }) => {
  const jobs: { id: PipelineJob; name: string; description: string }[] = [
    { id: 'ingest', name: 'Ingest Data', description: 'Fetch EODHD prices & fundamentals.' },
    { id: 'train', name: 'Train Model', description: 'Train ranking model with walk-forward CV.' },
    { id: 'backtest', name: 'Run Backtest', description: 'Simulate strategy with cost modeling.' },
    { id: 'report', name: 'Generate Report', description: 'Create HTML report with SHAP & plots.' },
  ];

  const getButtonState = (jobId: PipelineJob): { disabled: boolean; tooltip: string } => {
    if (status[jobId] === 'running') return { disabled: true, tooltip: 'Job is running...' };

    switch (jobId) {
      case 'ingest':
        return isKeySet
          ? { disabled: false, tooltip: 'Run data ingestion' }
          : { disabled: true, tooltip: 'Set EODHD API Key to enable' };
      case 'train':
        return status.ingest === 'completed'
          ? { disabled: false, tooltip: 'Run model training' }
          : { disabled: true, tooltip: 'Requires successful data ingestion' };
      case 'backtest':
        return status.train === 'completed'
          ? { disabled: false, tooltip: 'Run backtest' }
          : { disabled: true, tooltip: 'Requires successful model training' };
      case 'report':
        return status.backtest === 'completed'
          ? { disabled: false, tooltip: 'Generate final report' }
          : { disabled: true, tooltip: 'Requires successful backtest' };
      default:
        return { disabled: true, tooltip: '' };
    }
  };

  return (
    <Card title="Pipeline Runner" subtitle="Execute jobs sequentially">
      <div className="space-y-4">
        {jobs.map((job, index) => {
          const { disabled, tooltip } = getButtonState(job.id);
          return (
            <div key={job.id} className="flex items-center justify-between p-3 bg-gray-700/50 rounded-lg">
              <div className="flex items-center">
                <div className="flex-shrink-0 h-8 w-8 rounded-full bg-gray-600 flex items-center justify-center text-sm font-bold text-cyan-400">
                  {index + 1}
                </div>
                <div className="ml-4">
                  <p className="text-sm font-medium text-gray-100">{job.name}</p>
                  <p className="text-xs text-gray-400">{job.description}</p>
                </div>
              </div>
              <div className="flex items-center space-x-3">
                <JobStatus status={status[job.id]} />
                <button
                  onClick={() => onRun(job.id)}
                  disabled={disabled}
                  title={tooltip}
                  className="p-2 rounded-full bg-gray-600 text-gray-200 hover:bg-cyan-600 disabled:bg-gray-700 disabled:text-gray-500 disabled:cursor-not-allowed transition-colors"
                  aria-label={tooltip}
                >
                  <PlayIcon />
                </button>
              </div>
            </div>
          )
        })}
      </div>
    </Card>
  );
};

export default PipelineRunner;
