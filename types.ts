
export interface PnlDataPoint {
  date: string;
  pnl: number;
}

export interface Metric {
  label: string;
  value: string;
  change?: string;
  changeType?: 'positive' | 'negative';
}

export interface BacktestReportData {
  pnlData: PnlDataPoint[];
  metrics: Metric[];
  turnover: { date: string; value: number }[];
  shapImageUrl: string;
  factorReturns: { factor: string; returns: number }[];
}

export interface PipelineStatus {
  ingest: 'idle' | 'running' | 'completed' | 'failed';
  train: 'idle' | 'running' | 'completed' | 'failed';
  backtest: 'idle' | 'running' | 'completed' | 'failed';
  report: 'idle' | 'running' | 'completed' | 'failed';
}

export type PipelineJob = keyof PipelineStatus;

export interface ChatMessage {
  id: string;
  role: 'user' | 'model';
  text: string;
}
