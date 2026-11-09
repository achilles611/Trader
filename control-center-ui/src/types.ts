export type ControlState = { state: string; entries_allowed: boolean; paper_only: boolean; updated_at?: string; note?: string };
export type Candidate = {
  wallet: string; label?: string; operator_state: string; research_state: string; score?: number | null; qualified: boolean;
  analysis_timestamp?: string | null; stale_analysis: boolean; last_active?: string | null; campaigns?: number | null;
  target_net_pnl?: number | null; follower_net_pnl?: number | null; win_rate?: number | null; profit_factor?: number | null;
  target_max_drawdown?: number | null; follower_max_drawdown?: number | null; copyability?: number | string | null;
  coverage?: string; source_count?: number; concentration?: number | null; score_reasons?: string[];
};
export type CandidatesResponse = { items: Candidate[]; page: number; page_size: number; total: number; pages: number };
export type Portfolio = Record<string, any>;
