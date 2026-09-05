package main

import (
	"net/http"
)

// GET /internal/reconcile 账本 vs 充提单。
func (s *server) handleReconcile(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var mismatch int64
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM v_balance_mismatch`).Scan(&mismatch); err != nil {
		writeErr(w, http.StatusInternalServerError, 10001, "internal error")
		return
	}
	var assets, claims int64
	_ = s.pool.QueryRow(ctx, `SELECT COALESCE(SUM(balance),0) FROM accounts WHERE owner_type='system'`).Scan(&assets)
	_ = s.pool.QueryRow(ctx, `SELECT COALESCE(SUM(balance),0) FROM accounts WHERE owner_type<>'system'`).Scan(&claims)
	var orphanDep, orphanWd int64
	_ = s.pool.QueryRow(ctx, `
		SELECT count(*) FROM deposits d
		WHERE d.status='credited' AND NOT EXISTS (
			SELECT 1 FROM journals j
			WHERE j.biz_type='deposit' AND j.biz_id='onchain-'||d.txid||'-'||d.output_index::text
		)`).Scan(&orphanDep)
	_ = s.pool.QueryRow(ctx, `
		SELECT count(*) FROM withdrawals w
		WHERE w.status IN ('broadcasting','completed') AND NOT EXISTS (
			SELECT 1 FROM journals j WHERE j.biz_type='withdraw' AND j.biz_id='wd-'||w.id::text
		)`).Scan(&orphanWd)
	ok := mismatch == 0 && assets == claims && orphanDep == 0 && orphanWd == 0
	writeData(w, map[string]any{
		"ok": ok, "mismatchRows": mismatch, "assets": assets, "claims": claims,
		"orphanDeposits": orphanDep, "orphanWithdrawals": orphanWd,
	})
}
