package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/EmptyHeart5292/lcex/internal/fixed"
	"github.com/EmptyHeart5292/lcex/internal/ledger"
)

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]bool{"ok": true})
	})
	mux.HandleFunc("GET /api/v1/deposit-addresses", s.handleDepositAddress)
	mux.HandleFunc("GET /api/v1/deposits", s.handleListDeposits)
	mux.HandleFunc("GET /api/v1/withdrawals", s.handleListWithdrawals)
	mux.HandleFunc("POST /api/v1/withdrawals", s.handleWithdraw)
	mux.HandleFunc("GET /api/v1/account/balances", s.handleBalances)
	mux.HandleFunc("POST /internal/deposits", s.handleAdminCredit)
	mux.HandleFunc("POST /internal/chain/deposits", s.handleChainDeposit)
	mux.HandleFunc("POST /internal/chain/withdrawals/{id}/confirm", s.handleWithdrawConfirm)
	return mux
}

func parseUserID(r *http.Request) (int64, error) {
	v := r.Header.Get("X-User-Id")
	if v == "" {
		return 0, fmt.Errorf("missing X-User-Id")
	}
	id, err := strconv.ParseInt(v, 10, 64)
	if err != nil || id <= 0 {
		return 0, fmt.Errorf("invalid X-User-Id")
	}
	return id, nil
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func writeData(w http.ResponseWriter, data any) {
	writeJSON(w, map[string]any{"code": 0, "data": data})
}

func writeErr(w http.ResponseWriter, httpStatus, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	_ = json.NewEncoder(w).Encode(map[string]any{"code": code, "message": msg, "data": nil})
}

func (s *server) handleDepositAddress(w http.ResponseWriter, r *http.Request) {
	userID, err := parseUserID(r)
	if err != nil {
		writeErr(w, http.StatusUnauthorized, 20005, err.Error())
		return
	}
	currency := r.URL.Query().Get("currency")
	network := r.URL.Query().Get("network")
	ch, err := s.chains.Chain(currency, network)
	if err != nil {
		writeErr(w, http.StatusBadRequest, 60003, err.Error())
		return
	}
	if !ch.DepositOn {
		writeErr(w, http.StatusBadRequest, 60003, "deposit disabled")
		return
	}
	addr := s.provider.DeriveAddress(userID, currency, network)
	_, err = s.pool.Exec(r.Context(), `
		INSERT INTO deposit_addresses (user_id, currency, network, address)
		VALUES ($1,$2,$3,$4)
		ON CONFLICT (user_id, currency, network) DO NOTHING`,
		userID, currency, network, addr)
	if err != nil {
		s.log.Error("alloc address", "err", err)
		writeErr(w, http.StatusInternalServerError, 10001, "internal error")
		return
	}
	err = s.pool.QueryRow(r.Context(), `
		SELECT address FROM deposit_addresses WHERE user_id=$1 AND currency=$2 AND network=$3`,
		userID, currency, network).Scan(&addr)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, 10001, "internal error")
		return
	}
	writeData(w, map[string]any{
		"currency": currency, "network": network, "address": addr,
		"confirmations": ch.Confirmations, "minDeposit": fixed.Format(ch.MinDeposit),
	})
}

type adminCreditReq struct {
	UserID   int64  `json:"userId"`
	Currency string `json:"currency"`
	Amount   string `json:"amount"`
	BizID    string `json:"bizId"`
}

func (s *server) handleAdminCredit(w http.ResponseWriter, r *http.Request) {
	var req adminCreditReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, 10003, "invalid json body")
		return
	}
	if req.UserID <= 0 || req.Currency == "" || req.BizID == "" {
		writeErr(w, http.StatusBadRequest, 10003, "userId, currency, bizId required")
		return
	}
	amt, err := fixed.Parse(req.Amount)
	if err != nil || amt == 0 {
		writeErr(w, http.StatusBadRequest, 10003, "invalid amount")
		return
	}
	replayed, err := s.creditUser(r.Context(), req.UserID, req.Currency, int64(amt), req.BizID)
	if err != nil {
		s.log.Error("admin credit", "err", err)
		writeErr(w, http.StatusInternalServerError, 10001, "internal error")
		return
	}
	writeData(w, map[string]any{"bizId": req.BizID, "replayed": replayed})
}

func (s *server) creditUser(ctx context.Context, userID int64, currency string, amt int64, bizID string) (bool, error) {
	err := s.ledger.Post(ctx, "deposit", bizID, []ledger.Move{
		{Account: ledger.System(0, currency, "available"), Delta: amt},
		{Account: ledger.User(userID, currency, "available"), Delta: amt},
	})
	if errors.Is(err, ledger.ErrAlreadyProcessed) {
		return true, nil
	}
	return false, err
}

type chainDepositReq struct {
	Address       string `json:"address"`
	Txid          string `json:"txid"`
	OutputIndex   int    `json:"outputIndex"`
	Amount        string `json:"amount"`
	Confirmations int    `json:"confirmations"`
}

func (s *server) handleChainDeposit(w http.ResponseWriter, r *http.Request) {
	var req chainDepositReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, 10003, "invalid json body")
		return
	}
	if req.Address == "" || req.Txid == "" {
		writeErr(w, http.StatusBadRequest, 10003, "address and txid required")
		return
	}
	amt, err := fixed.Parse(req.Amount)
	if err != nil || amt == 0 {
		writeErr(w, http.StatusBadRequest, 10003, "invalid amount")
		return
	}
	var userID int64
	var currency, network string
	err = s.pool.QueryRow(r.Context(), `
		SELECT user_id, currency, network FROM deposit_addresses WHERE address=$1`,
		req.Address).Scan(&userID, &currency, &network)
	if errors.Is(err, pgx.ErrNoRows) {
		writeErr(w, http.StatusBadRequest, 10003, "unknown deposit address")
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, 10001, "internal error")
		return
	}
	ch, err := s.chains.Chain(currency, network)
	if err != nil {
		writeErr(w, http.StatusBadRequest, 60003, err.Error())
		return
	}
	if amt < ch.MinDeposit {
		writeErr(w, http.StatusBadRequest, 10003, "below min deposit")
		return
	}

	var id int64
	var status string
	var conf int
	err = s.pool.QueryRow(r.Context(), `
		INSERT INTO deposits (user_id, currency, network, address, amount, txid, output_index, confirmations, required_conf, status)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'pending')
		ON CONFLICT (txid, output_index) DO UPDATE SET confirmations = EXCLUDED.confirmations
		RETURNING id, status, confirmations`,
		userID, currency, network, req.Address, int64(amt), req.Txid, req.OutputIndex, req.Confirmations, ch.Confirmations,
	).Scan(&id, &status, &conf)
	if err != nil {
		s.log.Error("upsert deposit", "err", err)
		writeErr(w, http.StatusInternalServerError, 10001, "internal error")
		return
	}

	credited := status == "credited"
	if !credited && conf >= ch.Confirmations {
		bizID := fmt.Sprintf("onchain-%s-%d", req.Txid, req.OutputIndex)
		err = s.ledger.WithTx(r.Context(), func(tx pgx.Tx) error {
			err := s.ledger.PostTx(r.Context(), tx, "deposit", bizID, []ledger.Move{
				{Account: ledger.System(0, currency, "available"), Delta: int64(amt)},
				{Account: ledger.User(userID, currency, "available"), Delta: int64(amt)},
			})
			if err != nil && !errors.Is(err, ledger.ErrAlreadyProcessed) {
				return err
			}
			_, err = tx.Exec(r.Context(), `
				UPDATE deposits SET status='credited', credited_at=now() WHERE id=$1 AND status='pending'`, id)
			return err
		})
		if err != nil {
			s.log.Error("credit onchain deposit", "err", err)
			writeErr(w, http.StatusInternalServerError, 10001, "internal error")
			return
		}
		credited = true
		status = "credited"
	}
	writeData(w, map[string]any{
		"depositId": id, "status": status, "confirmations": conf,
		"required": ch.Confirmations, "credited": credited,
	})
}

type withdrawReq struct {
	Currency      string `json:"currency"`
	Network       string `json:"network"`
	Address       string `json:"address"`
	Amount        string `json:"amount"`
	ClientOrderID string `json:"clientOrderId"`
}

func (s *server) handleWithdraw(w http.ResponseWriter, r *http.Request) {
	userID, err := parseUserID(r)
	if err != nil {
		writeErr(w, http.StatusUnauthorized, 20005, err.Error())
		return
	}
	var req withdrawReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, 10003, "invalid json body")
		return
	}
	ch, err := s.chains.Chain(req.Currency, req.Network)
	if err != nil {
		writeErr(w, http.StatusBadRequest, 60003, err.Error())
		return
	}
	if !ch.WithdrawOn {
		writeErr(w, http.StatusBadRequest, 60004, "withdrawal suspended")
		return
	}
	if req.Address == "" {
		writeErr(w, http.StatusBadRequest, 10003, "address required")
		return
	}
	amt, err := fixed.Parse(req.Amount)
	if err != nil || amt == 0 {
		writeErr(w, http.StatusBadRequest, 10003, "invalid amount")
		return
	}
	if amt < ch.MinWithdraw {
		writeErr(w, http.StatusBadRequest, 60001, "amount below minimum withdrawal")
		return
	}
	clientID := req.ClientOrderID
	if clientID == "" {
		clientID = fmt.Sprintf("wd-%d-%d", userID, time.Now().UnixNano())
	}

	var wdID int64
	err = s.ledger.WithTx(r.Context(), func(tx pgx.Tx) error {
		err := tx.QueryRow(r.Context(), `
			INSERT INTO withdrawals (user_id, currency, network, address, amount, fee, client_order_id, status)
			VALUES ($1,$2,$3,$4,$5,$6,$7,'broadcasting')
			RETURNING id`,
			userID, req.Currency, req.Network, req.Address, int64(amt), int64(ch.WithdrawFee), clientID,
		).Scan(&wdID)
		if err != nil {
			return err
		}
		total := int64(amt + ch.WithdrawFee)
		return s.ledger.PostTx(r.Context(), tx, "withdraw", fmt.Sprintf("wd-%d", wdID), []ledger.Move{
			{Account: ledger.User(userID, req.Currency, "available"), Delta: -total},
			{Account: ledger.System(0, req.Currency, "available"), Delta: -int64(amt)},
			{Account: ledger.Fee(0, req.Currency, "available"), Delta: int64(ch.WithdrawFee)},
		})
	})
	if errors.Is(err, ledger.ErrInsufficientBalance) {
		writeErr(w, http.StatusBadRequest, 51001, "insufficient balance")
		return
	}
	if err != nil {
		s.log.Error("withdraw", "err", err)
		writeErr(w, http.StatusInternalServerError, 10001, "internal error")
		return
	}
	writeData(w, map[string]any{
		"withdrawalId": wdID, "status": "broadcasting",
		"amount": fixed.Format(amt), "fee": fixed.Format(ch.WithdrawFee),
	})
}

func (s *server) handleWithdrawConfirm(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeErr(w, http.StatusBadRequest, 10003, "invalid id")
		return
	}
	var body struct {
		Txid string `json:"txid"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	if body.Txid == "" {
		body.Txid = fmt.Sprintf("mocktxid-%d", id)
	}
	tag, err := s.pool.Exec(r.Context(), `
		UPDATE withdrawals SET status='completed', txid=$1, completed_at=now()
		WHERE id=$2 AND status='broadcasting'`, body.Txid, id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, 10001, "internal error")
		return
	}
	if tag.RowsAffected() == 0 {
		writeErr(w, http.StatusBadRequest, 10003, "withdrawal not confirmable")
		return
	}
	writeData(w, map[string]any{"withdrawalId": id, "status": "completed", "txid": body.Txid})
}

func (s *server) handleListDeposits(w http.ResponseWriter, r *http.Request) {
	userID, err := parseUserID(r)
	if err != nil {
		writeErr(w, http.StatusUnauthorized, 20005, err.Error())
		return
	}
	rows, err := s.pool.Query(r.Context(), `
		SELECT id, currency, network, amount, txid, output_index, confirmations, status, created_at
		FROM deposits WHERE user_id=$1 ORDER BY id DESC LIMIT 50`, userID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, 10001, "internal error")
		return
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var id int64
		var cur, net, txid, st string
		var amt int64
		var idx, conf int
		var ts time.Time
		if err := rows.Scan(&id, &cur, &net, &amt, &txid, &idx, &conf, &st, &ts); err != nil {
			writeErr(w, http.StatusInternalServerError, 10001, "internal error")
			return
		}
		out = append(out, map[string]any{
			"depositId": id, "currency": cur, "network": net, "amount": fixed.Format(uint64(amt)),
			"txid": txid, "outputIndex": idx, "confirmations": conf, "status": st,
			"createdAt": ts.UTC().Format(time.RFC3339Nano),
		})
	}
	if out == nil {
		out = []map[string]any{}
	}
	writeData(w, out)
}

func (s *server) handleListWithdrawals(w http.ResponseWriter, r *http.Request) {
	userID, err := parseUserID(r)
	if err != nil {
		writeErr(w, http.StatusUnauthorized, 20005, err.Error())
		return
	}
	rows, err := s.pool.Query(r.Context(), `
		SELECT id, currency, network, address, amount, fee, COALESCE(txid,''), status, created_at
		FROM withdrawals WHERE user_id=$1 ORDER BY id DESC LIMIT 50`, userID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, 10001, "internal error")
		return
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var id int64
		var cur, net, addr, txid, st string
		var amt, fee int64
		var ts time.Time
		if err := rows.Scan(&id, &cur, &net, &addr, &amt, &fee, &txid, &st, &ts); err != nil {
			writeErr(w, http.StatusInternalServerError, 10001, "internal error")
			return
		}
		out = append(out, map[string]any{
			"withdrawalId": id, "currency": cur, "network": net, "address": addr,
			"amount": fixed.Format(uint64(amt)), "fee": fixed.Format(uint64(fee)),
			"txid": txid, "status": st, "createdAt": ts.UTC().Format(time.RFC3339Nano),
		})
	}
	if out == nil {
		out = []map[string]any{}
	}
	writeData(w, out)
}

func (s *server) handleBalances(w http.ResponseWriter, r *http.Request) {
	userID, err := parseUserID(r)
	if err != nil {
		writeErr(w, http.StatusUnauthorized, 20005, err.Error())
		return
	}
	b, err := s.ledger.UserBalances(r.Context(), userID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, 10001, "internal error")
		return
	}
	writeData(w, b)
}
