package httpapi

import (
	_ "embed"
	"encoding/json"
	"net/http"
	"strconv"
)

//go:embed dashboard.html
var dashboardHTML []byte

func (h *Handler) getDashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(dashboardHTML)
}

func (h *Handler) getSystemSummary(w http.ResponseWriter, r *http.Request) {
	sum, err := h.svc.Summary(r.Context())
	if err != nil {
		h.log.Error("summary failed", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to get summary")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(sum)
}

func (h *Handler) getRecentCalls(w http.ResponseWriter, r *http.Request) {
	limit := 25
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 && parsed <= 100 {
			limit = parsed
		}
	}
	calls, err := h.svc.RecentCalls(r.Context(), limit)
	if err != nil {
		h.log.Error("recent calls failed", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to get recent calls")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"calls": calls,
	})
}
