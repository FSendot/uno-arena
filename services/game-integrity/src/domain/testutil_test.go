package domain

import "testing"

func assertRejected(t *testing.T, out CommandOutcome, code RejectionCode) {
	t.Helper()
	if out.Kind != OutcomeRejected {
		t.Fatalf("kind=%s want rejected; out=%+v", out.Kind, out)
	}
	if out.Rejection == nil || out.Rejection.Code != code {
		t.Fatalf("rejection=%+v want %s", out.Rejection, code)
	}
	if len(out.Facts) != 0 {
		t.Fatalf("rejected outcome must not carry facts")
	}
}
