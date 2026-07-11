package store

import (
	"testing"

	"unoarena/services/analytics/domain"
)

func TestMarshalDurableOutcome_StableAcrossCalls(t *testing.T) {
	out := domain.ApplyOutcome{
		Kind:    domain.OutcomeQuarantined,
		EventID: "e1",
		Rejection: &domain.Rejection{
			Code: domain.RejectForbiddenField, Message: "forbidden private field: hand",
		},
		Facts: []domain.Fact{{
			Name: domain.FactProjectionEventQuarantined,
			Data: map[string]string{"z": "1", "a": "2", "eventId": "e1", "code": "forbidden_field"},
		}},
	}
	a, err := marshalDurableOutcome(out)
	if err != nil {
		t.Fatal(err)
	}
	b, err := marshalDurableOutcome(out)
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatalf("not byte-stable:\n%s\n%s", a, b)
	}
	parsed, err := unmarshalDurableOutcome(a)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Kind != domain.OutcomeQuarantined || parsed.Rejection == nil || parsed.Rejection.Code != domain.RejectForbiddenField {
		t.Fatalf("parsed=%+v", parsed)
	}
}
