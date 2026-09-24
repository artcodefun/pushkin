package domain

import "testing"

func TestPushPayloadDefensivelyCopiesData(t *testing.T) {
	t.Parallel()

	input := map[string]string{"kind": "original"}
	payload, err := NewPushPayload("title", "body", "https://example.com/image.png", input)
	if err != nil {
		t.Fatalf("new payload: %v", err)
	}

	input["kind"] = "changed"
	output := payload.Data()
	output["kind"] = "also-changed"

	if got := payload.Data()["kind"]; got != "original" {
		t.Fatalf("payload data was mutated: %q", got)
	}
}
