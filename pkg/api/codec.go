package api

import (
	"bytes"
	"encoding/json"
)

// KnownTopLevelFields lists top-level fields this build understands.
var KnownTopLevelFields = map[string]struct{}{
	"kind": {}, "apiVersion": {}, "name": {}, "generation": {},
	"spec": {}, "status": {}, "finalizers": {}, "deletionTimestamp": {},
}

// DecodeResource parses raw JSON into a Resource while preserving
// unknown top-level fields in Resource.Unknown. Unknown fields never
// cause a decode failure and are never silently dropped: forward
// compatibility is explicit rather than accidental.
func DecodeResource(raw []byte) (*Resource, error) {
	// First pass: decode everything into a loose map so that unknown
	// fields can be captured before typed decoding ignores them.
	var loose map[string]json.RawMessage
	if err := json.Unmarshal(raw, &loose); err != nil {
		return nil, err
	}

	// Second pass: strict typing for the fields we know. json.Unmarshal on
	// the struct silently ignores absent/extra fields, which is exactly
	// what we want: an old version object missing a new field still works.
	var r Resource
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, err
	}

	unknown := map[string]any{}
	for k, v := range loose {
		if _, known := KnownTopLevelFields[k]; known {
			continue
		}
		var val any
		dec := json.NewDecoder(bytes.NewReader(v))
		dec.UseNumber() // preserve number precision instead of float64
		if err := dec.Decode(&val); err != nil {
			return nil, err
		}
		unknown[k] = val
	}
	if len(unknown) > 0 {
		r.Unknown = unknown
	}
	return &r, nil
}

// EncodeResource serializes a Resource including its unknown fields,
// so a round trip through this build does not erase data it does not
// understand.
func EncodeResource(r *Resource) ([]byte, error) {
	type alias Resource // avoid recursion
	known, err := json.Marshal((*alias)(r))
	if err != nil {
		return nil, err
	}
	if len(r.Unknown) == 0 {
		return known, nil
	}

	var merged map[string]json.RawMessage
	if err := json.Unmarshal(known, &merged); err != nil {
		return nil, err
	}
	for k, v := range r.Unknown {
		raw, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		merged[k] = raw
	}
	return json.Marshal(merged)
}
