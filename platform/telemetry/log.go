package telemetry

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"
)

var eventPattern = regexp.MustCompile(`^[a-z][a-z0-9]*(?:_[a-z0-9]+)*$`)

var reservedLogFields = map[string]struct{}{
	"timestamp": {}, "level": {}, "service": {}, "component": {},
	"environment": {}, "version": {}, "instance_id": {},
	"trace_id": {}, "span_id": {}, "message": {},
}

type logIdentity struct {
	service, component, environment, version, instanceID string
}

type jsonHandler struct {
	identity logIdentity
	writer   io.Writer
	mu       *sync.Mutex
	attrs    []boundAttr
	groups   []string
}

type boundAttr struct {
	groups []string
	attr   slog.Attr
}

func newJSONHandler(c Config) slog.Handler {
	return &jsonHandler{
		identity: logIdentity{
			service: c.ServiceName, component: c.Component,
			environment: c.Environment, version: c.ServiceVersion,
			instanceID: c.InstanceID,
		},
		writer: c.Writer,
		mu:     &sync.Mutex{},
	}
}

func (h *jsonHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *jsonHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	clone := h.clone()
	for _, attr := range attrs {
		clone.attrs = append(clone.attrs, boundAttr{
			groups: append([]string(nil), clone.groups...),
			attr:   attr,
		})
	}
	return clone
}

func (h *jsonHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	if name == "err" {
		name = "error"
	}
	clone := h.clone()
	clone.groups = append(clone.groups, name)
	return clone
}

func (h *jsonHandler) clone() *jsonHandler {
	clone := *h
	clone.attrs = append([]boundAttr(nil), h.attrs...)
	clone.groups = append([]string(nil), h.groups...)
	return &clone
}

func (h *jsonHandler) Handle(ctx context.Context, record slog.Record) error {
	timestamp := record.Time
	if timestamp.IsZero() {
		timestamp = time.Now()
	}
	event := "log"
	attributes := make(map[string]any)
	for _, bound := range h.attrs {
		if err := addLogAttr(attributes, bound.groups, bound.attr, &event); err != nil {
			return err
		}
	}
	var attrErr error
	record.Attrs(func(attr slog.Attr) bool {
		if err := addLogAttr(attributes, h.groups, attr, &event); err != nil {
			attrErr = err
			return false
		}
		return true
	})
	if attrErr != nil {
		return attrErr
	}

	fields := []orderedField{
		{"timestamp", timestamp.UTC().Format(time.RFC3339Nano)},
		{"level", strings.ToLower(record.Level.String())},
		{"service", h.identity.service},
		{"component", h.identity.component},
		{"environment", h.identity.environment},
		{"version", h.identity.version},
		{"instance_id", h.identity.instanceID},
		{"event", event},
	}
	if record.Message != "" {
		fields = append(fields, orderedField{"message", record.Message})
	}
	spanContext := trace.SpanContextFromContext(ctx)
	if spanContext.IsValid() {
		fields = append(fields,
			orderedField{"trace_id", spanContext.TraceID().String()},
			orderedField{"span_id", spanContext.SpanID().String()},
		)
	}
	keys := make([]string, 0, len(attributes))
	for key := range attributes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		fields = append(fields, orderedField{key, attributes[key]})
	}
	line, err := marshalOrdered(fields)
	if err != nil {
		return fmt.Errorf("encode structured log: %w", err)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, err := h.writer.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("write structured log: %w", err)
	}
	return nil
}

type orderedField struct {
	key   string
	value any
}

func marshalOrdered(fields []orderedField) ([]byte, error) {
	var output strings.Builder
	output.WriteByte('{')
	for index, field := range fields {
		key, err := json.Marshal(field.key)
		if err != nil {
			return nil, err
		}
		value, err := json.Marshal(field.value)
		if err != nil {
			return nil, err
		}
		if index > 0 {
			output.WriteByte(',')
		}
		output.Write(key)
		output.WriteByte(':')
		output.Write(value)
	}
	output.WriteByte('}')
	return []byte(output.String()), nil
}

func addLogAttr(root map[string]any, groups []string, attr slog.Attr, event *string) error {
	attr.Value = attr.Value.Resolve()
	if attr.Equal(slog.Attr{}) {
		return nil
	}
	if attr.Key == "err" {
		attr.Key = "error"
	}
	if len(groups) > 0 {
		if _, reserved := reservedLogFields[groups[0]]; reserved || groups[0] == "event" {
			return nil
		}
	}
	if len(groups) == 0 && attr.Key == "event" {
		value := attr.Value.String()
		if !eventPattern.MatchString(value) {
			return fmt.Errorf("event must be bounded lower_snake_case: %q", value)
		}
		*event = value
		return nil
	}
	if len(groups) == 0 {
		if _, reserved := reservedLogFields[attr.Key]; reserved {
			return nil
		}
	}
	container := root
	for _, group := range groups {
		if group == "" {
			continue
		}
		next, ok := container[group].(map[string]any)
		if !ok {
			next = make(map[string]any)
			container[group] = next
		}
		container = next
	}
	if attr.Value.Kind() == slog.KindGroup {
		for _, child := range attr.Value.Group() {
			if err := addLogAttr(container, []string{attr.Key}, child, event); err != nil {
				return err
			}
		}
		return nil
	}
	container[attr.Key] = slogValue(attr.Value)
	return nil
}

func slogValue(value slog.Value) any {
	switch value.Kind() {
	case slog.KindAny:
		if err, ok := value.Any().(error); ok {
			return err.Error()
		}
		return value.Any()
	case slog.KindBool:
		return value.Bool()
	case slog.KindDuration:
		return value.Duration().String()
	case slog.KindFloat64:
		return value.Float64()
	case slog.KindInt64:
		return value.Int64()
	case slog.KindString:
		return value.String()
	case slog.KindTime:
		return value.Time().UTC().Format(time.RFC3339Nano)
	case slog.KindUint64:
		return value.Uint64()
	default:
		return value.Any()
	}
}

var _ slog.Handler = (*jsonHandler)(nil)
