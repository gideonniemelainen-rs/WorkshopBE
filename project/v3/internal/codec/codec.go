package codec

import (
	"context"
	"fmt"
	"log"
	"time"

	recentchangev1 "wikistream_project/internal/gen/recentchange/v1"
	"wikistream_project/internal/models"

	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/proto"
)

const (
	lookupAttempts = 20
	lookupBackoff  = 2 * time.Second
)

// Have also never heard of "serde" as a combo term for serialize/deserialize, neat
type Codec struct {
	serde *sr.Serde
}

func New(schemaID int) *Codec {
	s := sr.NewSerde()
	s.Register(
		schemaID,
		&recentchangev1.RecentChange{},
		sr.EncodeFn(func(v any) ([]byte, error) {
			return proto.Marshal(v.(*recentchangev1.RecentChange))
		}),
		sr.DecodeFn(func(b []byte, v any) error {
			return proto.Unmarshal(b, v.(*recentchangev1.RecentChange))
		}),
		sr.GenerateFn(func() any { return new(recentchangev1.RecentChange) }),
		sr.Index(0),
	)
	return &Codec{serde: s}
}

// Handler for serializing recent change struct into protobuf format
func (c *Codec) EncodeEvent(rc models.RecentChange) ([]byte, error) {
	b, err := c.serde.Encode(toProto(rc))
	if err != nil {
		return nil, fmt.Errorf("encode event failure: %w", err)
	}
	return b, nil
}

func (c *Codec) DecodeEvent(b []byte) (models.RecentChange, error) {
	var msg recentchangev1.RecentChange
	if err := c.serde.Decode(b, &msg); err != nil {
		return models.RecentChange{}, fmt.Errorf("decode event failure: %w", err)
	}
	return fromProto(&msg), nil
}

// Helper for handling versioning of protobuf schema, as well as implicit validation that it is in fact a protobuf schema, as opposed to something like Avro
func LookupSchemaID(ctx context.Context, registryURL string, subject string) (int, error) {
	cl, err := sr.NewClient(sr.URLs(registryURL))
	if err != nil {
		// This is where I learned that capitalizing an error string causes lint problems, neat
		return 0, fmt.Errorf("schema registry client: %w", err)
	}

	var lastError error
	for attempt := 1; attempt <= lookupAttempts; attempt++ {
		ss, err := cl.SchemaByVersion(ctx, subject, -1)
		if err == nil {
			if ss.Type != sr.TypeProtobuf {
				return 0, fmt.Errorf("subject %q has schema type %v, expected PROTOBUF", subject, ss.Type)
			}
			return ss.ID, nil
		}
		lastError = err
		log.Printf("schema lookup attempt %d/%d for %q failed: %v", attempt, lookupAttempts, subject, err)

		if attempt < lookupAttempts {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(lookupBackoff):
			}
		}
	}
	return 0, fmt.Errorf("lookup schema for subject %q after %d attempts: %w", subject, lookupAttempts, lastError)
}

func toProto(rc models.RecentChange) *recentchangev1.RecentChange {
	return &recentchangev1.RecentChange{
		Id:         rc.ID,
		Type:       rc.Type,
		Namespace:  rc.Namespace,
		Title:      rc.Title,
		User:       rc.User,
		Bot:        rc.Bot,
		Minor:      rc.Minor,
		ServerUrl:  rc.ServerURL,
		ServerName: rc.ServerName,
		Wiki:       rc.Wiki,
		Timestamp:  rc.Timestamp,
		Comment:    rc.Comment,
	}
}

// Side note, I love that the protobuf generator gives us all of the getters "for free"
func fromProto(msg *recentchangev1.RecentChange) models.RecentChange {
	return models.RecentChange{
		ID:         msg.GetId(),
		Type:       msg.GetType(),
		Namespace:  msg.GetNamespace(),
		Title:      msg.GetTitle(),
		User:       msg.GetUser(),
		Bot:        msg.GetBot(),
		Minor:      msg.GetMinor(),
		ServerURL:  msg.GetServerUrl(),
		ServerName: msg.GetServerName(),
		Wiki:       msg.GetWiki(),
		Timestamp:  msg.GetTimestamp(),
		Comment:    msg.GetComment(),
	}
}
