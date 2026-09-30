// Copyright 2025 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package elasticsearch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestIntegrationElasticsearch(t *testing.T) {
	integration.CheckSkip(t)

	ctx := t.Context()
	ctr, err := testcontainers.Run(t.Context(), "docker.elastic.co/elasticsearch/elasticsearch:9.1.7",
		testcontainers.WithExposedPorts("9200/tcp"),
		testcontainers.WithEnv(map[string]string{
			"discovery.type": "single-node",
			"cluster.routing.allocation.disk.threshold_enabled": "false",
			"xpack.security.enabled":                            "false",
			"ES_JAVA_OPTS":                                      "-Xms256m -Xmx256m",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForHTTP("/").WithPort("9200/tcp").WithStartupTimeout(time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	mappedPort, err := ctr.MappedPort(t.Context(), "9200/tcp")
	require.NoError(t, err)
	url := fmt.Sprintf("http://127.0.0.1:%v", mappedPort.Port())

	client, err := elasticsearch.NewTypedClient(elasticsearch.Config{
		Addresses: []string{url},
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		ok, err := client.Ping().Do(ctx)
		return err == nil && ok
	}, time.Second*30, time.Millisecond*500)

	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.AddOutputYAML(fmt.Sprintf(`
elasticsearch_v9:
  urls: ['%s']
  index: "things"
  action: ${! meta("action") }
  id: ${! meta("id") }
`, url)))

	inFunc, err := streamBuilder.AddProducerFunc()
	require.NoError(t, err)

	stream, err := streamBuilder.Build()
	require.NoError(t, err)

	go func() {
		if err := stream.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()
	defer func() {
		err := stream.StopWithin(time.Second * 3)
		require.NoError(t, err)
	}()

	t.Run("index", func(t *testing.T) {
		msgBytes := []byte(`{"message":"blobfish are cool","likes":1}`)
		msg := service.NewMessage(msgBytes)
		msg.MetaSet("action", "index")
		msg.MetaSet("id", "1")
		err = inFunc(ctx, msg)
		require.NoError(t, err)

		resp, err := client.Get("things", "1").Do(ctx)
		require.NoError(t, err)

		require.Equal(t, string(msgBytes), string(resp.Source_))
	})

	t.Run("update", func(t *testing.T) {
		msgBytes, err := json.Marshal(map[string]any{
			"script": map[string]any{
				"source": "ctx._source.likes += 1",
				"lang":   "painless",
			},
		})
		require.NoError(t, err)

		msg := service.NewMessage(msgBytes)
		msg.MetaSet("id", "1")
		msg.MetaSet("action", "update")
		err = inFunc(ctx, msg)
		require.NoError(t, err)

		resp, err := client.Get("things", "1").Do(ctx)
		require.NoError(t, err)

		require.Equal(t, `{"message":"blobfish are cool","likes":2}`, string(resp.Source_))
	})

	t.Run("delete", func(t *testing.T) {
		msg := service.NewMessage([]byte("{}"))
		msg.MetaSet("id", "1")
		msg.MetaSet("action", "delete")
		err = inFunc(ctx, msg)
		require.NoError(t, err)

		resp, err := client.Get("things", "1").Do(ctx)
		require.NoError(t, err)
		require.False(t, resp.Found)
	})

	t.Run("create", func(t *testing.T) {
		// Create a new document
		createMsgBytes := []byte(`{"message":"mantis shrimp are epic","likes":10}`)
		createMsg := service.NewMessage(createMsgBytes)
		createMsg.MetaSet("action", "create")
		createMsg.MetaSet("id", "2")
		err = inFunc(ctx, createMsg)
		require.NoError(t, err)

		resp, err := client.Get("things", "2").Do(ctx)
		require.NoError(t, err)
		require.True(t, resp.Found)
		require.Equal(t, string(createMsgBytes), string(resp.Source_))

		// Attempt to create the same document again (should fail)
		err = inFunc(ctx, createMsg)
		require.Error(t, err) // Expecting an error here

		// Verify the document was not overwritten
		resp, err = client.Get("things", "2").Do(ctx)
		require.NoError(t, err)
		require.True(t, resp.Found)
		require.Equal(t, string(createMsgBytes), string(resp.Source_))
	})

	t.Run("upsert", func(t *testing.T) {
		// Upsert a new document
		upsertNewMsgBytes := []byte(`{"message":"dragonflies are ancient","likes":5}`)
		upsertNewMsg := service.NewMessage(upsertNewMsgBytes)
		upsertNewMsg.MetaSet("action", "upsert")
		upsertNewMsg.MetaSet("id", "3")
		err = inFunc(ctx, upsertNewMsg)
		require.NoError(t, err)

		resp, err := client.Get("things", "3").Do(ctx)
		require.NoError(t, err)
		require.True(t, resp.Found)
		require.Equal(t, string(upsertNewMsgBytes), string(resp.Source_))

		// Upsert an existing document (update)
		upsertUpdateMsgBytes := []byte(`{"message":"dragonflies are truly ancient","likes":6}`)
		upsertUpdateMsg := service.NewMessage(upsertUpdateMsgBytes)
		upsertUpdateMsg.MetaSet("action", "upsert")
		upsertUpdateMsg.MetaSet("id", "3")
		err = inFunc(ctx, upsertUpdateMsg)
		require.NoError(t, err)

		resp, err = client.Get("things", "3").Do(ctx)
		require.NoError(t, err)
		require.True(t, resp.Found)
		require.Equal(t, string(upsertUpdateMsgBytes), string(resp.Source_))
	})
}

func TestIntegrationElasticsearchVersioning(t *testing.T) {
	integration.CheckSkip(t)

	ctx := t.Context()
	ctr, err := testcontainers.Run(t.Context(), "docker.elastic.co/elasticsearch/elasticsearch:9.1.7",
		testcontainers.WithExposedPorts("9200/tcp"),
		testcontainers.WithEnv(map[string]string{
			"discovery.type": "single-node",
			"cluster.routing.allocation.disk.threshold_enabled": "false",
			"xpack.security.enabled":                            "false",
			"ES_JAVA_OPTS":                                      "-Xms256m -Xmx256m",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForHTTP("/").WithPort("9200/tcp").WithStartupTimeout(time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	mappedPort, err := ctr.MappedPort(t.Context(), "9200/tcp")
	require.NoError(t, err)
	url := fmt.Sprintf("http://127.0.0.1:%v", mappedPort.Port())

	client, err := elasticsearch.NewTypedClient(elasticsearch.Config{
		Addresses: []string{url},
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		ok, err := client.Ping().Do(ctx)
		return err == nil && ok
	}, time.Second*30, time.Millisecond*500)

	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.AddOutputYAML(fmt.Sprintf(`
elasticsearch_v9:
  urls: ['%s']
  index: "versioned"
  action: ${! meta("action") }
  id: ${! meta("id") }
  version: ${! meta("version") }
`, url)))

	inFunc, err := streamBuilder.AddProducerFunc()
	require.NoError(t, err)

	stream, err := streamBuilder.Build()
	require.NoError(t, err)

	go func() {
		if err := stream.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()
	defer func() {
		err := stream.StopWithin(time.Second * 3)
		require.NoError(t, err)
	}()

	t.Run("newer_version_wins_and_older_is_noop", func(t *testing.T) {
		bodyA := []byte(`{"body":"A"}`)
		msgA := service.NewMessage(bodyA)
		msgA.MetaSet("action", "index")
		msgA.MetaSet("id", "x")
		msgA.MetaSet("version", "2")
		require.NoError(t, inFunc(ctx, msgA))

		resp, err := client.Get("versioned", "x").Do(ctx)
		require.NoError(t, err)
		require.Equal(t, string(bodyA), string(resp.Source_))

		// An older version is rejected by Elasticsearch with a 409, which this
		// output treats as a successful no-op rather than an error.
		bodyB := []byte(`{"body":"B"}`)
		msgB := service.NewMessage(bodyB)
		msgB.MetaSet("action", "index")
		msgB.MetaSet("id", "x")
		msgB.MetaSet("version", "1")
		require.NoError(t, inFunc(ctx, msgB))

		resp, err = client.Get("versioned", "x").Do(ctx)
		require.NoError(t, err)
		require.Equal(t, string(bodyA), string(resp.Source_))

		bodyC := []byte(`{"body":"C"}`)
		msgC := service.NewMessage(bodyC)
		msgC.MetaSet("action", "index")
		msgC.MetaSet("id", "x")
		msgC.MetaSet("version", "3")
		require.NoError(t, inFunc(ctx, msgC))

		resp, err = client.Get("versioned", "x").Do(ctx)
		require.NoError(t, err)
		require.Equal(t, string(bodyC), string(resp.Source_))
	})

	t.Run("versioned_delete_honours_ordering", func(t *testing.T) {
		bodyY := []byte(`{"body":"Y"}`)
		msgY := service.NewMessage(bodyY)
		msgY.MetaSet("action", "index")
		msgY.MetaSet("id", "y")
		msgY.MetaSet("version", "2")
		require.NoError(t, inFunc(ctx, msgY))

		resp, err := client.Get("versioned", "y").Do(ctx)
		require.NoError(t, err)
		require.True(t, resp.Found)

		// A stale delete (lower version) is rejected with a 409, treated as a
		// no-op, and the document remains present.
		staleDelete := service.NewMessage([]byte("{}"))
		staleDelete.MetaSet("action", "delete")
		staleDelete.MetaSet("id", "y")
		staleDelete.MetaSet("version", "1")
		require.NoError(t, inFunc(ctx, staleDelete))

		resp, err = client.Get("versioned", "y").Do(ctx)
		require.NoError(t, err)
		require.True(t, resp.Found)

		// A newer version delete is applied.
		freshDelete := service.NewMessage([]byte("{}"))
		freshDelete.MetaSet("action", "delete")
		freshDelete.MetaSet("id", "y")
		freshDelete.MetaSet("version", "3")
		require.NoError(t, inFunc(ctx, freshDelete))

		resp, err = client.Get("versioned", "y").Do(ctx)
		require.NoError(t, err)
		require.False(t, resp.Found)
	})
}

func TestElasticsearchV9ConnectionTestIntegration(t *testing.T) {
	integration.CheckSkip(t)

	ctx := t.Context()
	ctr, err := testcontainers.Run(t.Context(), "docker.elastic.co/elasticsearch/elasticsearch:9.0.0",
		testcontainers.WithExposedPorts("9200/tcp"),
		testcontainers.WithEnv(map[string]string{
			"discovery.type": "single-node",
			"cluster.routing.allocation.disk.threshold_enabled": "false",
			"xpack.security.enabled":                            "false",
			"ES_JAVA_OPTS":                                      "-Xms256m -Xmx256m",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForHTTP("/").WithPort("9200/tcp").WithStartupTimeout(time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	mappedPort, err := ctr.MappedPort(t.Context(), "9200/tcp")
	require.NoError(t, err)
	url := fmt.Sprintf("http://127.0.0.1:%v", mappedPort.Port())

	client, err := elasticsearch.NewTypedClient(elasticsearch.Config{
		Addresses: []string{url},
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		ok, err := client.Ping().Do(ctx)
		return err == nil && ok
	}, time.Second*30, time.Millisecond*500)

	t.Run("output_valid", func(t *testing.T) {
		resBuilder := service.NewResourceBuilder()

		require.NoError(t, resBuilder.AddOutputYAML(fmt.Sprintf(`
label: test_output
elasticsearch_v9:
  urls: ['%s']
  index: test-index
  action: index
  id: ${! counter() }
`, url)))

		resources, _, err := resBuilder.BuildSuspended()
		require.NoError(t, err)

		require.NoError(t, resources.AccessOutput(t.Context(), "test_output", func(o *service.ResourceOutput) {
			connResults := o.ConnectionTest(t.Context())
			require.Len(t, connResults, 1)
			require.NoError(t, connResults[0].Err)
		}))
	})

	t.Run("output_invalid", func(t *testing.T) {
		resBuilder := service.NewResourceBuilder()

		require.NoError(t, resBuilder.AddOutputYAML(`
label: test_output
elasticsearch_v9:
  urls: ['http://localhost:11111']
  index: test-index
  action: index
  id: ${! counter() }
`))

		resources, _, err := resBuilder.BuildSuspended()
		require.NoError(t, err)

		require.NoError(t, resources.AccessOutput(t.Context(), "test_output", func(o *service.ResourceOutput) {
			connResults := o.ConnectionTest(t.Context())
			require.Len(t, connResults, 1)
			require.Error(t, connResults[0].Err)
		}))
	})
}

// TestElasticsearchVersioningValidation exercises the external versioning
// validation logic in WriteBatch. These are purely offline: they never
// contact Elasticsearch, since the validation errors are returned before the
// bulk request is dispatched.
func TestElasticsearchVersioningValidation(t *testing.T) {
	tests := []struct {
		name        string
		conf        string
		errContains string
	}{
		{
			name: "versioned update is rejected",
			conf: `
urls: ['http://localhost:9200']
index: foo
action: update
id: '${! "1" }'
version: '${! "5" }'
`,
			errContains: `not supported with`,
		},
		{
			name: "versioned create is rejected",
			conf: `
urls: ['http://localhost:9200']
index: foo
action: create
id: '${! "1" }'
version: '${! "5" }'
`,
			errContains: `not supported with`,
		},
		{
			name: "non-numeric version fails to parse",
			conf: `
urls: ['http://localhost:9200']
index: foo
action: index
id: '${! "1" }'
version: '${! "not-a-number" }'
`,
			errContains: `int64`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spec := elasticsearchConfigSpec()
			parsed, err := spec.ParseYAML(test.conf, nil)
			require.NoError(t, err)

			out, err := outputFromParsed(parsed, service.MockResources())
			require.NoError(t, err)

			require.NoError(t, out.Connect(t.Context()))

			err = out.WriteBatch(t.Context(), service.MessageBatch{
				service.NewMessage([]byte(`{"doc":{"a":1}}`)),
			})
			require.Error(t, err)
			require.ErrorContains(t, err, test.errContains)
		})
	}

	// Sanity check: valid configuration (numeric version, or no version at
	// all) does not trip any validation, so the only error possible is a
	// network error from attempting to reach the (unreachable) address.
	for _, test := range []struct {
		name string
		conf string
	}{
		{
			name: "valid numeric version with index action",
			conf: `
urls: ['http://localhost:11111']
index: foo
action: index
id: '${! "1" }'
version: '${! "5" }'
`,
		},
		{
			// upsert is an alias of index in this output, so it accepts an
			// external version just like index does.
			name: "valid numeric version with upsert action",
			conf: `
urls: ['http://localhost:11111']
index: foo
action: upsert
id: '${! "1" }'
version: '${! "5" }'
`,
		},
		{
			name: "version unset with index action",
			conf: `
urls: ['http://localhost:11111']
index: foo
action: index
id: '${! "1" }'
`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			spec := elasticsearchConfigSpec()
			parsed, err := spec.ParseYAML(test.conf, nil)
			require.NoError(t, err)

			out, err := outputFromParsed(parsed, service.MockResources())
			require.NoError(t, err)

			require.NoError(t, out.Connect(t.Context()))

			err = out.WriteBatch(t.Context(), service.MessageBatch{
				service.NewMessage([]byte(`{"doc":{"a":1}}`)),
			})
			// The op itself builds without error; only the network call
			// (to a deliberately unreachable address) fails.
			require.Error(t, err)
			require.NotContains(t, err.Error(), "not supported with")
			require.NotContains(t, err.Error(), "int64")
		})
	}
}
