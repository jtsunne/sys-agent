package external

import (
	"testing"
	"time"

	"github.com/go-pkgz/mongo/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMongoProvider_Status(t *testing.T) {
	_, _, teardown := mongo.MakeTestConnection(t)
	defer teardown()

	{
		p := MongoProvider{TimeOut: time.Second}
		resp, err := p.Status(Request{Name: "test", URL: "mongodb://localhost:27017"})
		require.NoError(t, err)

		assert.Equal(t, "test", resp.Name)
		assert.Equal(t, 200, resp.StatusCode)
		assert.True(t, resp.ResponseTime > 0)
		assert.Equal(t, map[string]interface{}{"status": "ok"}, resp.Body)
	}

	{
		p := MongoProvider{TimeOut: time.Second}
		_, err := p.Status(Request{Name: "test", URL: "mongodb://localhost:27000"})
		require.Error(t, err)
	}
}