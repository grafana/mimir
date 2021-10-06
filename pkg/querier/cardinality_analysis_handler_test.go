package querier

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/grafana/mimir/pkg/ingester/client"
)

func TestLabelNamesCardinalityHandler(t *testing.T) {
	items := []*client.LabelValues{
		{LabelName: "label-c", Values: []string{"0c"}},
		{LabelName: "label-b", Values: []string{"0b", "1b"}},
		{LabelName: "label-a", Values: []string{"0a", "1a"}},
		{LabelName: "label-z", Values: []string{"0z", "1z", "2z"}},
	}
	distributor := MockDistributor{items: items}
	handler := LabelNamesCardinalityHandler(distributor)
	ctx := user.InjectOrgID(context.Background(), "team-a")
	request, err := http.NewRequestWithContext(ctx, "GET", "/ignored-url?limit=4", http.NoBody)
	require.NoError(t, err)
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Result().StatusCode)
	body := recorder.Result().Body
	defer body.Close()
	responseBody := LabelNamesCardinalityResponse{}
	bodyContent, err := ioutil.ReadAll(body)
	require.NoError(t, err)
	err = json.Unmarshal(bodyContent, &responseBody)
	require.NoError(t, err)
	require.Equal(t, 4, responseBody.LabelNamesCount)
	require.Equal(t, 8, responseBody.ValuesCountTotal)
	require.Len(t, responseBody.Cardinality, 4)
	require.Equal(t, responseBody.Cardinality[0], &LabelNamesCardinalityItem{LabelName: "label-z", ValuesCount: 3},
		"items must be sorted by ValuesCount in DESC order and by LabelName in ASC order")
	require.Equal(t, responseBody.Cardinality[1], &LabelNamesCardinalityItem{LabelName: "label-a", ValuesCount: 2},
		"items must be sorted by ValuesCount in DESC order and by LabelName in ASC order")
	require.Equal(t, responseBody.Cardinality[2], &LabelNamesCardinalityItem{LabelName: "label-b", ValuesCount: 2},
		"items must be sorted by ValuesCount in DESC order and by LabelName in ASC order")
	require.Equal(t, responseBody.Cardinality[3], &LabelNamesCardinalityItem{LabelName: "label-c", ValuesCount: 1},
		"items must be sorted by ValuesCount in DESC order and by LabelName in ASC order")
}

func TestLabelNamesCardinalityHandler_LimitTest(t *testing.T) {
	td := []struct {
		name                string
		limit               int
		expectedValuesCount int
	}{
		{
			name:                "expected 10 labels in response if limit param is 10",
			limit:               10,
			expectedValuesCount: 10,
		},
		{
			name:                "expected default limit 20 to be applied if limit param is not defined",
			limit:               -1,
			expectedValuesCount: 20,
		},
		{
			name:                "expected all items in response if limit param is greater than count of items",
			limit:               40,
			expectedValuesCount: 30,
		},
		{
			name:                "expected empty items list in response if limit param is 0",
			limit:               0,
			expectedValuesCount: 0,
		},
	}
	for _, data := range td {
		t.Run(data.name, func(t *testing.T) {
			labelCountTotal := 30
			items, valuesCountTotal := generateLabelValues(labelCountTotal)
			distributor := MockDistributor{items: items}
			handler := LabelNamesCardinalityHandler(distributor)

			ctx := user.InjectOrgID(context.Background(), "team-a")
			path := "/ignored-url"
			if data.limit >= 0 {
				path += fmt.Sprintf("?limit=%v", data.limit)
			}
			request, err := http.NewRequestWithContext(ctx, "GET", path, http.NoBody)
			require.NoError(t, err)
			recorder := httptest.NewRecorder()

			handler.ServeHTTP(recorder, request)

			require.Equal(t, http.StatusOK, recorder.Result().StatusCode)
			body := recorder.Result().Body
			defer body.Close()
			responseBody := LabelNamesCardinalityResponse{}
			bodyContent, err := ioutil.ReadAll(body)
			require.NoError(t, err)
			err = json.Unmarshal(bodyContent, &responseBody)
			require.NoError(t, err)
			require.Equal(t, labelCountTotal, responseBody.LabelNamesCount)
			require.Equal(t, valuesCountTotal, responseBody.ValuesCountTotal)
			require.Len(t, responseBody.Cardinality, data.expectedValuesCount)
		})
	}
}

func TestLabelNamesCardinalityHandler_NegativeTests(t *testing.T) {
	td := []struct {
		name                 string
		request              *http.Request
		expectedErrorMessage string
	}{
		{
			name:                 "expected error if `limit` param is negative",
			request:              createRequest("/ignored-url?limit=-1", "team-a"),
			expectedErrorMessage: "limit param can not be negative",
		},
		{
			name:                 "expected error if `limit` param is negative",
			request:              createRequest("/ignored-url?limit=5000", "team-a"),
			expectedErrorMessage: "limit param can not greater than 500",
		},
		{
			name:                 "expected error if tenantId is not defined",
			request:              createRequest("/ignored-url", ""),
			expectedErrorMessage: "no org id",
		},
	}
	for _, data := range td {
		t.Run(data.name, func(t *testing.T) {
			handler := LabelNamesCardinalityHandler(MockDistributor{})

			recorder := httptest.NewRecorder()

			handler.ServeHTTP(recorder, data.request)

			require.Equal(t, http.StatusBadRequest, recorder.Result().StatusCode)
			body := recorder.Result().Body
			defer body.Close()
			bytes, err := ioutil.ReadAll(body)
			require.NoError(t, err)
			require.Contains(t, string(bytes), data.expectedErrorMessage)
		})
	}
}

func createRequest(path string, tenantID string) *http.Request {
	ctx := context.Background()
	if len(tenantID) > 0 {
		ctx = user.InjectOrgID(ctx, tenantID)
	}
	request, _ := http.NewRequestWithContext(ctx, "GET", path, http.NoBody)
	return request
}

func generateLabelValues(count int) ([]*client.LabelValues, int) {
	valuesCount := 0
	items := make([]*client.LabelValues, count)
	for i := 0; i < count; i++ {
		values := make([]string, i+1)
		for j := 0; j < i+1; j++ {
			valuesCount++
			values[i] = fmt.Sprintf("value-%v", j)
		}
		items[i] = &client.LabelValues{LabelName: fmt.Sprintf("label-%v", i), Values: values}
	}
	return items, valuesCount
}

type MockDistributor struct {
	Distributor
	items            []*client.LabelValues
	receivedMatchers []*labels.Matcher
}

func (d MockDistributor) LabelNamesAndValues(_ context.Context, matchers []*labels.Matcher) (*client.LabelNamesAndValuesResponse, error) {
	d.receivedMatchers = matchers
	return &client.LabelNamesAndValuesResponse{Items: d.items}, nil
}
