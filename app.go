package poc_elasticsearch_appengine

import (
	"encoding/json"
	"net/http"

	"github.com/olivere/elastic"
	"google.golang.org/appengine/urlfetch"
	"golang.org/x/net/context"
	"google.golang.org/appengine"
)

const (
	USERNAME = ""
	PASSWORD = ""
	URL = ""
)

type AppEngineTransport struct {
	Username string
	Password string
	Transport http.RoundTripper
}

func (t *AppEngineTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	rt := t.Transport
	if rt == nil {
		panic("transport must be supplied")
	}
	req.SetBasicAuth(t.Username, t.Password)
	return rt.RoundTrip(req)
}

func GetElasticClient(ctx context.Context) *elastic.Client {
	transport := &AppEngineTransport{
		Username:  USERNAME,
		Password:  PASSWORD,
		Transport: &urlfetch.Transport{Context: ctx},
	}
	httpClient := &http.Client{Transport: transport}

	elasticClient, err := elastic.NewClient(
		elastic.SetHttpClient(httpClient),
		elastic.SetURL(URL),
		elastic.SetBasicAuth(USERNAME, PASSWORD),
		elastic.SetSniff(false),
		elastic.SetHealthcheck(false),
		elastic.SetMaxRetries(1),
		elastic.SetHealthcheckTimeoutStartup(0),
	)

	if err != nil {
		panic(err)
	}

	return elasticClient
}

func init() {
	http.HandleFunc("/create-index", handleCreateIndex)
	http.HandleFunc("/create-new-tweet", handleCreateNewTweet)
}

func handleCreateIndex(w http.ResponseWriter, r *http.Request) {
	client := GetElasticClient(appengine.NewContext(r))
	_, err := client.CreateIndex("twitter").Do()
	if err != nil {
		panic(err)
	}
}

func handleCreateNewTweet(w http.ResponseWriter, r *http.Request) {
	var tweet Tweet
	d := json.NewDecoder(r.Body)
	d.Decode(&tweet)

	client := GetElasticClient(appengine.NewContext(r))

	_, err := client.Index().
	Index("twitter").
	Type("tweet").
	BodyJson(tweet).
	Refresh(true).
	Do()
	if err != nil {
		// Handle error
		panic(err)
	}
}

type Tweet struct {
	User    string `json:"user"`
	Message string `json:"message"`
}
