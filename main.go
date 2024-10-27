package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"os"

	"cloud.google.com/go/pubsub"
	"go.uber.org/fx"
	"google.golang.org/api/option"
)

type Publisher struct {
	logger *log.Logger
	topic  *pubsub.Topic
}

type Email struct {
	Publisher Publisher
}

type PubSubParams struct {
	Config struct {
		ProjectId       string
		CredentialsPath string
	}
	Logger *log.Logger
}

func newPubSubClient(lifecycle fx.Lifecycle, params PubSubParams) *pubsub.Client {
	client := new(pubsub.Client)
	lifecycle.Append(
		fx.Hook{
			OnStart: func(ctx context.Context) error {
				params.Logger.Println("Connecting to PubSub...")
				clientOption := option.WithCredentialsFile(params.Config.CredentialsPath)
				newClient, err := pubsub.NewClient(ctx, params.Config.ProjectId, clientOption)
				if err == nil {
					*client = *newClient
					params.Logger.Println("Successfully connected to PubSub.")
				} else {
					params.Logger.Printf("Failed to connect to PubSub: %v", err)
				}
				return err
			},
			OnStop: func(ctx context.Context) error {
				params.Logger.Println("Closing PubSub connection...")
				return client.Close()
			},
		},
	)
	return client
}

func NewEmailTopic(ctx context.Context, client *pubsub.Client, topicId string) (*Email, error) {
	topic := client.Topic(topicId)
	exists, err := topic.Exists(ctx)
	if err != nil {
		return nil, err
	} else if !exists {
		return nil, errors.New("PubSub topic doesn't exist")
	}
	return &Email{
		Publisher: Publisher{
			logger: log.New(os.Stdout, "[email] ", log.LstdFlags|log.Lmicroseconds),
			topic:  topic,
		},
	}, nil
}

func main() {
	logger := log.New(os.Stdout, "[app] ", log.LstdFlags|log.Lmicroseconds)

	app := fx.New(
		fx.Provide(
			func() PubSubParams {
				return PubSubParams{
					Logger: logger,
					Config: struct {
						ProjectId       string
						CredentialsPath string
					}{
						ProjectId:       os.Getenv("PROJECT_ID"),
						CredentialsPath: os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"),
					},
				}
			},
			newPubSubClient,
		),
		fx.Invoke(func(lifecycle fx.Lifecycle) {
			go func() {
				names, err := net.LookupHost("pubsub.googleapis.com")
				if err != nil {
					return
				}
				logger.Printf("%#v\n", names)
			}()
		}),
		fx.Invoke(func(lifecycle fx.Lifecycle, client *pubsub.Client) {
			http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte("Hello, Cloud Run!"))
			})

			http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
				topic := client.Topic("support-test")
				exists, err := topic.Exists(r.Context())
				if err != nil {
					http.Error(w, "Failed to check topic existence: "+err.Error(), http.StatusInternalServerError)
					return
				}
				if !exists {
					http.Error(w, "Topic does not exist", http.StatusNotFound)
					return
				}
				w.Write([]byte("PubSub connection is healthy. Topic exists."))
			})

			go func() {
				if err := http.ListenAndServe(":8080", nil); err != nil {
					logger.Fatal(err)
				}
			}()
		}),
	)
	app.Run()
}
