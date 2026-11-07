// Package scaler changes the number of delivery workers from the dashboard. With Docker
// Compose it talks to the Docker Engine API over the mounted socket; in Kubernetes it patches
// the worker Deployment's scale subresource with the pod's service account.
package scaler

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ErrUnavailable means no scaler is configured.
var ErrUnavailable = errors.New("scaler unavailable")

// ErrNotFound means the worker does not exist.
var ErrNotFound = errors.New("worker not found")

// Scaler controls worker replicas.
type Scaler interface {
	Kind() string
	Scale(ctx context.Context, replicas int) error
	Kill(ctx context.Context, workerID string) error
	Stop(ctx context.Context, workerID string) error
}

// None is used when scaling is not possible.
type None struct{}

func (None) Kind() string                       { return "none" }
func (None) Scale(context.Context, int) error   { return ErrUnavailable }
func (None) Kill(context.Context, string) error { return ErrUnavailable }
func (None) Stop(context.Context, string) error { return ErrUnavailable }

// --- Docker ---

// Docker scales a Compose service by cloning one of its containers.
type Docker struct {
	hc      *http.Client
	project string
	service string
}

// NewDocker returns a Docker scaler if the socket is reachable.
func NewDocker(socket, project, service string) (*Docker, error) {
	if _, err := os.Stat(socket); err != nil {
		return nil, err
	}
	hc := &http.Client{Timeout: 60 * time.Second, Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		},
	}}
	d := &Docker{hc: hc, project: project, service: service}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := d.call(ctx, http.MethodGet, "/_ping", nil, nil); err != nil {
		return nil, err
	}
	return d, nil
}

func (d *Docker) Kind() string { return "docker" }

func (d *Docker) call(ctx context.Context, method, path string, body any, out any) (int, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://docker"+path, rd)
	if err != nil {
		return 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := d.hc.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return resp.StatusCode, fmt.Errorf("docker %s %s: %s: %s", method, path, resp.Status, strings.TrimSpace(string(b)))
	}
	if out != nil && len(b) > 0 {
		return resp.StatusCode, json.Unmarshal(b, out)
	}
	return resp.StatusCode, nil
}

type container struct {
	ID     string            `json:"Id"`
	Names  []string          `json:"Names"`
	State  string            `json:"State"`
	Labels map[string]string `json:"Labels"`
}

func (d *Docker) list(ctx context.Context, all bool) ([]container, error) {
	f, _ := json.Marshal(map[string][]string{"label": {
		"com.docker.compose.project=" + d.project, "com.docker.compose.service=" + d.service,
	}})
	var cs []container
	_, err := d.call(ctx, http.MethodGet, "/containers/json?all="+strconv.FormatBool(all)+"&filters="+url.QueryEscape(string(f)), nil, &cs)
	sort.Slice(cs, func(i, j int) bool { return num(cs[i]) < num(cs[j]) })
	return cs, err
}

func num(c container) int {
	n, _ := strconv.Atoi(c.Labels["com.docker.compose.container-number"])
	return n
}

// Scale starts or stops worker containers until `replicas` are running.
func (d *Docker) Scale(ctx context.Context, replicas int) error {
	all, err := d.list(ctx, true)
	if err != nil {
		return err
	}
	var running, stopped []container
	for _, c := range all {
		if c.State == "running" {
			running = append(running, c)
		} else {
			stopped = append(stopped, c)
		}
	}
	switch {
	case len(running) > replicas:
		// Stop the newest workers gracefully (they drain and commit), then remove them.
		for _, c := range running[replicas:] {
			if _, err := d.call(ctx, http.MethodPost, "/containers/"+c.ID+"/stop?t=30", nil, nil); err != nil {
				return err
			}
			_, _ = d.call(ctx, http.MethodDelete, "/containers/"+c.ID, nil, nil)
		}
	case len(running) < replicas:
		need := replicas - len(running)
		// Restart previously stopped/killed workers first.
		for _, c := range stopped {
			if need == 0 {
				break
			}
			if _, err := d.call(ctx, http.MethodPost, "/containers/"+c.ID+"/start", nil, nil); err != nil {
				return err
			}
			need--
		}
		if need == 0 {
			return nil
		}
		if len(all) == 0 {
			return errors.New("no worker container to use as a template; run `docker compose up -d worker` once")
		}
		maxNum := 0
		for _, c := range all {
			maxNum = max(maxNum, num(c))
		}
		for i := 0; i < need; i++ {
			if err := d.clone(ctx, all[0].ID, maxNum+1+i); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *Docker) clone(ctx context.Context, templateID string, n int) error {
	var insp struct {
		Config          map[string]any `json:"Config"`
		HostConfig      map[string]any `json:"HostConfig"`
		NetworkSettings struct {
			Networks map[string]struct {
				Aliases []string `json:"Aliases"`
			} `json:"Networks"`
		} `json:"NetworkSettings"`
	}
	if _, err := d.call(ctx, http.MethodGet, "/containers/"+templateID+"/json", nil, &insp); err != nil {
		return err
	}
	cfg := insp.Config
	delete(cfg, "Hostname") // a fresh hostname becomes the new worker's id
	if labels, ok := cfg["Labels"].(map[string]any); ok {
		labels["com.docker.compose.container-number"] = strconv.Itoa(n)
		labels["com.docker.compose.oneoff"] = "False"
	}
	cfg["HostConfig"] = insp.HostConfig
	eps := map[string]any{}
	for name := range insp.NetworkSettings.Networks {
		eps[name] = map[string]any{"Aliases": []string{d.service}}
	}
	cfg["NetworkingConfig"] = map[string]any{"EndpointsConfig": eps}
	var created struct {
		ID string `json:"Id"`
	}
	name := fmt.Sprintf("%s-%s-%d", d.project, d.service, n)
	if _, err := d.call(ctx, http.MethodPost, "/containers/create?name="+url.QueryEscape(name), cfg, &created); err != nil {
		return err
	}
	_, err := d.call(ctx, http.MethodPost, "/containers/"+created.ID+"/start", nil, nil)
	return err
}

func (d *Docker) find(ctx context.Context, workerID string) (*container, error) {
	cs, err := d.list(ctx, false)
	if err != nil {
		return nil, err
	}
	for _, c := range cs {
		if strings.HasPrefix(c.ID, workerID) {
			return &c, nil
		}
	}
	return nil, ErrNotFound
}

// Kill sends SIGKILL: no graceful shutdown, no final commit.
func (d *Docker) Kill(ctx context.Context, workerID string) error {
	c, err := d.find(ctx, workerID)
	if err != nil {
		return err
	}
	_, err = d.call(ctx, http.MethodPost, "/containers/"+c.ID+"/kill?signal=SIGKILL", nil, nil)
	return err
}

// Stop sends SIGTERM and waits for a graceful drain.
func (d *Docker) Stop(ctx context.Context, workerID string) error {
	c, err := d.find(ctx, workerID)
	if err != nil {
		return err
	}
	_, err = d.call(ctx, http.MethodPost, "/containers/"+c.ID+"/stop?t=30", nil, nil)
	return err
}

// --- Kubernetes ---

// Kubernetes scales the worker Deployment through the API server.
type Kubernetes struct {
	hc         *http.Client
	base       string
	token      string
	namespace  string
	deployment string
}

const saDir = "/var/run/secrets/kubernetes.io/serviceaccount"

// NewKubernetes returns an in-cluster scaler.
func NewKubernetes(namespace, deployment string) (*Kubernetes, error) {
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" {
		return nil, errors.New("not running in a cluster")
	}
	token, err := os.ReadFile(saDir + "/token")
	if err != nil {
		return nil, err
	}
	ca, err := os.ReadFile(saDir + "/ca.crt")
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca)
	return &Kubernetes{
		hc:   &http.Client{Timeout: 15 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}},
		base: "https://" + net.JoinHostPort(host, port), token: strings.TrimSpace(string(token)),
		namespace: namespace, deployment: deployment,
	}, nil
}

func (k *Kubernetes) Kind() string { return "kubernetes" }

func (k *Kubernetes) do(ctx context.Context, method, path, contentType string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, method, k.base+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+k.token)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := k.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		return ErrNotFound
	}
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("kubernetes %s %s: %s: %s", method, path, resp.Status, b)
	}
	return nil
}

// Scale sets the Deployment's replicas. (With KEDA enabled, the autoscaler may change it again.)
func (k *Kubernetes) Scale(ctx context.Context, replicas int) error {
	body, _ := json.Marshal(map[string]any{"spec": map[string]any{"replicas": replicas}})
	return k.do(ctx, http.MethodPatch, fmt.Sprintf("/apis/apps/v1/namespaces/%s/deployments/%s/scale", k.namespace, k.deployment),
		"application/merge-patch+json", body)
}

// Kill deletes the worker pod immediately (grace period 0).
func (k *Kubernetes) Kill(ctx context.Context, workerID string) error {
	body, _ := json.Marshal(map[string]any{"gracePeriodSeconds": 0})
	return k.do(ctx, http.MethodDelete, fmt.Sprintf("/api/v1/namespaces/%s/pods/%s", k.namespace, workerID), "application/json", body)
}

// Stop deletes the worker pod with its normal grace period (SIGTERM, drain).
func (k *Kubernetes) Stop(ctx context.Context, workerID string) error {
	return k.do(ctx, http.MethodDelete, fmt.Sprintf("/api/v1/namespaces/%s/pods/%s", k.namespace, workerID), "", nil)
}
