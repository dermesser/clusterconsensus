package main

import (
	"bytes"
	con "clusterconsensus"
	"clusterconsensus/http"
	"flag"
	"fmt"
	"log"
	nhttp "net/http"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
)

const (
	change_ADD string = "ADD"
	change_RM         = "RM"
)

// Simple state machine
type State struct {
	sync.Mutex
	inner map[string]string
}

func (s *State) Snapshot() []byte {
	s.Lock()
	defer s.Unlock()
	buf := bytes.NewBuffer(nil)

	for k, v := range s.inner {
		buf.WriteString(k)
		buf.WriteString("×")
		buf.WriteString(v)
		buf.WriteString("×")
	}

	return buf.Bytes()
}

func (s *State) Apply(c con.Change) {
	s.Lock()
	defer s.Unlock()
	chg := c.(Change)

	glog.Info("Applying", chg)

	if chg.t == change_ADD {
		s.inner[chg.key] = chg.val
	} else if chg.t == change_RM {
		delete(s.inner, chg.key)
	}
}

func (s *State) Install(ss []byte) {
	s.Lock()
	defer s.Unlock()
	parts := strings.Split(string(ss), "×")

	for i := 0; i < len(parts)-1; {
		key := parts[i]
		i++
		val := parts[i]
		i++
		s.inner[key] = val
	}
}

// Change to state machine
type Change struct {
	t   string
	key string
	val string
}

func (c Change) Serialize() []byte {
	return []byte(fmt.Sprintf("%s×%s×%s", c.t, c.key, c.val))
}

type ChangeDeserializer struct{}

func (cd ChangeDeserializer) Deserialize(b []byte) con.Change {
	parts := strings.Split(string(b), "×")

	return Change{t: parts[0], key: parts[1], val: parts[2]}
}

type EventHandler struct{}

var isMaster bool = false

func (eh EventHandler) OnBecomeMaster(*con.Participant) {
	glog.Info("BECAME MASTER")
	isMaster = true
}

func (eh EventHandler) OnLoseMaster(*con.Participant) {
	glog.Info("LOST MASTERSHIP")
	isMaster = false
}

func (eh EventHandler) OnCommit(p *con.Participant, s con.SequenceNumber, chg []con.Change) {
	glog.Info("COMMITTED: ", s, chg)
}

func main() {
	initMaster := flag.Bool("initMaster", false, "Initialize as master, then add others")
	participants := flag.String("participants", "", "Comma-separated list of other participants' addresses")
	addr := flag.String("listen", ":9000", "Address to listen on")
	cluster := flag.String("cluster", "cluster1", "ClusterID")
	interval := flag.Uint("interval", 2, "interval for submitting random changes")

	flag.Parse()

	participant := con.NewParticipant(*cluster, http.NewHttpConnector(3*time.Second), &State{inner: make(map[string]string)})
	participant.SetEventHandler(EventHandler{})
	server := http.NewHttpConsensusServer()

	server.Register(*cluster, participant, ChangeDeserializer{})
	go nhttp.ListenAndServe(*addr, server)

	if *initMaster {
		participant.InitMaster(con.Member{Address: "http://" + *addr}, []byte{})

		for _, a := range strings.Split(*participants, ",") {
			log.Println("Adding", a)
			participant.AddParticipant(con.Member{Address: "http://" + a})
		}

		participant.Submit([]con.Change{})
	}

	i := 0
	for {
		time.Sleep(time.Duration(*interval) * time.Second)

		if isMaster {
			glog.Info("<MASTER>")
		} else if err := participant.PingMaster(); err != nil {
			glog.Info("Master down:", err)
		} else {
			glog.Info("Master is up")
		}

		err := participant.SubmitOne(Change{t: change_ADD, key: fmt.Sprintf(*addr+"k%d", i), val: fmt.Sprintf("val%d", i)})

		if err != nil {
			glog.Info("couldn't submit change:", err)
			continue
		}

		if i%5 == 0 {
			participant.Lock()
			glog.Info("master: ", participant.IsMaster(), " state len: ", len(participant.GetState().(*State).inner),
				" state: ", participant.GetState().(*State))
			participant.Unlock()
		}

		i++
	}

}
