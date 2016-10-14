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
	"time"
)

const (
	change_ADD string = "ADD"
	change_RM         = "RM"
)

// Simple state machine
type State struct {
	inner map[string]string
}

func (s State) Snapshot() []byte {
	buf := bytes.NewBuffer(nil)

	for k, v := range s.inner {
		buf.WriteString(k)
		buf.WriteString("×")
		buf.WriteString(v)
		buf.WriteString("×")
	}

	return buf.Bytes()
}

func (s State) Apply(c con.Change) {
	chg := c.(Change)

	log.Println("Applying", chg)

	if chg.t == change_ADD {
		s.inner[chg.key] = chg.val
	} else if chg.t == change_RM {
		delete(s.inner, chg.key)
	}
}

func (s State) Install(ss []byte) {
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

func main() {
	initMaster := flag.Bool("initMaster", false, "Initialize as master, then add others")
	participants := flag.String("participants", "", "Comma-separated list of other participants' addresses")
	addr := flag.String("listen", "localhost:9000", "Address to listen on")
	cluster := flag.String("cluster", "cluster1", "ClusterID")
	interval := flag.Uint("interval", 2, "interval for submitting random changes")

	flag.Parse()

	participant := con.NewParticipant(*cluster, http.NewHttpConnector(3*time.Second), State{inner: make(map[string]string)})
	server := http.NewHttpConsensusServer()

	server.Register(*cluster, participant, ChangeDeserializer{})
	go nhttp.ListenAndServe(*addr, server)

	if *initMaster {
		participant.InitMaster(con.Member{Address: "http://" + *addr}, []byte{})

		for _, a := range strings.Split(*participants, ",") {
			log.Println("Adding", a)
			participant.AddParticipant(con.Member{Address: a})
		}

		participant.Submit([]con.Change{})
	}

	i := 0
	for {
		time.Sleep(time.Duration(*interval) * time.Second)

		if err := participant.PingMaster(); err != nil {
			fmt.Println("Master down:", err)
		} else {
			fmt.Println("Master is up")
		}

		err := participant.SubmitOne(Change{t: change_ADD, key: fmt.Sprintf("k%d", i), val: fmt.Sprintf("val%d", i)})

		if err != nil {
			fmt.Println(err)
		}

		if i%5 == 0 {
			log.Println("master:", participant.IsMaster(), len(participant.GetState().(State).inner), participant.GetState().(State))
		}

		i++
	}

}
