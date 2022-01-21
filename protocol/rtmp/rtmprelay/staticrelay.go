package rtmprelay

import (
	"context"
	"fmt"
	"golang.org/x/sync/semaphore"
	"sync"
	"time"

	"github.com/vidvi-app/livego/av"
	"github.com/vidvi-app/livego/protocol/rtmp/core"
	"github.com/vidvi-app/livego/utils/retry"

	log "github.com/sirupsen/logrus"
)

type StaticPush struct {
	RtmpUrl       string
	packetChan    chan *av.Packet
	sndctrlChan   chan string
	connectClient *core.ConnClient
	startFlag     bool
	stopping      bool
	abandoned     bool
	sem           *semaphore.Weighted
}

var staticPushMap = make(map[string]*StaticPush)
var mapLock = new(sync.RWMutex)

var (
	STATIC_RELAY_STOP_CTRL = "STATIC_RTMPRELAY_STOP"
)

func GetAndCreateStaticPushObject(rtmpurl string) *StaticPush {
	mapLock.RLock()
	staticpush, ok := staticPushMap[rtmpurl]
	log.Debugf("GetAndCreateStaticPushObject: %s, return %v", rtmpurl, ok)
	if !ok {
		mapLock.RUnlock()
		newStaticpush := NewStaticPush(rtmpurl)

		mapLock.Lock()
		staticPushMap[rtmpurl] = newStaticpush
		mapLock.Unlock()

		return newStaticpush
	}
	mapLock.RUnlock()

	return staticpush
}

func GetStaticPushObject(rtmpurl string) (*StaticPush, error) {
	mapLock.RLock()
	if staticpush, ok := staticPushMap[rtmpurl]; ok {
		mapLock.RUnlock()
		return staticpush, nil
	}
	mapLock.RUnlock()

	return nil, fmt.Errorf("G_StaticPushMap[%s] not exist....", rtmpurl)
}

func ReleaseStaticPushObject(rtmpurl string) {
	mapLock.RLock()
	if _, ok := staticPushMap[rtmpurl]; ok {
		mapLock.RUnlock()

		log.Debugf("ReleaseStaticPushObject %s ok", rtmpurl)
		mapLock.Lock()
		delete(staticPushMap, rtmpurl)
		mapLock.Unlock()
	} else {
		mapLock.RUnlock()
		log.Debugf("ReleaseStaticPushObject: not find %s", rtmpurl)
	}
}

func NewStaticPush(rtmpurl string) *StaticPush {
	return &StaticPush{
		RtmpUrl:     rtmpurl,
		packetChan:  make(chan *av.Packet, 500),
		sndctrlChan: make(chan string),
		sem:         semaphore.NewWeighted(1),
	}
}

func (s *StaticPush) Start() (err error) {
	if s.startFlag || s.abandoned {
		return
	}
	if !s.sem.TryAcquire(1) {
		return
	}
	defer s.sem.Release(1)

	s.connectClient = core.NewConnClient()

	log.Debugf("static publish server addr:%v starting....", s.RtmpUrl)

	re := retry.Retry{
		Times:    5,
		Cooldown: time.Second * 5,
	}
	err = re.Do(func() error {
		if s.stopping == true {
			return nil
		}
		return s.connectClient.Start(s.RtmpUrl, "publish")
	})
	if err != nil {
		log.Debugf("connectClient.Start url=%v error", s.RtmpUrl)
		s.abandoned = true
		return
	}
	log.Debugf("static publish server addr:%v started, streamid=%d", s.RtmpUrl, s.connectClient.GetStreamId())
	s.startFlag = true
	go s.HandleAvPacket()
	return
}

func (s *StaticPush) Stop() {
	s.stopping = true
	s.sem.Acquire(context.Background(), 1)
	defer s.sem.Release(1)
	if !s.startFlag || s.abandoned {
		s.abandoned = false
		return
	}

	log.Debugf("StaticPush Stop: %s", s.RtmpUrl)
	s.sndctrlChan <- STATIC_RELAY_STOP_CTRL
	s.startFlag = false
	s.stopping = false
}

func (s *StaticPush) WriteAvPacket(packet *av.Packet) {
	if !s.startFlag || s.abandoned {
		return
	}

	s.packetChan <- packet
}

func (s *StaticPush) sendPacket(p *av.Packet) {
	if !s.startFlag || s.abandoned {
		return
	}
	var cs core.ChunkStream

	cs.Data = p.Data
	cs.Length = uint32(len(p.Data))
	cs.StreamID = s.connectClient.GetStreamId()
	cs.Timestamp = p.TimeStamp
	//cs.Timestamp += v.BaseTimeStamp()

	if p.IsVideo {
		cs.TypeID = av.TAG_VIDEO
	} else {
		if p.IsMetadata {
			cs.TypeID = av.TAG_SCRIPTDATAAMF0
		} else {
			cs.TypeID = av.TAG_AUDIO
		}
	}

	s.connectClient.Write(cs)
}

func (s *StaticPush) HandleAvPacket() {
	if !s.IsStart() || s.abandoned {
		log.Debugf("static push %s not started", s.RtmpUrl)
		return
	}

	for {
		select {
		case packet := <-s.packetChan:
			s.sendPacket(packet)
		case ctrlcmd := <-s.sndctrlChan:
			if ctrlcmd == STATIC_RELAY_STOP_CTRL {
				s.connectClient.Close(nil)
				log.Debugf("Static HandleAvPacket close: publishurl=%s", s.RtmpUrl)
				return
			}
		}
	}
}

func (s *StaticPush) IsStart() bool {
	return s.startFlag
}

func (s *StaticPush) Abandoned() bool {
	return s.abandoned
}
