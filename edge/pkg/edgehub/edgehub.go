package edgehub

import (
	"log"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/kubeedge/beehive/pkg/core"
	beehiveContext "github.com/kubeedge/beehive/pkg/core/context"
	"github.com/kubeedge/kubeedge/edge/pkg/common/modules"
	"github.com/kubeedge/kubeedge/edge/pkg/edgehub/certificate"
	"github.com/kubeedge/kubeedge/edge/pkg/edgehub/clients"
	"github.com/kubeedge/kubeedge/edge/pkg/edgehub/config"
	"github.com/kubeedge/kubeedge/pkg/apis/componentconfig/edgecore/v1alpha1"
	"k8s.io/klog/v2"
)

var HasTLSTunnelCerts = make(chan bool, 1)

//EdgeHub defines edgehub object structure
type EdgeHub struct {
	certManager   certificate.CertManager
	chClient      clients.Adapter
	reconnectChan chan struct{}
	keeperLock    sync.RWMutex
	enable        bool
	pulsarClient  pulsar.Client
}

var _ core.Module = (*EdgeHub)(nil)

func newEdgeHub(enable bool) *EdgeHub {
	return &EdgeHub{
		reconnectChan: make(chan struct{}),
		enable:        enable,
	}
}

// Register register edgehub
func Register(eh *v1alpha1.EdgeHub, nodeName string) {
	config.InitConfigure(eh, nodeName)
	core.Register(newEdgeHub(eh.Enable))
}

//Name returns the name of EdgeHub module
func (eh *EdgeHub) Name() string {
	return modules.EdgeHubModuleName
}

//Group returns EdgeHub group
func (eh *EdgeHub) Group() string {
	return modules.HubGroup
}

//Enable indicates whether this module is enabled
func (eh *EdgeHub) Enable() bool {
	return eh.enable
}

//Start sets context and starts the controller
func (eh *EdgeHub) Start() {
	eh.certManager = certificate.NewCertManager(config.Config.EdgeHub, config.Config.NodeName)
	eh.certManager.Start()

	HasTLSTunnelCerts <- true
	close(HasTLSTunnelCerts)

	//启动pulsar客户端
	pulsarClient := createPulsarClient()
	eh.pulsarClient = pulsarClient

	go eh.ifRotationDone()

	for {
		select {
		case <-beehiveContext.Done():
			klog.Warning("EdgeHub stop")
			// 进程退出 关闭连接
			eh.pulsarClient.Close()
			return
		default:
		}
		err := eh.initial()
		if err != nil {
			klog.Exitf("failed to init controller: %v", err)
			return
		}

		waitTime := time.Duration(config.Config.Heartbeat) * time.Second * 2

		err = eh.chClient.Init()
		if err != nil {
			klog.Errorf("connection failed: %v, will reconnect after %s", err, waitTime.String())
			time.Sleep(waitTime)
			continue
		}
		// execute hook func after connect
		eh.pubConnectInfo(true)
		go eh.routeToEdge()
		go eh.routeToCloud()
		go eh.keepalive()

		// wait the stop signal
		// stop authinfo manager/websocket connection
		<-eh.reconnectChan
		eh.chClient.UnInit()

		// execute hook fun after disconnect
		eh.pubConnectInfo(false)

		// sleep one period of heartbeat, then try to connect cloud hub again
		klog.Warningf("connection is broken, will reconnect after %s", waitTime.String())
		time.Sleep(waitTime)

		// clean channel
	clean:
		for {
			select {
			case <-eh.reconnectChan:
			default:
				break clean
			}
		}
	}
}

// 创建pulsar客户端
func createPulsarClient() pulsar.Client {

	operationTimeout := time.Duration(config.Config.Pulsar.OperationTimeout)
	connectionTimeout := time.Duration(config.Config.Pulsar.ConnectionTimeout)

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               config.Config.Pulsar.PulsarServerURL,
		OperationTimeout:  operationTimeout * time.Second,
		ConnectionTimeout: connectionTimeout * time.Second,
	})
	if err != nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}

	return client
}
