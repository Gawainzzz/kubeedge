package edgehub

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"k8s.io/klog/v2"
	"time"

	beehiveContext "github.com/kubeedge/beehive/pkg/core/context"
	"github.com/kubeedge/beehive/pkg/core/model"
	connect "github.com/kubeedge/kubeedge/edge/pkg/common/cloudconnection"
	messagepkg "github.com/kubeedge/kubeedge/edge/pkg/common/message"
	"github.com/kubeedge/kubeedge/edge/pkg/common/modules"
	"github.com/kubeedge/kubeedge/edge/pkg/edgehub/clients"
	"github.com/kubeedge/kubeedge/edge/pkg/edgehub/common/msghandler"
	"github.com/kubeedge/kubeedge/edge/pkg/edgehub/config"
)

var groupMap = map[string]string{
	"resource": modules.MetaGroup,
	"twin":     modules.TwinGroup,
	"func":     modules.MetaGroup,
	"user":     modules.BusGroup,
}

func (eh *EdgeHub) initial() (err error) {
	cloudHubClient, err := clients.GetClient()
	if err != nil {
		return err
	}

	eh.chClient = cloudHubClient

	return nil
}

func isSyncResponse(msgID string) bool {
	return msgID != ""
}

func init() {
	handler := &defaultHandler{}
	msghandler.RegisterHandler(handler)
}

type defaultHandler struct {
}

func (*defaultHandler) Filter(message *model.Message) bool {
	group := message.GetGroup()
	return group == messagepkg.ResourceGroupName || group == messagepkg.TwinGroupName ||
		group == messagepkg.FuncGroupName || group == messagepkg.UserGroupName
}

func (*defaultHandler) Process(message *model.Message, clientHub clients.Adapter) error {
	group := message.GetGroup()
	md := ""
	switch group {
	case messagepkg.ResourceGroupName:
		md = modules.MetaGroup
	case messagepkg.TwinGroupName:
		md = modules.TwinGroup
	case messagepkg.FuncGroupName:
		md = modules.MetaGroup
	case messagepkg.UserGroupName:
		md = modules.BusGroup
	}

	isResponse := isSyncResponse(message.GetParentID())
	if isResponse {
		beehiveContext.SendResp(*message)
		return nil
	}
	if group == messagepkg.UserGroupName && message.GetSource() == "router_eventbus" {
		beehiveContext.Send(modules.EventBusModuleName, *message)
	} else if group == messagepkg.UserGroupName && message.GetSource() == "router_servicebus" {
		beehiveContext.Send(modules.ServiceBusModuleName, *message)
	} else {
		beehiveContext.SendToGroup(md, *message)
	}
	return nil
}

func (eh *EdgeHub) dispatch(message model.Message) error {
	// handler for msg.
	err := msghandler.ProcessHandler(message, eh.chClient)
	if err != nil {
		return err
	}

	return nil
}

func (eh *EdgeHub) routeToEdge() {
	for {
		select {
		case <-beehiveContext.Done():
			klog.Warning("EdgeHub RouteToEdge stop")
			return
		default:
		}
		message, err := eh.chClient.Receive()
		if err != nil {
			klog.Errorf("websocket read error: %v", err)
			eh.reconnectChan <- struct{}{}
			return
		}

		klog.V(4).Infof("[edgehub/routeToEdge] receive msg from cloud, msg:% +v", message)
		err = eh.dispatch(message)
		if err != nil {
			klog.Errorf("failed to dispatch message, discard: %v", err)
		}
	}
}

func (eh *EdgeHub) sendToCloud(message model.Message) error {
	eh.keeperLock.Lock()
	klog.V(4).Infof("[edgehub/sendToCloud] send msg to cloud, msg: %+v", message)
	err := eh.chClient.Send(message)
	eh.keeperLock.Unlock()
	if err != nil {
		return fmt.Errorf("failed to send message, error: %v", err)
	}

	return nil
}

func (eh *EdgeHub) routeToCloud() {
	for {
		select {
		case <-beehiveContext.Done():
			klog.Warning("EdgeHub RouteToCloud stop")
			return
		default:
		}
		message, err := beehiveContext.Receive(modules.EdgeHubModuleName)
		if err != nil {
			klog.Errorf("failed to receive message from edge: %v", err)
			time.Sleep(time.Second)
			continue
		}

		// post message to cloud hub
		//err = eh.sendToCloud(message)
		//if err != nil {
		//    klog.Errorf("failed to send message to cloud: %v", err)
		//    eh.reconnectChan <- struct{}{}
		//    return
		//}

		// 修改上报消息通过 pulsar发送至云端
		err = eh.pulsarProducer(message, config.Config.Pulsar.UpdateDeviceModleTopic)
		if err != nil {
			klog.Errorf("failed to send message to pulsar cloud: %v", err)
			eh.reconnectChan <- struct{}{}
			return
		}
	}
}

func (eh *EdgeHub) keepalive() {
	for {
		select {
		case <-beehiveContext.Done():
			klog.Warning("EdgeHub KeepAlive stop")
			return
		default:
		}
		msg := model.NewMessage("").
			BuildRouter(modules.EdgeHubModuleName, "resource", "node", messagepkg.OperationKeepalive).
			FillBody("ping")

		// post message to cloud hub
		err := eh.sendToCloud(*msg)
		if err != nil {
			klog.Errorf("websocket write error: %v", err)
			eh.reconnectChan <- struct{}{}
			return
		}

		time.Sleep(time.Duration(config.Config.Heartbeat) * time.Second)
	}
}

func (eh *EdgeHub) pubConnectInfo(isConnected bool) {
	// var info model.Message
	content := connect.CloudConnected
	if !isConnected {
		content = connect.CloudDisconnected
	}

	for _, group := range groupMap {
		message := model.NewMessage("").BuildRouter(messagepkg.SourceNodeConnection, group,
			messagepkg.ResourceTypeNodeConnection, messagepkg.OperationNodeConnection).FillBody(content)
		beehiveContext.SendToGroup(group, *message)
	}
}

func (eh *EdgeHub) ifRotationDone() {
	if eh.certManager.RotateCertificates {
		for {
			<-eh.certManager.Done
			eh.reconnectChan <- struct{}{}
		}
	}
}

// 启动监听 服务端设备信息更新
func (eh *EdgeHub) pulsarCounsunmer(topic string) error {
	//接收消息
	consumer, err := eh.pulsarClient.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "test-sub",
		Type:             pulsar.Shared,
	})
	if err != nil {
		klog.Errorf("create producer failed with error: %v", err)
		return err
	}

	defer consumer.Close()

	for {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			klog.Errorf("recv msg failed with error: %v", err)
			//return err
		}

		klog.Infof("Recv msg: %v", msg)

		consumer.Ack(msg)
	}

	if err = consumer.Unsubscribe(); err != nil {
		klog.Errorf("close consumer failed with error: %v", err)
		return err
	}

	return nil
}

// 启动上报 采集设备信息上报
func (eh *EdgeHub) pulsarProducer(model model.Message, topic string) error {
	// 创建生产者
	producer, err := eh.pulsarClient.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
	})

	if err != nil {
		klog.Errorf("create producer failed with error: %v", err)
		return err
	}

	defer producer.Close()

	// 发送信息至云端
	paylod, err := json.Marshal(model.Content)

	_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
		Payload: paylod,
	})
	if err != nil {
		klog.Errorf("producer send msg failed with error: %v", err)
		return err
	}

	return nil
}
