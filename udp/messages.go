// This file is part of GoSimView
// Copyright (C) 2026 KagurazakaYukari
//
// This program is dual-licensed under the GNU Affero General Public License v3.0
// and a commercial license. See LICENSE.md for details.

package udp

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/sirupsen/logrus"
	"golang.org/x/text/encoding/unicode/utf32"
)

// RealtimePosIntervalMs is the interval to request real time positional information.
// Set this to greater than 0 to enable.
// 外部配置：只在创建客户端前读取一次；运行期修改用 SetRealtimePosInterval。
var RealtimePosIntervalMs = 200

// PosIntervalModifierEnabled 控制是否按消息队列积压自适应调整实时位置间隔。
// 外部配置：只在创建客户端前读取一次；运行期修改用 SetPosIntervalModifierEnabled。
var PosIntervalModifierEnabled = true

// maxUDPPayload 是 IPv4 上单个 UDP 数据报的最大净荷（65535 - 20 字节 IP 头 - 8 字节 UDP 头）。
// 接收缓冲按该上界分配，避免截断协议允许的长报文（长服务器名、多车完圈报文）。
const maxUDPPayload = 65507

// readErrorBackoff 是接收循环遇到非致命读取错误后的退避时长。
// 已连接 UDP socket 会把对端 ICMP 错误（如 port unreachable）回传给读取，
// 这类瞬时错误不应终止接收，退避后再继续读。
const readErrorBackoff = 100 * time.Millisecond

func NewServerClient(addr string, receivePort, sendPort int, forward bool, forwardAddrStr string, forwardListenPort int, callback CallbackFunc) (*AssettoServerUDP, error) {
	remote, err := net.ResolveUDPAddr("udp", net.JoinHostPort(addr, fmt.Sprint(sendPort)))
	if err != nil {
		return nil, err
	}
	listener, err := net.DialUDP("udp", &net.UDPAddr{Port: receivePort}, remote)

	if err != nil {
		return nil, err
	}

	if runtime.GOOS != "darwin" {
		if err := listener.SetReadBuffer(1e8); err != nil {
			logrus.WithError(err).Error("unable to set read buffer")
		}
	}

	ctx, cfn := context.WithCancel(context.Background())

	u := &AssettoServerUDP{
		ctx:      ctx,
		cfn:      cfn,
		callback: callback,
		forward:  forward,
		listener: listener,

		// 包级配置在创建时读入实例，之后不再被并发读写。
		posInterval:                RealtimePosIntervalMs,
		currentPosInterval:         RealtimePosIntervalMs,
		posIntervalModifierEnabled: PosIntervalModifierEnabled,
	}

	if forward && forwardAddrStr != "" && forwardListenPort != 0 {
		forwardAddr, err := net.ResolveUDPAddr("udp", forwardAddrStr)

		if err != nil {
			return nil, err
		}

		u.forwarder, err = net.DialUDP("udp", &net.UDPAddr{IP: net.ParseIP(addr), Port: forwardListenPort}, forwardAddr)

		if err != nil {
			return nil, err
		}
	}

	u.wg.Add(3) // serve、forwardServe、消息处理协程各一个
	go u.serve()
	go u.forwardServe()
	logrus.Debugf("Started new UDP server connection")

	return u, nil
}

type CallbackFunc func(response Message)

type AssettoServerUDP struct {
	listener  *net.UDPConn
	forwarder *net.UDPConn

	forward bool

	cfn      func()
	ctx      context.Context
	callback CallbackFunc

	// 实时位置间隔的运行时状态：包级配置只在创建时读入，之后一律走方法，
	// 避免处理协程与外部写包级变量产生数据竞争。
	posIntervalMutex           sync.Mutex
	posInterval                int
	currentPosInterval         int
	posIntervalModifierEnabled bool

	closeOnce sync.Once
	closeErr  error
	wg        sync.WaitGroup
}

// RealtimePosInterval 返回请求的实时位置间隔。
func (asu *AssettoServerUDP) RealtimePosInterval() int {
	asu.posIntervalMutex.Lock()
	defer asu.posIntervalMutex.Unlock()
	return asu.posInterval
}

// SetRealtimePosInterval 更新请求的实时位置间隔；负值表示不启用。
func (asu *AssettoServerUDP) SetRealtimePosInterval(interval int) {
	asu.posIntervalMutex.Lock()
	defer asu.posIntervalMutex.Unlock()
	asu.posInterval = interval
	asu.currentPosInterval = interval
}

// CurrentRealtimePosInterval 返回当前实际下发的实时位置间隔。
func (asu *AssettoServerUDP) CurrentRealtimePosInterval() int {
	asu.posIntervalMutex.Lock()
	defer asu.posIntervalMutex.Unlock()
	return asu.currentPosInterval
}

// PosIntervalModifierEnabled 返回是否允许按队列积压自适应调整间隔。
func (asu *AssettoServerUDP) PosIntervalModifierEnabled() bool {
	asu.posIntervalMutex.Lock()
	defer asu.posIntervalMutex.Unlock()
	return asu.posIntervalModifierEnabled
}

// SetPosIntervalModifierEnabled 开关按队列积压自适应调整间隔。
func (asu *AssettoServerUDP) SetPosIntervalModifierEnabled(enabled bool) {
	asu.posIntervalMutex.Lock()
	defer asu.posIntervalMutex.Unlock()
	asu.posIntervalModifierEnabled = enabled
}

// Close 关闭连接并等待所有收发协程退出；重复调用返回首次结果。
func (asu *AssettoServerUDP) Close() error {
	asu.closeOnce.Do(asu.shutdown)
	return asu.closeErr
}

// shutdown 先取消 context 让接收与转发循环退出，再关闭 socket，
// 最后等待处理协程结束：返回后不会再有旧 callback 改动外部状态。
func (asu *AssettoServerUDP) shutdown() {
	asu.cfn()

	err := asu.listener.Close()
	if asu.forwarder != nil {
		if forwardErr := asu.forwarder.Close(); forwardErr != nil && err == nil {
			err = forwardErr
		}
	}

	asu.wg.Wait()
	asu.closeErr = err
	logrus.Debugf("Closed UDP server connection")
}

func (asu *AssettoServerUDP) forwardServe() {
	defer asu.wg.Done()

	if !asu.forward || asu.forwarder == nil {
		return
	}

	// 接收缓冲按 UDP 最大净荷分配一次，避免逐报文重复分配。
	buf := make([]byte, maxUDPPayload)

	for {
		select {
		case <-asu.ctx.Done():
			return
		default:
			n, _, err := asu.forwarder.ReadFromUDP(buf)

			if err != nil {
				if asu.ctx.Err() != nil {
					return
				}
				continue
			}

			_, err = asu.listener.Write(buf[:n])

			if err != nil {
				continue
			}
		}
	}
}

func (asu *AssettoServerUDP) serve() {
	defer asu.wg.Done()

	messageChan := make(chan []byte, 1000)
	// 唯一关闭点：读循环退出后关闭通道，处理协程据此结束，不会读到 nil 空转。
	defer close(messageChan)

	lastQueueSize := 0

	go func() {
		defer asu.wg.Done()

		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			// 收到关闭信号时立即退出，不再消费队列里残留的报文。
			select {
			case <-asu.ctx.Done():
				return
			default:
			}

			select {
			case buf, ok := <-messageChan:
				if !ok {
					return
				}

				msg, err := asu.handleMessage(bytes.NewReader(buf))

				if err != nil {
					logrus.WithError(err).Error("could not handle UDP message")
					continue
				}

				asu.callback(msg)

				if asu.forward && asu.forwarder != nil {
					// write the message to the forwarding address
					_, _ = asu.forwarder.Write(buf)
				}
			case <-ticker.C:
				asu.adjustPosInterval(len(messageChan), &lastQueueSize)
			case <-asu.ctx.Done():
				return
			}
		}
	}()

	// 接收缓冲按 UDP 最大净荷分配一次，逐报文只按实际长度复制。
	readBuf := make([]byte, maxUDPPayload)

	for {
		n, _, err := asu.listener.ReadFromUDP(readBuf)

		if err != nil {
			// 只有 ctx 取消或 socket 被关闭才真正退出；其余错误视为瞬时，
			// 记录后短退避再继续，退避期间同样可被 ctx 取消。
			if asu.ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			logrus.WithError(err).Error("could not read from UDP, retrying")
			select {
			case <-time.After(readErrorBackoff):
			case <-asu.ctx.Done():
				return
			}
			continue
		}

		msg := make([]byte, n)
		copy(msg, readBuf[:n])

		select {
		case messageChan <- msg:
		case <-asu.ctx.Done():
			return
		}
	}
}

// adjustPosInterval 按消息队列积压调整下发的实时位置间隔。
func (asu *AssettoServerUDP) adjustPosInterval(currentQueueSize int, lastQueueSize *int) {
	interval, changed := asu.nextPosInterval(currentQueueSize, *lastQueueSize)
	*lastQueueSize = currentQueueSize

	if !changed {
		return
	}

	logrus.Debugf("Adjusting real time pos interval, is now: %d", interval)

	if err := asu.SendMessage(NewEnableRealtimePosInterval(interval)); err != nil {
		logrus.WithError(err).Error("Could not send realtime pos interval adjustment")
	}
}

// nextPosInterval 计算新的实时位置间隔；第二个返回值表示是否需下发。
func (asu *AssettoServerUDP) nextPosInterval(currentQueueSize, lastQueueSize int) (int, bool) {
	asu.posIntervalMutex.Lock()
	defer asu.posIntervalMutex.Unlock()

	if asu.posInterval < 0 || !asu.posIntervalModifierEnabled {
		// 未设置实时位置间隔或已关闭自适应，不需要检查是否跟得上消息
		return asu.currentPosInterval, false
	}

	switch {
	case currentQueueSize > lastQueueSize:
		logrus.Warnf("Can't keep up! queue size: %d vs %d: changed by %d", currentQueueSize, lastQueueSize, currentQueueSize-lastQueueSize)

		// 尽早更新，但保持在合理范围内
		if currentQueueSize > 5 { // 此时已经落后半秒
			asu.currentPosInterval += (currentQueueSize * 2) + 1
			return asu.currentPosInterval, true
		}
	case currentQueueSize <= lastQueueSize && currentQueueSize < 5 && asu.currentPosInterval > asu.posInterval:
		logrus.Debugf("Catching up, queue size: %d vs %d: changed by %d", currentQueueSize, lastQueueSize, currentQueueSize-lastQueueSize)

		if asu.currentPosInterval-1 >= asu.posInterval {
			asu.currentPosInterval--
			return asu.currentPosInterval, true
		}
	}

	return asu.currentPosInterval, false
}

func readStringW(r io.Reader) string {
	return readString(r, 4)
}

func readString(r io.Reader, sizeMultiplier int) string {
	var size uint8
	err := binary.Read(r, binary.LittleEndian, &size)

	if err != nil {
		return ""
	}

	b := make([]byte, int(size)*sizeMultiplier)

	err = binary.Read(r, binary.LittleEndian, &b)

	if err != nil {
		return ""
	}

	if sizeMultiplier == 4 {
		bs, err := utf32.UTF32(utf32.LittleEndian, utf32.IgnoreBOM).NewDecoder().Bytes(b)

		if err != nil {
			return ""
		}

		return string(bs)
	}

	return string(b)
}

// writeStringW 按 Kunos stringW 编码：码点数 uint8 + 无 BOM 的 UTF-32LE。
func writeStringW(w io.Writer, value string) error {
	if !utf8.ValidString(value) {
		return errors.New("UDP 字符串不是有效 UTF-8")
	}
	count := utf8.RuneCountInString(value)
	if count > 255 {
		return errors.New("UDP 字符串超过 255 个码点")
	}
	encoded, err := utf32.UTF32(utf32.LittleEndian, utf32.IgnoreBOM).NewEncoder().Bytes([]byte(value))
	if err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint8(count)); err != nil {
		return err
	}
	_, err = w.Write(encoded)
	return err
}

func (asu *AssettoServerUDP) SendMessage(message Message) error {
	switch a := message.(type) {
	case EnableRealtimePosInterval:
		if asu.PosIntervalModifierEnabled() {
			return binary.Write(asu.listener, binary.LittleEndian, a)
		}

		return nil

	case GetSessionInfo:
		buf := new(bytes.Buffer)
		if err := binary.Write(buf, binary.LittleEndian, a.Event()); err != nil {
			return err
		}
		if err := binary.Write(buf, binary.LittleEndian, a.SessionIndex); err != nil {
			return err
		}
		_, err := asu.listener.Write(buf.Bytes())
		return err

	case GetCarInfo:
		_, err := asu.listener.Write([]byte{byte(a.Event()), byte(a.CarID)})
		return err

	case SetSessionInfo:
		buf := new(bytes.Buffer)
		if err := binary.Write(buf, binary.LittleEndian, a.Event()); err != nil {
			return err
		}
		if err := binary.Write(buf, binary.LittleEndian, a.SessionIndex); err != nil {
			return err
		}
		if err := writeStringW(buf, a.Name); err != nil {
			return err
		}
		if err := binary.Write(buf, binary.LittleEndian, a.Type); err != nil {
			return err
		}
		for _, value := range []uint32{a.Laps, a.Time, a.WaitTime} {
			if err := binary.Write(buf, binary.LittleEndian, value); err != nil {
				return err
			}
		}
		_, err := asu.listener.Write(buf.Bytes())
		return err

	case *RestartSession, *NextSession:
		return binary.Write(asu.listener, binary.LittleEndian, a.Event())

	case *SendChat:
		buf := new(bytes.Buffer)

		if err := binary.Write(buf, binary.LittleEndian, a.EventType); err != nil {
			return err
		}

		if err := binary.Write(buf, binary.LittleEndian, a.CarID); err != nil {
			return err
		}

		if err := binary.Write(buf, binary.LittleEndian, a.Len); err != nil {
			return err
		}

		if _, err := buf.Write(a.UTF32Encoded); err != nil {
			return err
		}

		if _, err := io.Copy(asu.listener, buf); err != nil {
			return err
		}

		return nil

	case *BroadcastChat:
		buf := new(bytes.Buffer)

		if err := binary.Write(buf, binary.LittleEndian, a.EventType); err != nil {
			return err
		}

		if err := binary.Write(buf, binary.LittleEndian, a.Len); err != nil {
			return err
		}

		if _, err := buf.Write(a.UTF32Encoded); err != nil {
			return err
		}

		if _, err := io.Copy(asu.listener, buf); err != nil {
			return err
		}

		return nil

	case *AdminCommand:
		buf := new(bytes.Buffer)

		if err := binary.Write(buf, binary.LittleEndian, a.EventType); err != nil {
			return err
		}

		if err := binary.Write(buf, binary.LittleEndian, a.Len); err != nil {
			return err
		}

		if _, err := buf.Write(a.UTF32Encoded); err != nil {
			return err
		}

		if _, err := io.Copy(asu.listener, buf); err != nil {
			return err
		}

		return nil

	case *KickUser:
		buf := new(bytes.Buffer)

		if err := binary.Write(buf, binary.LittleEndian, a.EventType); err != nil {
			return err
		}

		if err := binary.Write(buf, binary.LittleEndian, a.CarID); err != nil {
			return err
		}

		if _, err := io.Copy(asu.listener, buf); err != nil {
			return err
		}

		return nil

	}

	return errors.New("udp: invalid message type")
}

func (asu *AssettoServerUDP) handleMessage(r io.Reader) (Message, error) {
	var messageType uint8

	err := binary.Read(r, binary.LittleEndian, &messageType)

	if err != nil {
		return nil, err
	}

	eventType := Event(messageType)

	var response Message

	switch eventType {
	case EventNewConnection, EventConnectionClosed:
		driverName := readStringW(r)
		driverGUID := readStringW(r)

		var carID CarID

		err = binary.Read(r, binary.LittleEndian, &carID)

		if err != nil {
			return nil, err
		}

		carMode := readString(r, 1)
		carSkin := readString(r, 1)

		response = SessionCarInfo{
			CarID:      carID,
			DriverName: driverName,
			DriverGUID: DriverGUID(driverGUID),
			CarModel:   carMode,
			CarSkin:    carSkin,
			EventType:  eventType,
		}

	case EventCarUpdate:
		carUpdate := CarUpdate{}

		err := binary.Read(r, binary.LittleEndian, &carUpdate)

		if err != nil {
			return nil, err
		}

		response = carUpdate
	case EventCarInfo:
		var carID CarID

		err = binary.Read(r, binary.LittleEndian, &carID)

		if err != nil {
			return nil, err
		}

		var isConnected uint8

		err = binary.Read(r, binary.LittleEndian, &isConnected)

		if err != nil {
			return nil, err
		}

		response = CarInfo{
			CarID:       carID,
			IsConnected: isConnected != 0,
			CarModel:    readStringW(r),
			CarSkin:     readStringW(r),
			DriverName:  readStringW(r),
			DriverTeam:  readStringW(r),
			DriverGUID:  DriverGUID(readStringW(r)),
		}
	case EventEndSession:
		filename := readStringW(r)

		response = EndSession(filename)
	case EventVersion:
		var version uint8

		err = binary.Read(r, binary.LittleEndian, &version)

		if err != nil {
			return nil, err
		}

		response = Version(version)
	case EventChat:
		var carID CarID

		err := binary.Read(r, binary.LittleEndian, &carID)

		if err != nil {
			return nil, err
		}

		message := readStringW(r)

		response = Chat{
			CarID:   carID,
			Message: message,
		}
	case EventClientLoaded:
		var carID CarID

		err := binary.Read(r, binary.LittleEndian, &carID)

		if err != nil {
			return nil, err
		}

		response = ClientLoaded(carID)
	case EventNewSession, EventSessionInfo:
		sessionInfo := SessionInfo{}

		err := binary.Read(r, binary.LittleEndian, &sessionInfo.Version)

		if err != nil {
			return nil, err
		}

		err = binary.Read(r, binary.LittleEndian, &sessionInfo.SessionIndex)

		if err != nil {
			return nil, err
		}

		err = binary.Read(r, binary.LittleEndian, &sessionInfo.CurrentSessionIndex)

		if err != nil {
			return nil, err
		}

		err = binary.Read(r, binary.LittleEndian, &sessionInfo.SessionCount)

		if err != nil {
			return nil, err
		}

		sessionInfo.ServerName = readStringW(r)
		sessionInfo.Track = readString(r, 1)
		sessionInfo.TrackConfig = readString(r, 1)
		sessionInfo.Name = readString(r, 1)

		err = binary.Read(r, binary.LittleEndian, &sessionInfo.Type)

		if err != nil {
			return nil, err
		}

		err = binary.Read(r, binary.LittleEndian, &sessionInfo.Time)

		if err != nil {
			return nil, err
		}

		err = binary.Read(r, binary.LittleEndian, &sessionInfo.Laps)

		if err != nil {
			return nil, err
		}

		err = binary.Read(r, binary.LittleEndian, &sessionInfo.WaitTime)

		if err != nil {
			return nil, err
		}

		err = binary.Read(r, binary.LittleEndian, &sessionInfo.AmbientTemp)

		if err != nil {
			return nil, err
		}

		err = binary.Read(r, binary.LittleEndian, &sessionInfo.RoadTemp)

		if err != nil {
			return nil, err
		}

		sessionInfo.WeatherGraphics = readString(r, 1)

		err = binary.Read(r, binary.LittleEndian, &sessionInfo.ElapsedMilliseconds)

		if err != nil {
			return nil, err
		}

		sessionInfo.EventType = eventType

		response = sessionInfo

		if interval := asu.RealtimePosInterval(); interval > 0 && eventType == EventNewSession {
			// 副作用失败不能丢弃已解析的报文：会话信息必须送达回调。
			if err := asu.SendMessage(NewEnableRealtimePosInterval(interval)); err != nil {
				logrus.WithError(err).Error("could not enable realtime pos interval on new session")
			}
		}
	case EventError:
		message := readStringW(r)

		response = ServerError{errors.New(message)}

	case EventLapCompleted:
		lapCompleted := lapCompletedInternal{}

		err := binary.Read(r, binary.LittleEndian, &lapCompleted)

		if err != nil {
			return nil, err
		}

		lc := LapCompleted{
			CarID:     lapCompleted.CarID,
			LapTime:   lapCompleted.LapTime,
			Cuts:      lapCompleted.Cuts,
			CarsCount: lapCompleted.CarsCount,
		}

		for i := uint8(0); i < lapCompleted.CarsCount; i++ {
			var car LapCompletedCar
			err := binary.Read(r, binary.LittleEndian, &car)

			if err != nil {
				return nil, err
			}

			lc.Cars = append(lc.Cars, &car)
		}

		// 原始 Kunos 样例和 MsgLapCompleted 均在列表后读取 float32 抓地力。
		if err := binary.Read(r, binary.LittleEndian, &lc.GripLevel); err != nil {
			return nil, fmt.Errorf("读取完成圈尾部抓地力失败: %w", err)
		}
		response = lc
	case EventClientEvent:
		var collisionType uint8

		err := binary.Read(r, binary.LittleEndian, &collisionType)

		if err != nil {
			return nil, err
		}

		if Event(collisionType) == EventCollisionWithCar {
			collision := CollisionWithCar{}

			err := binary.Read(r, binary.LittleEndian, &collision)

			if err != nil {
				return nil, err
			}

			response = collision
		} else if Event(collisionType) == EventCollisionWithEnv {
			collision := CollisionWithEnvironment{}

			err := binary.Read(r, binary.LittleEndian, &collision)

			if err != nil {
				return nil, err
			}

			response = collision
		}

	default:
		buf := new(bytes.Buffer)

		_, err = buf.ReadFrom(r)

		if err != nil {
			return nil, err
		}

		fmt.Println("Unknown response type", eventType)
		fmt.Printf("%x\n", buf)

		return nil, errors.New("unknown response type")
	}

	return response, nil
}
