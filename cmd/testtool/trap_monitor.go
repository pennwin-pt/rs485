// trap_monitor.go — 多设备长连接轮询 + 读卡查询模块
//
// 本文件与 rs485_client.go 同属 package main，可以直接复用其中已有的
// crc16Modbus / connectTimeout 等函数与常量，无需重复定义。
//
// 使用方式（在现有 go 文件里调用）：
//
//	func main() {
//	    ...
//	    // gatewayAddr 是 RS485 网关的 TCP 地址，例如 "192.168.2.170:defaultPort"
//	    StartTrapDetectionSystem(gatewayAddr)
//	    ... // 主流程继续做其他事情，两个后台线程会一直跑
//	}
//
// 整体分为两个后台线程：
//
//  1. pollLoop：与网关建立一个长连接，按 deviceAddressCodes 列表循环，
//     每隔 pollInterval（10ms）向下一个设备地址发送一次轮询指令，
//     解析回包倒数第三个字节的踩中状态，写入 trapState 共享内存。
//
//  2. cardReaderLoop：定期读取 trapState，对当前处于“踩中”状态的电机，
//     通过 motorReaderMap 找到对应读卡器的 IP/端口，用《UHF RFID通信协议
//     (CPH)》里的“主动盘存标签(0x22)”指令去查询一次当前能读到的卡号列表，
//     写入 cardStore 共享内存，并打印到终端。
//     未来前端会通过接口读取 cardStore 的内容，这部分不需要在这里实现。
package main

import (
	"bufio"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/pennwin-pt/rs485"
	"io"
	"net"
	"sort"
	"strings"
	"sync"
	"time"
)

// ==================== 配置区 ====================

// deviceAddressCodes 是需要建立长连接轮询的设备地址码列表（目前 15 个）。
// TODO：现在先写成常量列表，后续如果需要从外部传入，可以把这里改成
// StartTrapDetectionSystem 的参数，调用方传什么就轮询什么。
var deviceAddressCodes = []byte{
	0x01, 0x02, 0x03, 0x04, 0x05,
	0x06, 0x07, 0x08, 0x09, 0x0A,
	0x0B, 0x0C, 0x0D, 0x0E, 0x0F,
}

// pollCommandBody 是轮询指令里除地址码之外的固定部分：
// 功能码 02（读离散输入）+ 起始地址 0x0002 + 数量 0x0005。
// 加上地址码后正好是 6 字节（与 rs485_client.go 里的 payloadLen 一致），
// 发送时会自动在末尾追加 2 字节 CRC，总共 8 字节。
var pollCommandBody = []byte{0x02, 0x00, 0x02, 0x00, 0x05}

// pollInterval 是轮询节奏：每隔多久向“下一个”设备发送一条指令。
// 15 个设备按顺序轮询一圈大约耗时 15 * pollInterval。
const pollInterval = 50 * time.Millisecond

// pollReadTimeout 是单次轮询等待回包的超时时间。
// 特意设置得比较小，避免个别设备无响应时拖慢整体轮询节奏。
const pollReadTimeout = 50 * time.Millisecond

// triggerChannelMask 配置“哪几路被踩中才算触发”，按位表示：
//
//	bit0 (0x01) = 第 1 路   bit1 (0x02) = 第 2 路   bit2 (0x04) = 第 3 路
//	bit3 (0x08) = 第 4 路   bit4 (0x10) = 第 5 路
//
// 默认值 1：只要状态字节里第 1 路对应的 bit 被置位（比如收到 01、03 这种
// 只要包含 0x01 的值），就认为该设备的电机进入“被踩中”状态。
// 如果想同时关注第 1、2 路，可以改成 0x01 | 0x02 = 0x03，以此类推。
var triggerChannelMask byte = 0x01

// ReaderInfo 描述一个电机对应的读卡器网络地址，以及该读卡器在 CPH 协议里的设备地址。
type ReaderInfo struct {
	IP   string
	Port string

	// Address 是该读卡器在《UHF RFID通信协议(CPH)》帧头里的设备地址字段。
	// 0x0000 是广播地址，所有读卡器都会响应；如果想"点对点"地只让指定地址的
	// 读卡器响应（比如同一网口/网段下挂了多台读卡器，需要区分开），把这里
	// 填成该读卡器实际配置的地址码即可。
	Address uint16
}

// motorReaderMap 保存 电机（设备地址码） -> 读卡器 IP/端口 的映射。
// TODO：这里先写死 15 条示例数据，实际部署时请替换为真实的读卡器地址，
// 或者改造成从配置文件 / 数据库加载。
// Address 先都写成 0x0000（广播），实际部署时请按每台读卡器真实配置的地址码填写，
// 比如 0x01、0x02……这样查询时就会带上对应的地址，只由该台读卡器响应。
var motorReaderMap = map[byte]ReaderInfo{
	0x00: {IP: defaultHost, Port: defaultCardReaderPort, Address: 0x0000},
	0x01: {IP: defaultHost, Port: defaultCardReaderPort, Address: 0x0001},
	0x02: {IP: defaultHost, Port: defaultCardReaderPort, Address: 0x0002},
	0x03: {IP: defaultHost, Port: defaultCardReaderPort, Address: 0x0003},
	0x04: {IP: defaultHost, Port: defaultCardReaderPort, Address: 0x0004},
	0x05: {IP: defaultHost, Port: defaultCardReaderPort, Address: 0x0005},
	0x06: {IP: defaultHost, Port: defaultCardReaderPort, Address: 0x0000},
	0x07: {IP: defaultHost, Port: defaultCardReaderPort, Address: 0x0000},
	0x08: {IP: defaultHost, Port: defaultCardReaderPort, Address: 0x0000},
	0x09: {IP: defaultHost, Port: defaultCardReaderPort, Address: 0x0000},
	0x0A: {IP: defaultHost, Port: defaultCardReaderPort, Address: 0x0000},
	0x0B: {IP: defaultHost, Port: defaultCardReaderPort, Address: 0x0000},
	0x0C: {IP: defaultHost, Port: defaultCardReaderPort, Address: 0x0000},
	0x0D: {IP: defaultHost, Port: defaultCardReaderPort, Address: 0x0000},
	0x0E: {IP: defaultHost, Port: defaultCardReaderPort, Address: 0x0000},
	0x0F: {IP: defaultHost, Port: defaultCardReaderPort, Address: 0x0000},
}

// cardQueryInterval 是读卡线程扫描 trapState、向读卡器发起查询的间隔。
// 不需要像轮询那样快，几百毫秒一次即可。
const cardQueryInterval = 200 * time.Millisecond

// readerFrameTimeout 是等待读卡器返回一帧数据的超时时间。
const readerFrameTimeout = 500 * time.Millisecond

// debugTraffic 控制是否把每一条发送/接收的原始报文打印到终端。
// 轮询线程每个设备 50ms 发一条、15 个设备一圈，开启后打印会比较密集；
// 如果只是想看触发状态变化、不需要看每一帧原始数据，可以把这里改成 false。
var debugTraffic = true

// ==================== 共享内存：踩中状态 ====================

// trappedState 记录每个电机（设备地址码）当前是否处于“被踩中”状态。
// 写者：pollLoop（轮询线程）；读者：cardReaderLoop（读卡线程）。
type trappedState struct {
	mu      sync.RWMutex
	trapped map[byte]bool
}

var trapState = &trappedState{trapped: make(map[byte]bool)}

// SetAndCheckChanged 更新状态，返回这次状态是否发生了变化（用于减少重复打印）。
func (s *trappedState) SetAndCheckChanged(motorID byte, isTrapped bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	prev, ok := s.trapped[motorID]
	s.trapped[motorID] = isTrapped
	return !ok || prev != isTrapped
}

// Snapshot 返回当前所有电机踩中状态的一份拷贝，避免调用方长时间持锁。
func (s *trappedState) Snapshot() map[byte]bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[byte]bool, len(s.trapped))
	for k, v := range s.trapped {
		out[k] = v
	}
	return out
}

// ==================== 共享内存：卡号列表 ====================

// cardState 记录每个电机当前踩中状态下，读卡器读到的卡号列表。
// 写者：cardReaderLoop（读卡线程）；读者：未来的前端接口层（本文件不实现）。
type cardState struct {
	mu    sync.RWMutex
	cards map[byte][]string
}

var cardStore = &cardState{cards: make(map[byte][]string)}

// Set 写入某个电机当前的卡号列表；传入空列表时视为清空。
func (s *cardState) Set(motorID byte, cards []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(cards) == 0 {
		delete(s.cards, motorID)
		return
	}
	s.cards[motorID] = cards
}

// Snapshot 返回当前所有电机卡号列表的一份拷贝，供前端接口层未来调用。
func (s *cardState) Snapshot() map[byte][]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[byte][]string, len(s.cards))
	for k, v := range s.cards {
		cp := make([]string, len(v))
		copy(cp, v)
		out[k] = cp
	}
	return out
}

// ==================== 对外入口 ====================

// StartTrapDetectionSystem 启动两个后台线程：设备轮询线程 + 读卡查询线程。
// gatewayAddr 是 RS485 网关的 TCP 地址，例如 "192.168.2.170:defaultPort"。
// 调用后立即返回，不会阻塞当前线程。
func StartTrapDetectionSystem(gatewayAddr string) {
	go pollLoop(gatewayAddr)
	go cardReaderLoop()
}

// ==================== 线程一：设备轮询 ====================

// pollLoop 与网关建立一个长连接，循环轮询 deviceAddressCodes 里的每个设备。
func pollLoop(addr string) {
	conn := connectWithRetry(addr)
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		for _, deviceAddr := range deviceAddressCodes {
			payload := buildPollCommand(deviceAddr)

			if err := conn.SetDeadline(time.Now().Add(pollReadTimeout)); err != nil {
				fmt.Printf("[轮询线程] 设置超时失败：%v\n", err)
			}

			if _, err := conn.Write(payload); err != nil {
				fmt.Printf("[轮询线程] 设备 %02X 发送失败：%v，尝试重连...\n", deviceAddr, err)
				conn.Close()
				conn = connectWithRetry(addr)
				reader = bufio.NewReader(conn)
				continue
			}
			if debugTraffic {
				fmt.Printf("[轮询线程][调试] → 设备 %02X 发送：%s\n", deviceAddr, rs485.FormatHex(payload))
			}

			resp, err := readPollResponse(conn, reader)
			if err != nil {
				fmt.Printf("[轮询线程] 设备 %02X 读取回包出错：%v，尝试重连...\n", deviceAddr, err)
				conn.Close()
				conn = connectWithRetry(addr)
				reader = bufio.NewReader(conn)
				continue
			}
			if resp != nil {
				if debugTraffic {
					fmt.Printf("[轮询线程][调试] ← 设备 %02X 收到：%s\n", deviceAddr, rs485.FormatHex(resp))
				}
				handlePollResponse(deviceAddr, resp)
			} else if debugTraffic {
				fmt.Printf("[轮询线程][调试] ← 设备 %02X 未收到回包（超时）\n", deviceAddr)
			}

			time.Sleep(pollInterval)
		}
	}
}

// connectWithRetry 不断尝试连接网关，直到成功为止。
func connectWithRetry(addr string) net.Conn {
	for {
		conn, err := net.DialTimeout("tcp", addr, connectTimeout)
		if err == nil {
			fmt.Printf("[轮询线程] 已连接设备网关 %s\n", addr)
			return conn
		}
		fmt.Printf("[轮询线程] 连接 %s 失败：%v，1 秒后重试\n", addr, err)
		time.Sleep(1 * time.Second)
	}
}

// buildPollCommand 拼出“地址码 + 固定指令体 + CRC”的完整轮询报文。
func buildPollCommand(deviceAddr byte) []byte {
	body := make([]byte, 0, 1+len(pollCommandBody))
	body = append(body, deviceAddr)
	body = append(body, pollCommandBody...)

	crc := crc16Modbus(body)
	payload := make([]byte, 0, len(body)+2)
	payload = append(payload, body...)
	payload = append(payload, byte(crc&0xFF), byte(crc>>8))
	return payload
}

// readPollResponse 读取固定 8 字节的回包。
// 超时返回 (nil, nil)：本次跳过，不算错误，不会触发重连；
// 其他错误（比如连接被对端关闭）返回非 nil 的 err，由调用方决定重连。
func readPollResponse(conn net.Conn, reader *bufio.Reader) ([]byte, error) {
	buf := make([]byte, 8)
	_, err := io.ReadFull(reader, buf)
	if err != nil {
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return nil, nil
		}
		return nil, err
	}
	return buf, nil
}

// handlePollResponse 解析一条回包，判断是否触发踩中，并更新共享状态。
// 回包格式类似：05 02 00 02 00 01 19 8E
// 倒数第三个字节（示例里的 01）是 5 路踩中状态的位掩码。
func handlePollResponse(deviceAddr byte, resp []byte) {
	if len(resp) < 3 {
		fmt.Printf("[轮询线程] 设备 %02X 回包长度异常：% X\n", deviceAddr, resp)
		return
	}

	statusByte := resp[len(resp)-3]
	triggered := statusByte&triggerChannelMask != 0

	changed := trapState.SetAndCheckChanged(deviceAddr, triggered)
	if !changed {
		return // 状态没变化就不重复打印，避免 10ms 一条指令把终端刷屏
	}

	if triggered {
		fmt.Printf("[轮询线程] 电机 %02X 触发踩中，状态字节=%02X（关注掩码=%02X）\n", deviceAddr, statusByte, triggerChannelMask)
	} else {
		fmt.Printf("[轮询线程] 电机 %02X 已恢复，状态字节=%02X\n", deviceAddr, statusByte)
	}
}

// ==================== 线程二：读卡查询 ====================

// cardReaderLoop 定期扫描 trapState，对处于踩中状态的电机去查询读卡器，
// 把卡号列表写入 cardStore，并打印到终端。
func cardReaderLoop() {
	for {
		trapped := trapState.Snapshot()

		for motorID, isTrapped := range trapped {
			if !isTrapped {
				cardStore.Set(motorID, nil) // 未踩中，清空该电机的卡号记录
				continue
			}

			reader, ok := motorReaderMap[motorID]
			if !ok {
				fmt.Printf("[读卡线程] 电机 %02X 未配置读卡器地址，跳过\n", motorID)
				continue
			}

			cards, err := queryCardsFromReader(reader)
			if err != nil {
				fmt.Printf("[读卡线程] 查询电机 %02X 对应读卡器 %s:%s 失败：%v\n", motorID, reader.IP, reader.Port, err)
				continue
			}

			cardStore.Set(motorID, cards)
			fmt.Printf("[读卡线程] 电机 %02X 当前卡号：%v\n", motorID, cards)
		}

		time.Sleep(cardQueryInterval)
	}
}

// ==================== UHF RFID 通信协议 (CPH) ====================
//
// 协议来自《UHF RFID通信协议》文档，帧格式：
//
//	'R' 'F' | FrameType(1) | Address(2, MSB LSB) | FrameCode(1) | ParamLen(2, MSB LSB) | Params(N) | Checksum(1)
//
// FrameType：0x00=命令帧 0x01=响应帧 0x02=通知帧
// Checksum：从 'R' 开始一直到 Checksum 前一个字节的累加和，取反加一（结果取低 8 位）。
//
// 本文件只用到“主动盘存标签(0x22)”这一条指令：
// 发送后设备读一次标签就停止，并在同一个响应帧里把状态和读到的标签数据都带回来，
// 非常适合“按需查询当前有哪些卡”这种一次性查询场景。

const (
	rfFrameTypeCommand  byte = 0x00
	rfFrameTypeResponse byte = 0x01
	rfFrameTypeNotify   byte = 0x02

	rfCmdSingleInventory byte = 0x22 // 主动盘存标签：读一次就停

	rfTLVStatus    byte = 0x07 // 状态 TLV
	rfTLVSingleTag byte = 0x50 // 单张标签 TLV（内部还嵌套 EPC/RSSI/Time 等 TLV）
	rfTLVEPC       byte = 0x01 // EPC TLV，值就是卡号
)

// buildRFFrame 拼出一条完整的 CPH 协议帧（命令帧用 rfFrameTypeCommand）。
func buildRFFrame(frameType byte, address uint16, frameCode byte, params []byte) []byte {
	frame := make([]byte, 0, 8+len(params))
	frame = append(frame, 'R', 'F', frameType, byte(address>>8), byte(address&0xFF), frameCode)
	frame = append(frame, byte(len(params)>>8), byte(len(params)&0xFF))
	frame = append(frame, params...)
	frame = append(frame, rfChecksum(frame))
	return frame
}

// rfFrame 是解析出来的一条 CPH 协议帧。
type rfFrame struct {
	FrameType byte
	Address   uint16
	FrameCode byte
	Params    []byte
}

// readRFFrame 从连接里读取并校验一条完整的 CPH 协议帧。
// 除了解析结果和 error，还会把目前为止实际读到的原始字节一并返回（哪怕解析失败/读取出错也会返回已读到的部分），
// 方便调用方在打印调试信息时，无论成功失败都能看到“收到了什么”。
func readRFFrame(reader *bufio.Reader) (*rfFrame, []byte, error) {
	var raw []byte

	header := make([]byte, 2)
	n, err := io.ReadFull(reader, header)
	raw = append(raw, header[:n]...)
	if err != nil {
		return nil, raw, err
	}
	if header[0] != 'R' || header[1] != 'F' {
		return nil, raw, fmt.Errorf("帧头不是 RF：% X", header)
	}

	rest := make([]byte, 6) // FrameType(1) + Address(2) + FrameCode(1) + ParamLen(2)
	n, err = io.ReadFull(reader, rest)
	raw = append(raw, rest[:n]...)
	if err != nil {
		return nil, raw, err
	}
	paramLen := int(rest[4])<<8 | int(rest[5])

	params := make([]byte, paramLen)
	if paramLen > 0 {
		n, err = io.ReadFull(reader, params)
		raw = append(raw, params[:n]...)
		if err != nil {
			return nil, raw, err
		}
	}

	checksumByte := make([]byte, 1)
	n, err = io.ReadFull(reader, checksumByte)
	raw = append(raw, checksumByte[:n]...)
	if err != nil {
		return nil, raw, err
	}

	full := append(append(header, rest...), params...)
	if want := rfChecksum(full); want != checksumByte[0] {
		return nil, raw, fmt.Errorf("校验和不匹配：期望 %02X 实际 %02X", want, checksumByte[0])
	}

	return &rfFrame{
		FrameType: rest[0],
		Address:   uint16(rest[1])<<8 | uint16(rest[2]),
		FrameCode: rest[3],
		Params:    params,
	}, raw, nil
}

// rfTLV 是一条通用 TLV：1 字节类型 + 1 字节长度 + N 字节值。
type rfTLV struct {
	Type  byte
	Value []byte
}

// parseRFTLVs 按 Type(1) Length(1) Value(N) 的格式顺序切出所有 TLV。
func parseRFTLVs(data []byte) []rfTLV {
	var out []rfTLV
	i := 0
	for i+2 <= len(data) {
		t := data[i]
		l := int(data[i+1])
		i += 2
		if i+l > len(data) {
			break // 数据不完整，直接放弃剩余部分
		}
		out = append(out, rfTLV{Type: t, Value: data[i : i+l]})
		i += l
	}
	return out
}

// extractCardsFromTagTLVs 从响应帧的参数里找出所有“单张标签 TLV(0x50)”，
// 再从每一个里面取出 EPC TLV(0x01) 的值作为卡号（十六进制字符串）。
func extractCardsFromTagTLVs(params []byte) []string {
	var cards []string
	for _, t := range parseRFTLVs(params) {
		if t.Type != rfTLVSingleTag {
			continue
		}
		for _, inner := range parseRFTLVs(t.Value) {
			if inner.Type == rfTLVEPC {
				cards = append(cards, strings.ToUpper(hex.EncodeToString(inner.Value)))
			}
		}
	}
	return cards
}

// queryCardsFromReader 连接指定读卡器，发送“主动盘存标签(0x22)”指令查询一次，
// 解析响应帧，返回当前读到的所有卡号（EPC，十六进制字符串，大写）。
func queryCardsFromReader(reader ReaderInfo) ([]string, error) {
	addr := net.JoinHostPort(reader.IP, reader.Port)

	conn, err := net.DialTimeout("tcp", addr, connectTimeout)
	if err != nil {
		return nil, fmt.Errorf("连接失败: %w", err)
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(readerFrameTimeout)); err != nil {
		return nil, err
	}

	// 使用 reader.Address：0x0000 是广播地址所有设备都会响应；如果 motorReaderMap
	// 里给这台读卡器配置了非 0 的具体地址，则只有该地址的读卡器会响应，
	// 便于同一网段/网口下有多台读卡器时做点对点查询。
	cmd := buildRFFrame(rfFrameTypeCommand, reader.Address, rfCmdSingleInventory, nil)
	if debugTraffic {
		fmt.Printf("[读卡线程][调试] → 地址=%04X 发送：%s\n", reader.Address, rs485.FormatHex(cmd))
	}
	if _, err := conn.Write(cmd); err != nil {
		return nil, fmt.Errorf("发送失败: %w", err)
	}

	resp, raw, err := readRFFrame(bufio.NewReader(conn))
	if err != nil {
		if debugTraffic {
			fmt.Printf("[读卡线程][调试] ← 地址=%04X 收到（解析失败，共 %d 字节原始数据）：%s\n", reader.Address, len(raw), rs485.FormatHex(raw))
		}
		return nil, fmt.Errorf("读取响应失败: %w", err)
	}
	if debugTraffic {
		fmt.Printf("[读卡线程][调试] ← 地址=%04X 收到完整原始报文（%d 字节）：%s\n", reader.Address, len(raw), rs485.FormatHex(raw))
		fmt.Printf("[读卡线程][调试] ← 地址=%04X 解析结果：FrameType=%02X FrameCode=%02X Params=%s\n",
			reader.Address, resp.FrameType, resp.FrameCode, rs485.FormatHex(resp.Params))
	}
	if resp.FrameType != rfFrameTypeResponse {
		return nil, fmt.Errorf("响应帧类型不符合预期：FrameType=%02X（期望响应帧 %02X）", resp.FrameType, rfFrameTypeResponse)
	}
	// 注：协议文档里“主动盘存标签(0x22)”响应示例表格里的 FrameCode 写的是 0x21，
	// 疑似文档笔误（前一节 0x21 表格的残留），这里不对 FrameCode 做硬性校验，
	// 只要是响应帧就尝试解析，避免因为文档笔误误判失败。

	// 响应参数里第一个 TLV 是状态(0x07)，之后跟着 0~N 个标签 TLV(0x50)。
	tlvs := parseRFTLVs(resp.Params)
	if len(tlvs) > 0 && tlvs[0].Type == rfTLVStatus && len(tlvs[0].Value) > 0 && tlvs[0].Value[0] != 0x00 {
		return nil, fmt.Errorf("读写器返回状态码 %02X（非成功）", tlvs[0].Value[0])
	}

	return extractCardsFromTagTLVs(resp.Params), nil
}

// QueryCardsVerbose 和 queryCardsFromReader 逻辑完全一致，
// 区别是会把拼装的指令、读卡器返回的原始数据、TLV 解析的中间过程、
// 最终提炼出的卡号都打印到终端，供命令行手动调试读卡器时使用。
func QueryCardsVerbose(reader ReaderInfo, isDefault bool) ([]string, error) {
	addr := ""
	if isDefault {
		addr = net.JoinHostPort(defaultHost, defaultCardReaderPort)
	} else {
		addr = net.JoinHostPort(reader.IP, reader.Port)
	}
	fmt.Printf("  → 连接读卡器 %s ...\n", addr)

	conn, err := net.DialTimeout("tcp", addr, connectTimeout)
	if err != nil {
		return nil, fmt.Errorf("连接失败: %w", err)
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(readerFrameTimeout)); err != nil {
		return nil, err
	}

	cmd := buildRFFrame(rfFrameTypeCommand, reader.Address, rfCmdSingleInventory, nil)
	fmt.Printf("  → 发送指令（地址=%04X）：%s\n", reader.Address, rs485.FormatHex(cmd))

	if _, err := conn.Write(cmd); err != nil {
		return nil, fmt.Errorf("发送失败: %w", err)
	}

	resp, raw, err := readRFFrame(bufio.NewReader(conn))
	if err != nil {
		fmt.Printf("  ← 收到原始数据（%d 字节，解析失败）：%s\n", len(raw), rs485.FormatHex(raw))
		return nil, fmt.Errorf("读取响应失败: %w", err)
	}
	fmt.Printf("  ← 收到完整原始报文（%d 字节）：%s\n", len(raw), rs485.FormatHex(raw))
	fmt.Printf("  ← 收到响应：FrameType=%02X Address=%04X FrameCode=%02X ParamLen=%d\n",
		resp.FrameType, resp.Address, resp.FrameCode, len(resp.Params))
	fmt.Printf("  ← 参数原始字节：%s\n", rs485.FormatHex(resp.Params))

	if resp.FrameType != rfFrameTypeResponse {
		return nil, fmt.Errorf("响应帧类型不符合预期：FrameType=%02X（期望响应帧 %02X）", resp.FrameType, rfFrameTypeResponse)
	}

	tlvs := parseRFTLVs(resp.Params)
	fmt.Printf("  ← 解析出 %d 个顶层 TLV：\n", len(tlvs))
	for _, t := range tlvs {
		fmt.Printf("     - Type=%02X Len=%d Value=%s\n", t.Type, len(t.Value), rs485.FormatHex(t.Value))
		if t.Type == rfTLVSingleTag {
			for _, inner := range parseRFTLVs(t.Value) {
				fmt.Printf("        · 内嵌 TLV Type=%02X Len=%d Value=%s\n", inner.Type, len(inner.Value), rs485.FormatHex(inner.Value))
			}
		}
	}

	if len(tlvs) > 0 && tlvs[0].Type == rfTLVStatus && len(tlvs[0].Value) > 0 && tlvs[0].Value[0] != 0x00 {
		return nil, fmt.Errorf("读写器返回状态码 %02X（非成功）", tlvs[0].Value[0])
	}

	cards := extractCardsFromTagTLVs(resp.Params)
	fmt.Printf("  ← 提炼出的卡号（共 %d 张）：%v\n", len(cards), cards)

	return cards, nil
}

// cardModeDuration / cardModePollInterval：card 命令的连续读卡参数——
// 总共读 2 秒，期间每隔 50ms 发一次查询指令，最后统计命中/未命中次数。
const cardModeDuration = 1 * time.Second
const cardModePollInterval = 130 * time.Millisecond

// QueryCardsRepeated 在 duration 时间内，每隔 interval 向读卡器发一次
// “主动盘存标签(0x22)”指令，直到时间用完。期间只连接一次（复用同一个 TCP 连接
// 和同一个 bufio.Reader，避免每次都重新握手，也避免残留的粘包字节被 bufio 缓冲区吞掉）。
// 每次尝试打印一行简要结果（读到/没读到），结束后打印命中/未命中次数汇总，
// 以及本轮一共读到过哪些卡号（去重，并统计每张卡被读到的次数）。
func QueryCardsRepeated(tmpReader ReaderInfo, isDefault bool, readerAddr byte, duration, interval time.Duration) ([]string, error) {
	addr := ""
	if isDefault {
		addr = net.JoinHostPort(defaultHost, defaultCardReaderPort)
	} else {
		addr = net.JoinHostPort(tmpReader.IP, tmpReader.Port)
	}
	fmt.Printf("  → 连接读卡器 %s ...\n", addr)

	conn, err := net.DialTimeout("tcp", addr, connectTimeout)
	if err != nil {
		return nil, fmt.Errorf("连接失败: %w", err)
	}
	defer conn.Close()

	fmt.Printf("  → 开始连续读卡：共 %v，每 %v 查询一次\n", duration, interval)

	bufReader := bufio.NewReader(conn)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	deadline := time.Now().Add(duration)
	attempt := 0
	hitCount := 0
	missCount := 0
	cardHitCounts := make(map[string]int)

	for time.Now().Before(deadline) {
		attempt++

		if err := conn.SetDeadline(time.Now().Add(interval)); err != nil {
			fmt.Printf("  [%3d] ✗ 设置超时失败：%v\n", attempt, err)
			missCount++
			<-ticker.C
			continue
		}

		cmd := buildRFFrame(rfFrameTypeCommand, uint16(readerAddr), rfCmdSingleInventory, nil)
		if _, err := conn.Write(cmd); err != nil {
			fmt.Printf("  [%3d] ✗ 发送失败：%v\n", attempt, err)
			missCount++
			<-ticker.C
			continue
		}

		resp, raw, err := readRFFrame(bufReader)
		if err != nil {
			fmt.Printf("  [%3d] ✗ 未读到有效响应：%v（原始字节：%s）\n", attempt, err, rs485.FormatHex(raw))
			missCount++
			<-ticker.C
			continue
		}

		tlvs := parseRFTLVs(resp.Params)
		if len(tlvs) > 0 && tlvs[0].Type == rfTLVStatus && len(tlvs[0].Value) > 0 && tlvs[0].Value[0] != 0x00 {
			fmt.Printf("  [%3d] ✗ 未读到卡（状态码 %02X）\n", attempt, tlvs[0].Value[0])
			missCount++
			<-ticker.C
			continue
		}

		cards := extractCardsFromTagTLVs(resp.Params)
		if len(cards) == 0 {
			fmt.Printf("  [%3d] ✗ 未读到卡\n", attempt)
			missCount++
		} else {
			hitCount++
			fmt.Printf("  [%3d] ✓ 读到 %d 张卡：%v\n", attempt, len(cards), cards)
			for _, c := range cards {
				cardHitCounts[c]++
			}
		}

		<-ticker.C
	}

	fmt.Println("  ────────── 汇总 ──────────")
	fmt.Printf("  共查询 %d 次，读到卡 %d 次，未读到卡 %d 次\n", attempt, hitCount, missCount)

	uniqueCards := make([]string, 0, len(cardHitCounts))
	for c := range cardHitCounts {
		uniqueCards = append(uniqueCards, c)
	}
	sort.Strings(uniqueCards)

	if len(uniqueCards) == 0 {
		fmt.Println("  本轮未读到任何卡号")
	} else {
		fmt.Printf("  本轮共读到 %d 张不同的卡：\n", len(uniqueCards))
		for _, c := range uniqueCards {
			fmt.Printf("     %s  命中 %d 次\n", c, cardHitCounts[c])
		}
	}

	return uniqueCards, nil
}

// QueryCardsOnce 只读一次：连接读卡器，发送一次"主动盘存标签(0x22)"指令，
// 读取一次响应并解析出卡号列表，不重试、不轮询，读完（无论命中与否）立即返回。
// 与 QueryCardsRepeated 的区别：后者在 duration 时间内每隔 interval 反复查询，
// 这里只发一次、只读一次。
func QueryCardsOnce(tmpReader ReaderInfo, isDefault bool, readerAddr uint16) ([]string, error) {
	addr := ""
	if isDefault {
		addr = net.JoinHostPort(defaultHost, defaultCardReaderPort)
	} else {
		addr = net.JoinHostPort(tmpReader.IP, tmpReader.Port)
	}
	fmt.Printf("  → 连接读卡器 %s ...\n", addr)

	conn, err := net.DialTimeout("tcp", addr, connectTimeout)
	if err != nil {
		return nil, fmt.Errorf("连接失败: %w", err)
	}
	defer conn.Close()

	fmt.Println("  → 只读一次（发送一次查询指令，读取一次响应）")

	if err := conn.SetDeadline(time.Now().Add(readTimeout)); err != nil {
		return nil, fmt.Errorf("设置超时失败: %w", err)
	}

	bufReader := bufio.NewReader(conn)

	cmd := buildRFFrame(rfFrameTypeCommand, readerAddr, rfCmdSingleInventory, nil)
	if _, err := conn.Write(cmd); err != nil {
		return nil, fmt.Errorf("发送失败: %w", err)
	}

	resp, raw, err := readRFFrame(bufReader)
	if err != nil {
		fmt.Printf("  ✗ 未读到有效响应：%v（原始字节：%s）\n", err, rs485.FormatHex(raw))
		printSingleReadSummary(nil)
		return nil, nil
	}

	tlvs := parseRFTLVs(resp.Params)
	if len(tlvs) > 0 && tlvs[0].Type == rfTLVStatus && len(tlvs[0].Value) > 0 && tlvs[0].Value[0] != 0x00 {
		fmt.Printf("  ✗ 未读到卡（状态码 %02X）\n", tlvs[0].Value[0])
		printSingleReadSummary(nil)
		return nil, nil
	}

	cards := extractCardsFromTagTLVs(resp.Params)
	if len(cards) == 0 {
		fmt.Println("  ✗ 未读到卡")
	} else {
		fmt.Printf("  ✓ 读到 %d 张卡：%v\n", len(cards), cards)
	}

	sort.Strings(cards)
	printSingleReadSummary(cards)
	return cards, nil
}

// printSingleReadSummary 打印"只读一次"模式的汇总信息，cards 为空时表示未读到任何卡。
func printSingleReadSummary(cards []string) {
	fmt.Println("  ────────── 汇总 ──────────")
	if len(cards) == 0 {
		fmt.Println("  共查询 1 次，读到卡 0 次，未读到卡 1 次")
		fmt.Println("  本轮未读到任何卡号")
		return
	}
	fmt.Println("  共查询 1 次，读到卡 1 次，未读到卡 0 次")
	fmt.Printf("  本轮共读到 %d 张不同的卡：\n", len(cards))
	for _, c := range cards {
		fmt.Printf("     %s\n", c)
	}
}
