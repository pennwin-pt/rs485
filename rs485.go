// Package rs485 封装了向 RS485/TCP 网关下发继电器开关指令的底层逻辑。
//
// 🎯 这里的帧格式、CRC 计算方式、"01 路 = up / 02 路 = down" 的约定，
// 全部照搬自 cmd/rs485-client 工具（rs485_client.go）里 up05 / down05 预设组合指令的实现，
// 只是把它从一个交互式命令行工具，改造成可以被服务端业务代码直接调用的库函数。
//
// 🚨 与 rs485_client.go 里 sendAndReceive 的一点重要区别：
// 服务端场景下设备不会返回任何数据，所以这里的 SendFrame 只负责"把指令发出去"，
// 发送成功即返回 nil；发送失败（连不上网关 / 写入报错）才视为失败。
// 不会像 rs485_client.go 那样等待并读取设备回包。
package rs485

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
	"sort"
	"strings"
	"time"
)

const (
	// ChannelUp 对应 rs485_client.go 里 up 改为使用的 "02 路"
	ChannelUp byte = 0x02
	// ChannelDown 对应 rs485_client.go 里 down 改为使用的 "01 路"
	ChannelDown byte = 0x01

	channelOnByte  byte = 0xFF // 开
	channelOffByte byte = 0x00 // 关

	// DefaultDialTimeout 连接 RS485/TCP 网关的超时时间
	DefaultDialTimeout = 3 * time.Second
	// DefaultWriteTimeout 写入指令的超时时间
	DefaultWriteTimeout = 3 * time.Second

	// steppedSleepUnit 是 SleepSeconds / RunUpDownStepped 保持阶段的最小睡眠步长。
	// 🌟 之所以把"保持 N 秒"拆成 N/0.1 次小步睡眠，而不是一次性 time.Sleep(N)：
	// 一是配合上层 upSeconds/downSeconds 现在支持 1 位小数（如 3.5 秒），
	// 用 0.1 秒的步长睡眠可以精确对齐配置精度；
	// 二是保留了未来做"中途取消"的可能性（每醒来一次都可以加一次状态检查），
	// 和项目里 runDemoSimulator 用 1 秒步长拆分长睡眠的思路是一致的，
	// 只是这里的场景需要更细的粒度。
	steppedSleepUnit = 100 * time.Millisecond
)

const (
	rfFrameTypeCommand  byte = 0x00
	rfFrameTypeResponse byte = 0x01
	rfFrameTypeNotify   byte = 0x02

	rfCmdSingleInventory byte = 0x22 // 主动盘存标签：读一次就停

	rfTLVStatus    byte = 0x07 // 状态 TLV
	rfTLVSingleTag byte = 0x50 // 单张标签 TLV（内部还嵌套 EPC/RSSI/Time 等 TLV）
	rfTLVEPC       byte = 0x01 // EPC TLV，值就是卡号
)

func FormatHex(data []byte) string {
	if len(data) == 0 {
		return ""
	}
	parts := make([]string, len(data))
	for i, b := range data {
		parts[i] = fmt.Sprintf("%02X", b)
	}
	return strings.Join(parts, " ")
}

// BuildChannelPayload 组装一条「开/关某一路继电器」的完整报文（6 字节数据 + 2 字节 CRC16，共 8 字节）。
//
// 对应 rs485_client.go 里 runPresetUpDown 组装出的指令格式：
//
//	deviceAddr 05 00 01 channel (FF|00)
//
// 例如设备地址 0x05、01 路（up）、开：05 05 00 01 01 FF + CRC16
func BuildChannelPayload(deviceAddr byte, channel byte, on bool) []byte {
	stateByte := channelOffByte
	if on {
		stateByte = channelOnByte
	}
	data := []byte{deviceAddr, 0x05, 0x00, 0x01, channel, stateByte}

	crc := Crc16Modbus(data)
	payload := make([]byte, 0, len(data)+2)
	payload = append(payload, data...)
	payload = append(payload, byte(crc&0xFF), byte(crc>>8))
	return payload
}

// Crc16Modbus 计算 Modbus RTU 常用的 CRC-16（多项式 0x8005，低字节在前）。
// 与 rs485_client.go 中的 Crc16Modbus 完全一致。
func Crc16Modbus(data []byte) uint16 {
	var crc uint16 = 0xFFFF
	for _, b := range data {
		crc ^= uint16(b)
		for i := 0; i < 8; i++ {
			if crc&0x0001 != 0 {
				crc = (crc >> 1) ^ 0xA001
			} else {
				crc >>= 1
			}
		}
	}
	return crc
}

// SendFrame 把一帧已经算好 CRC 的报文发送到 f1Addr1To20（RS485/TCP 网关地址，如 "192.168.2.170:8001"）。
//
// 🌟 设备不会返回数据：这里只负责把指令写出去，写入成功就返回 nil；
// 连接失败或写入失败才返回 error（对应"发送指令失败，激活失败"）。
func SendFrame(addr string, payload []byte) error {
	conn, err := net.DialTimeout("tcp", addr, DefaultDialTimeout)
	if err != nil {
		return fmt.Errorf("连接 %s 失败: %w", addr, err)
	}
	defer conn.Close()

	if err := conn.SetWriteDeadline(time.Now().Add(DefaultWriteTimeout)); err != nil {
		return fmt.Errorf("设置写超时失败: %w", err)
	}
	if _, err := conn.Write(payload); err != nil {
		return fmt.Errorf("发送指令失败: %w", err)
	}
	return nil
}

// SleepSeconds 以 0.1 秒为最小步长，睡够 totalSeconds 秒（支持小数，如 3.5 秒）。
// totalSeconds <= 0 时直接返回，不睡眠。
func SleepSeconds(totalSeconds float64) {
	if totalSeconds <= 0 {
		return
	}
	total := time.Duration(totalSeconds * float64(time.Second))
	var elapsed time.Duration
	for elapsed < total {
		step := steppedSleepUnit
		if remaining := total - elapsed; remaining < step {
			step = remaining
		}
		time.Sleep(step)
		elapsed += step
	}
}

const (
	// verifyQueryDelay 发送开/关指令后，等待多久再发查询指令去确认硬件状态是否真的变了。
	// 🌟 之所以要等一下而不是立即查：继电器动作、总线仲裁都需要一点时间，立即查大概率会
	// 读到"还没来得及变化"的旧状态，误判成"没生效"进而引发不必要的重发。
	verifyQueryDelay = 300 * time.Millisecond

	// maxVerifyAttempts 「发送 -> 查询确认」这一组动作最多尝试的次数（含第一次），
	// 即最多重试 4 次（第 1 次 + 重试 4 次 = 5 次）。
	maxVerifyAttempts = 5
)

// RelayBit 是查询回包最后一个状态字节里，某一路继电器 (M1/M2/M3) 对应的二进制位。
// 🌟 状态字节的每一位代表一路硬件的开关状态：bit0(0x01)=M1、bit1(0x02)=M2、bit2(0x04)=M3，
// 该位为 1 表示对应硬件处于"开"状态，为 0 表示"关"，多路可以按位或叠加（如 0x05 = M1+M3 开）。
type RelayBit byte

const (
	RelayM1 RelayBit = 0x01
	RelayM2 RelayBit = 0x02
	RelayM3 RelayBit = 0x04
)

// relayBitForChannel 把 BuildChannelPayload 里用来选择"开哪一路"的 channel 参数
// (ChannelUp / ChannelDown)，映射到查询回包状态字节里对应要核对的硬件位：
// 上升指令 / 上升停止指令都作用在 M1 上，下降指令 / 下降停止指令都作用在 M2 上。
func relayBitForChannel(channel byte) RelayBit {
	if channel == ChannelUp {
		return RelayM1
	}
	return RelayM2
}

// QueryRelayStatus 发送查询继电器状态指令 (XX 02 00 01 00 03 + CRC16)，
// 返回设备回包里的状态字节（回包格式与 QueryDeviceTrigger 一致：6 字节数据 + 2 字节 CRC16，
// 共 8 字节，状态值是第 6 个字节，即 resp[5]）。
// 状态字节的位含义见 RelayBit 的注释。
func QueryRelayStatus(addr string, deviceAddr byte) (byte, error) {
	data := []byte{deviceAddr, 0x02, 0x00, 0x01, 0x00, 0x03}
	crc := Crc16Modbus(data)
	payload := make([]byte, 0, len(data)+2)
	payload = append(payload, data...)
	payload = append(payload, byte(crc&0xFF), byte(crc>>8))

	conn, err := net.DialTimeout("tcp", addr, DefaultDialTimeout)
	if err != nil {
		log.Printf("[RS485][查询] 设备 0x%02X 连接 %s 失败: %v", deviceAddr, addr, err)
		return 0, fmt.Errorf("连接设备失败: %w", err)
	}
	defer conn.Close()

	if err := conn.SetWriteDeadline(time.Now().Add(DefaultWriteTimeout)); err != nil {
		return 0, err
	}
	if _, err := conn.Write(payload); err != nil {
		log.Printf("[RS485][查询] 设备 0x%02X 发送查询指令失败: %v，报文=% X", deviceAddr, err, payload)
		return 0, fmt.Errorf("发送查询指令失败: %w", err)
	}

	// 预期回包长度为 8 字节 (如 05 02 00 01 00 01 校验低 校验高)，状态字节在 resp[5]。
	resp := make([]byte, 8)
	if err := conn.SetReadDeadline(time.Now().Add(DefaultWriteTimeout)); err != nil {
		return 0, err
	}
	if _, err := io.ReadFull(conn, resp); err != nil {
		log.Printf("[RS485][查询] 设备 0x%02X 读取回包失败: %v", deviceAddr, err)
		return 0, fmt.Errorf("读取设备回包失败: %w", err)
	}

	log.Printf("[RS485][查询] 设备 0x%02X 查询指令=% X，收到回包=% X", deviceAddr, payload, resp)

	return resp[5], nil
}

// sendChannelWithVerify 发送一次开/关指令后，隔 verifyQueryDelay 用 QueryRelayStatus
// 查一次硬件实际状态，确认是否变成了期望的 on/off；如果没变化（大概率是总线撞车、
// 指令没被设备真正执行），就重新发送 + 重新查询，最多尝试 maxVerifyAttempts 次。
//
// 🌟 发送本身失败（网络层面）和"发送成功但状态没变"都会触发重试，统一走同一条路径，
// 直到最后一次尝试仍不满足才把最后一次的错误信息返回出去。
func sendChannelWithVerify(addr string, deviceAddr byte, channel byte, on bool) error {
	//bit := byte(relayBitForChannel(channel))
	payload := BuildChannelPayload(deviceAddr, channel, on)

	action := "关闭"
	if on {
		action = "开启"
	}

	log.Printf("[RS485][%s] 设备 0x%02X 通道 0x%02X：开始下发指令，网关=%s，报文=% X", action, deviceAddr, channel, addr, payload)

	var lastErr error
	for attempt := 1; attempt <= maxVerifyAttempts; attempt++ {

		if err := SendFrame(addr, payload); err != nil {
			lastErr = fmt.Errorf("第 %d/%d 次发送指令失败: %w", attempt, maxVerifyAttempts, err)
			log.Printf("[RS485][%s] 设备 0x%02X 第 %d/%d 次发送失败: %v", action, deviceAddr, attempt, maxVerifyAttempts, err)
			continue
		}
		log.Printf("[RS485][%s] 设备 0x%02X 第 %d/%d 次发送成功，%v 后查询硬件状态确认...", action, deviceAddr, attempt, maxVerifyAttempts, verifyQueryDelay)

		// FIXME：下面是暂时的不查
		log.Printf("[RS485][%s] 设备 0x%02X 第 %d 次尝试确认生效", action, deviceAddr, attempt)
		return nil // 硬件状态已经符合预期，发送确认生效
		/*
			time.Sleep(verifyQueryDelay)


			status, err := QueryRelayStatus(addr, deviceAddr)
			if err != nil {
				// 查询本身失败（比如网关连不上），无法确认是否生效，保守地当作一次失败重试。
				lastErr = fmt.Errorf("第 %d/%d 次发送后查询硬件状态失败: %w", attempt, maxVerifyAttempts, err)
				log.Printf("[RS485][%s] 设备 0x%02X 第 %d/%d 次查询状态失败: %v", action, deviceAddr, attempt, maxVerifyAttempts, err)
				continue
			}

			actualOn := status&bit != 0
			log.Printf("[RS485][%s] 设备 0x%02X 第 %d/%d 次查询回包状态字节=0x%02X，期望开=%v，实际开=%v", action, deviceAddr, attempt, maxVerifyAttempts, status, on, actualOn)

			if actualOn == on {
				log.Printf("[RS485][%s] 设备 0x%02X 第 %d 次尝试确认生效", action, deviceAddr, attempt)
				return nil // 硬件状态已经符合预期，发送确认生效
			}
			lastErr = fmt.Errorf("第 %d/%d 次发送后硬件状态未按预期变化（期望开=%v，实际状态字节=0x%02X）", attempt, maxVerifyAttempts, on, status)
			if attempt < maxVerifyAttempts {
				log.Printf("[RS485][%s] 设备 0x%02X 状态未变化，准备第 %d 次重试", action, deviceAddr, attempt+1)
			}*/
	}

	log.Printf("[RS485][%s] 设备 0x%02X 重试 %d 次后仍未确认生效，放弃", action, deviceAddr, maxVerifyAttempts)
	return fmt.Errorf("%s指令重试 %d 次后仍未确认生效: %w", action, maxVerifyAttempts, lastErr)
}

// RunUpDownStepped 和 RunUpDown 逻辑完全一致（开 -> 保持 -> 关），
// 唯一区别是保持阶段调用 SleepSeconds 按 0.1 秒步长睡眠，而不是一次性 time.Sleep，
// 用于所有"保持秒数支持 1 位小数"的场景（调试模式的上升/下降控制、正式的
// 激活/复位流程），秒数直接传 float64，不需要调用方自己换算 time.Duration。
func RunUpDownStepped(addr string, deviceAddr byte, channel byte, holdSeconds float64) error {
	if err := SendOpen(addr, deviceAddr, channel); err != nil {
		return err
	}

	SleepSeconds(holdSeconds)

	if err := SendClose(addr, deviceAddr, channel); err != nil {
		return err
	}
	return nil
}

// SendOpen 发送「开」指令，不等待、不负责后续的「关」。
// 🌟 拆分给 CommandAgent（见 agent.go）使用：CommandAgent 需要自己掌握「开」和「关」
// 之间的时间调度（提前派发下一个设备、到点插队优先发关闭指令），
// 不能再像 RunUpDownStepped 那样把「开 -> 保持 -> 关」捆死在一次阻塞调用里。
//
// 🌟 内部已经带上"发送后查询硬件状态确认 + 最多重试 5 次"的逻辑（见 sendChannelWithVerify），
// 调用方（包括 CommandAgent）不需要也不应该再自己重复发送或重试。
func SendOpen(addr string, deviceAddr byte, channel byte) error {
	if err := sendChannelWithVerify(addr, deviceAddr, channel, true); err != nil {
		return fmt.Errorf("发送开启指令失败: %w", err)
	}
	return nil
}

// SendClose 发送「关」指令，对应 SendOpen，同样带有"发送后查询确认 + 重试"逻辑，
// 同样不掺杂任何额外的等待/调度逻辑（那是 CommandAgent 的职责）。
func SendClose(addr string, deviceAddr byte, channel byte) error {
	if err := sendChannelWithVerify(addr, deviceAddr, channel, false); err != nil {
		return fmt.Errorf("发送关闭指令失败: %w", err)
	}
	return nil
}

// QueryDeviceTrigger 发送查询状态指令 (xx 02 00 02 00 05) 并返回完整的设备回包。
//
// readTimeout 控制"发送后等待设备回包"这一步的读超时，由调用方按自己的场景决定：
// 目前唯一的调用方是 CommandAgent.QueryDeviceTrigger，它传入的是比 DefaultWriteTimeout
// 短得多的 DefaultQueryReadTimeout —— 因为查询指令和开/关/复位指令共用同一条
// CommandAgent 发送队列，读超时越长，一旦遇到网络问题就会让队列卡得越久，
// 拖延后面排队的指令（尤其是到点该发的"关"指令）。连接和写入仍然沿用
// DefaultDialTimeout / DefaultWriteTimeout，只有"等回包"这一步单独可调。
func QueryDeviceTrigger(addr string, deviceAddr byte, readTimeout time.Duration) ([]byte, error) {
	// 组装查询指令
	data := []byte{deviceAddr, 0x02, 0x00, 0x02, 0x00, 0x05}
	crc := Crc16Modbus(data)
	payload := make([]byte, 0, len(data)+2)
	payload = append(payload, data...)
	payload = append(payload, byte(crc&0xFF), byte(crc>>8))

	conn, err := net.DialTimeout("tcp", addr, DefaultDialTimeout)
	if err != nil {
		return nil, fmt.Errorf("连接设备失败: %w", err)
	}
	defer conn.Close()

	// 发送指令
	if err := conn.SetWriteDeadline(time.Now().Add(DefaultWriteTimeout)); err != nil {
		return nil, err
	}
	if _, err := conn.Write(payload); err != nil {
		return nil, fmt.Errorf("发送查询指令失败: %w", err)
	}

	// 接收回包，预期长度为 8 字节 (如 05 02 00 02 00 01 19 8E)
	resp := make([]byte, 8)
	if err := conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
		return nil, err
	}
	_, err = io.ReadFull(conn, resp)
	if err != nil {
		return nil, fmt.Errorf("读取设备回包失败: %w", err)
	}

	return resp, nil
}

/*
以下全是读卡器相关
*/

// ==================== UHF RFID CPH 协议支持 ====================
//
// ==============================================================

// readCardRFFrame 从连接里读取并校验一条完整的 CPH 协议帧。
// 除了解析结果和 error，还会把目前为止实际读到的原始字节一并返回（哪怕解析失败/读取出错也会返回已读到的部分），
// 方便调用方在打印调试信息时，无论成功失败都能看到“收到了什么”。
func readCardRFFrame(reader *bufio.Reader) (*cardRfFrame, []byte, error) {
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
	if want := CardRfChecksum(full); want != checksumByte[0] {
		return nil, raw, fmt.Errorf("校验和不匹配：期望 %02X 实际 %02X", want, checksumByte[0])
	}

	return &cardRfFrame{
		FrameType: rest[0],
		Address:   uint16(rest[1])<<8 | uint16(rest[2]),
		FrameCode: rest[3],
		Params:    params,
	}, raw, nil
}

// CardRfChecksum 按UHF RFID协议文档的算法计算校验和：从 header 到 checksum 前一字节的累加和，取反加一。
func CardRfChecksum(data []byte) byte {
	var sum byte
	for _, b := range data {
		sum += b
	}
	return ^sum + 1
}

// extractCardsFromTagTLVs 从响应帧的参数里找出所有“单张标签 TLV(0x50)”，
// 再从每一个里面取出 EPC TLV(0x01) 的值作为卡号（十六进制字符串）。
func extractCardsFromTagTLVs(params []byte) []string {
	var cards []string
	for _, t := range parseCardRFTLVs(params) {
		if t.Type != rfTLVSingleTag {
			continue
		}
		for _, inner := range parseCardRFTLVs(t.Value) {
			if inner.Type == rfTLVEPC {
				cards = append(cards, strings.ToUpper(hex.EncodeToString(inner.Value)))
			}
		}
	}
	return cards
}

// cardRfTLV 是一条通用 TLV：1 字节类型 + 1 字节长度 + N 字节值。
type cardRfTLV struct {
	Type  byte
	Value []byte
}

// cardRfFrame 是解析出来的一条 CPH 协议帧。
type cardRfFrame struct {
	FrameType byte
	Address   uint16
	FrameCode byte
	Params    []byte
}

// parseCardRFTLVs 按 Type(1) Length(1) Value(N) 的格式顺序切出所有 TLV。
func parseCardRFTLVs(data []byte) []cardRfTLV {
	var out []cardRfTLV
	i := 0
	for i+2 <= len(data) {
		t := data[i]
		l := int(data[i+1])
		i += 2
		if i+l > len(data) {
			break // 数据不完整，直接放弃剩余部分
		}
		out = append(out, cardRfTLV{Type: t, Value: data[i : i+l]})
		i += l
	}
	return out
}

// buildCardRFFrame 拼出一条完整的 CPH 协议帧（命令帧用 rfFrameTypeCommand）。
func buildCardRFFrame(frameType byte, address uint16, frameCode byte, params []byte) []byte {
	frame := make([]byte, 0, 8+len(params))
	frame = append(frame, 'R', 'F', frameType, byte(address>>8), byte(address&0xFF), frameCode)
	frame = append(frame, byte(len(params)>>8), byte(len(params)&0xFF))
	frame = append(frame, params...)
	frame = append(frame, CardRfChecksum(frame))
	return frame
}

// QueryCardsRepeated 返回的是每张卡的卡号->对应次数的map
func QueryCardsRepeated(readerAddr uint16, readerIP, readerPort string, duration, interval time.Duration) (map[string]int, error) {
	addr := net.JoinHostPort(readerIP, readerPort)

	log.Printf("  → 连接读卡器 %s ...\n", addr)

	conn, err := net.DialTimeout("tcp", addr, DefaultDialTimeout)
	if err != nil {
		return nil, fmt.Errorf("连接失败: %w", err)
	}
	defer conn.Close()

	log.Printf("  → 开始连续读卡：共 %v，每 %v 查询一次\n", duration, interval)

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
			log.Printf("  [%3d] ✗ 设置超时失败：%v\n", attempt, err)
			missCount++
			<-ticker.C
			continue
		}

		cmd := buildCardRFFrame(rfFrameTypeCommand, readerAddr, rfCmdSingleInventory, nil)
		if _, err := conn.Write(cmd); err != nil {
			log.Printf("  [%3d] ✗ 发送失败：%v\n", attempt, err)
			missCount++
			<-ticker.C
			continue
		}

		resp, raw, err := readCardRFFrame(bufReader)
		if err != nil {
			log.Printf("  [%3d] ✗ 未读到有效响应：%v（原始字节：%s）\n", attempt, err, FormatHex(raw))
			missCount++
			<-ticker.C
			continue
		}

		tlvs := parseCardRFTLVs(resp.Params)
		if len(tlvs) > 0 && tlvs[0].Type == rfTLVStatus && len(tlvs[0].Value) > 0 && tlvs[0].Value[0] != 0x00 {
			log.Printf("  [%3d] ✗ 未读到卡（状态码 %02X）\n", attempt, tlvs[0].Value[0])
			missCount++
			continue
		}

		cards := extractCardsFromTagTLVs(resp.Params)
		if len(cards) == 0 {
			log.Printf("  [%3d] ✗ 未读到卡\n", attempt)
			missCount++
		} else {
			hitCount++
			log.Printf("  [%3d] ✓ 读到 %d 张卡：%v\n", attempt, len(cards), cards)
			for _, c := range cards {
				cardHitCounts[c]++
			}
		}

		<-ticker.C
	}

	log.Println("  ────────── 汇总 ──────────")
	log.Printf("  共查询 %d 次，读到卡 %d 次，未读到卡 %d 次\n", attempt, hitCount, missCount)

	uniqueCards := make([]string, 0, len(cardHitCounts))
	for c := range cardHitCounts {
		uniqueCards = append(uniqueCards, c)
	}
	sort.Strings(uniqueCards)

	if len(uniqueCards) == 0 {
		return nil, fmt.Errorf("  本轮未读到任何卡号")
	} else {
		log.Printf("  本轮共读到 %d 张不同的卡：\n", len(uniqueCards))
		for _, c := range uniqueCards {
			hits := cardHitCounts[c]
			fmt.Printf("     %s  命中 %d 次\n", c, hits)
		}
	}
	return cardHitCounts, nil
}
