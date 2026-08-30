// rs485-debugger — 交互式 RS485/TCP 指令发送工具
//
// 用法:
//
//	go build -o rs485-debugger.exe ./cmd
//	rs485-debugger.exe 192.168.1.100
//	rs485-debugger.exe 192.168.1.100:8899 10
//
// 启动后进入循环：每一轮先选择本轮要用的模式：
//
//	· 直接回车（或输入 1）  → 组合指令模式：依次输入开启/关闭两组指令，
//	  自动执行 发送开启 → 倒计时等待 → 发送关闭 的完整流程。
//	· 输入 2 或 s           → 单个指令模式：输入一条指令并立即发送一次。
//	· 输入 up                → 01 路 开→关：输入后再输入十进制设备地址（如 0、1、5 均可），
//	  对该地址执行 开(XX 05 00 01 01 FF) → 关(XX 05 00 01 01 00)。
//	· 输入 down              → 02 路 开→关：输入后再输入十进制设备地址（如 0、1、5 均可），
//	  对该地址执行 开(XX 05 00 01 02 FF) → 关(XX 05 00 01 02 00)。
//	· 输入 check             → 查询状态：输入后再输入十进制设备地址（如 0、1、5 均可），
//	  对该地址发送查询状态指令 XX 02 00 02 00 05，立即发送一次。
//	· 输入 card              → 读卡模式：和 mode 用法一致，先输入 card 进入该模式，
//	  再输入十进制设备地址（如 0、1、5），随后连续读卡 2 秒（每 50ms 查询一次），
//	  结束后打印命中/未命中次数汇总和读到的卡号列表。
//	· 输入 card1             → 只读一次模式：用法与 card 一致，输入十进制设备地址后
//	  只发送一次查询指令、只读一次响应，立即打印结果，不做 2 秒轮询。
//	· 输入 mode / workmode  → 【新功能】设置 RFID 读写器工作模式：支持自定义设备地址，选择主动/被动/触发模式。
//	· 输入 verify / vf      → 【新功能】带确认重试的开→保持→关：输入十进制设备地址后，
//	  每一步（开/关）发送指令后都会查询继电器实际状态确认是否生效，不符合预期会自动重发，
//	  最多重试 5 次，直接调用 rs485 库里导出的 rs485.SendChannelWithVerify。
//	· 输入 q / quit / exit  → 退出程序。
//
// 每一轮流程结束后会自动回到模式选择，可重复执行任意次。
package main

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"github.com/pennwin-pt/rs485"
	"io"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	defaultPort           = "8001"
	defaultCardReaderPort = "8003"
	defaultHost           = "192.168.2.170"
	defaultTarget         = defaultHost + ":" + defaultPort
	defaultDelaySeconds   = 12 // 未指定关闭延时时使用的默认秒数
	warnAheadSeconds      = 1  // 发送关闭指令前多少秒开始提示“即将发送”
	readTimeout           = 1 * time.Second
	connectTimeout        = 5 * time.Second
	payloadLen            = 6 // 用户输入固定 6 字节，CRC-16 占 2 字节，共发送 8 字节
)

type runMode int

const (
	modeCombo       runMode = iota // 组合指令模式：开启 → 等待 → 关闭
	modeSingle                     // 单个指令模式：发送一条指令
	modeQuit                       // 退出程序
	modeUp                         // 01 路开→关：交互式输入十进制设备地址，可对任意地址下发
	modeDown                       // 02 路开→关：交互式输入十进制设备地址，可对任意地址下发
	modeCheck                      // 查询状态：交互式输入十进制设备地址，查询任意地址
	modeCard                       // 读卡模式：交互式输入十进制设备地址，连续读卡 2 秒并汇总命中次数
	modeCard1                      // 只读一次模式：交互式输入十进制设备地址，只发送并读取一次
	modeSetWorkMode                // 设置RFID读写器工作模式（主动/被动/触发）
	modeVerify                     // 带确认重试的开→保持→关：交互式输入十进制设备地址
)

var hexCleanRE = regexp.MustCompile(`[\s,:-]+`)

// cardModeDuration / cardModePollInterval：card 命令的连续读卡参数——
// 总共读 cardModeDuration，期间每隔 cardModePollInterval 发一次查询指令，最后统计命中/未命中次数。
const cardModeDuration = 1 * time.Second
const cardModePollInterval = 130 * time.Millisecond

func main() {
	var target string
	if len(os.Args) < 2 {
		target = defaultTarget
		fmt.Printf("未指定目标地址，使用默认值：%s\n", target)
	} else {
		target = os.Args[1]
	}

	delaySeconds := defaultDelaySeconds
	if len(os.Args) >= 3 {
		if v, err := strconv.Atoi(strings.TrimSpace(os.Args[2])); err == nil && v > 0 {
			delaySeconds = v
		} else {
			fmt.Printf("关闭延时参数无效，使用默认值：%d 秒\n", defaultDelaySeconds)
		}
	} else {
		fmt.Printf("未指定关闭延时，使用默认值：%d 秒\n", defaultDelaySeconds)
	}

	host, port := parseTarget(target)
	addr := net.JoinHostPort(host, port)

	printBanner(addr, delaySeconds)

	scanner := bufio.NewScanner(os.Stdin)

	for {
		mode, presetAddr := promptForMode(scanner)

		switch mode {
		case modeQuit:
			fmt.Println("已退出。")
			return

		case modeCombo:
			fmt.Println("\n【组合指令模式】请依次输入两组指令：")
			openPayload := promptForCommand(scanner, "第 1 组【开启】")
			closePayload := promptForCommand(scanner, "第 2 组【关闭】")
			runOpenCloseSequence(addr, openPayload, closePayload, delaySeconds)

		case modeSingle:
			fmt.Println("\n【单个指令模式】请输入要发送的一条指令：")
			payload := promptForCommand(scanner, "单条")
			sendAndPrint(addr, "指令", payload)

		case modeUp:
			runUpMode(addr, scanner, delaySeconds, presetAddr)

		case modeDown:
			runDownMode(addr, scanner, delaySeconds, presetAddr)

		case modeCheck:
			runCheckMode(addr, scanner, presetAddr)

		case modeCard:
			runCardMode(scanner, presetAddr)

		case modeCard1:
			runCardModeOnce(scanner, presetAddr)

		case modeSetWorkMode:
			runSetWorkMode(addr, scanner, presetAddr)

		case modeVerify:
			runVerifiedChannel(addr, scanner, delaySeconds, presetAddr)
		}
	}
}

// runPresetUpDown 根据预设名（"up05"/"down01" 等）和设备地址码执行对应的开→关组合指令。
// 预设名以 "up" 开头时用 01 路（05 00 01 01），以 "down" 开头时用 02 路（05 00 01 02）。
//
// 🚨 这里的 channel 写死用的是 0x01（up）/0x02（down），跟 rs485 库里 ChannelUp=0x02、
// ChannelDown=0x01 的约定正好相反——这是历史遗留下来的不一致，为了不改变现在发给硬件的
// 实际字节，这里暂时保留原始数值，没有直接换成 rs485.ChannelUp/rs485.ChannelDown。
// 如果后续要统一，需要先确认现场硬件到底哪个字节对应哪一路。
func runPresetUpDown(addr, presetName string, deviceAddr byte, delaySeconds int) {
	var channel byte
	if strings.HasPrefix(presetName, "up") {
		channel = 0x01
	} else {
		channel = 0x02
	}

	fmt.Printf("\n【预设组合：%s】设备地址 0x%02X，%02X 路 开→关：\n", presetName, deviceAddr, channel)
	openPayload := rs485.BuildChannelPayload(deviceAddr, channel, true)
	closePayload := rs485.BuildChannelPayload(deviceAddr, channel, false)
	runOpenCloseSequence(addr, openPayload, closePayload, delaySeconds)
}

// runUpMode 是 up05/up01 合并后的通用"01 路 开→关"模式：
// 用法与 down/check/card 一致——先输入 up 进入本模式，再输入十进制设备地址，
// 回车后对该地址执行 01 路 开(XX 05 00 01 01 FF) → 等待 → 关(XX 05 00 01 01 00)。
func runUpMode(addr string, scanner *bufio.Scanner, delaySeconds int, presetAddr int) {
	if presetAddr < 0 {
		fmt.Println("\n【up 模式】请输入十进制设备地址（如 0、1、5），随后将对该地址执行 01 路 开→保持→关。")
	}
	deviceAddr := resolveAddress(scanner, presetAddr, promptForCardAddress)
	if deviceAddr < 0 {
		fmt.Println("✗ 已取消操作。")
		return
	}
	runPresetUpDown(addr, fmt.Sprintf("up%d", deviceAddr), byte(deviceAddr), delaySeconds)
}

// runDownMode 是 down05/down01 合并后的通用"02 路 开→关"模式：
// 用法与 mode/card/check 一致——先输入 down 进入本模式，再输入十进制设备地址，
// 回车后对该地址执行 02 路 开(XX 05 00 01 02 FF) → 等待 → 关(XX 05 00 01 02 00)。
func runDownMode(addr string, scanner *bufio.Scanner, delaySeconds int, presetAddr int) {
	if presetAddr < 0 {
		fmt.Println("\n【down 模式】请输入十进制设备地址（如 0、1、5），随后将对该地址执行 02 路 开→保持→关。")
	}
	deviceAddr := resolveAddress(scanner, presetAddr, promptForCardAddress)
	if deviceAddr < 0 {
		fmt.Println("✗ 已取消操作。")
		return
	}
	runPresetUpDown(addr, fmt.Sprintf("down%d", deviceAddr), byte(deviceAddr), delaySeconds)
}

// runPresetCheck 根据设备地址码执行查询状态指令 (XX 02 00 02 00 05 + CRC16)。
func runPresetCheck(addr string, deviceAddr byte) {
	fmt.Printf("\n【查询状态】设备地址 0x%02X：\n", deviceAddr)
	data := []byte{deviceAddr, 0x02, 0x00, 0x02, 0x00, 0x05}
	crc := rs485.Crc16Modbus(data)
	payload := append(append([]byte(nil), data...), byte(crc&0xFF), byte(crc>>8))
	sendAndPrint(addr, "指令", payload)
}

// runCheckMode 是 check05/check01 合并后的通用查询模式：
// 用法与 mode/card 一致——先输入 check 进入本模式，再输入十进制设备地址，
// 回车后对该地址发送一次查询状态指令（02 00 02 00 05）并打印回包。
func runCheckMode(addr string, scanner *bufio.Scanner, presetAddr int) {
	if presetAddr < 0 {
		fmt.Println("\n【查询状态模式】请输入十进制设备地址（如 0、1、5），随后将对该地址发送一次查询状态指令。")
	}
	deviceAddr := resolveAddress(scanner, presetAddr, promptForCardAddress)
	if deviceAddr < 0 {
		fmt.Println("✗ 已取消操作。")
		return
	}
	runPresetCheck(addr, byte(deviceAddr))
}

// ==================== 带确认重试的开/关 ====================
//
// 🎯 之前 up05/down05 这类预设指令都是"发了就算"，不管继电器有没有真的动作。
// "verify" 模式需要"发送 → 隔一小段时间 → 查询继电器实际状态确认 → 不符合预期就重发"，
// 这套逻辑现在统一由 rs485 库导出的 rs485.SendChannelWithVerify 提供，
// 本文件不再维护自己的一份 sendChannelWithVerify/relayBitForChannel。

// runVerifiedChannel 是"verify"菜单：只需要输入设备地址，
// 随后自动执行"开→保持 delaySeconds 秒→关"整个流程，
// 开、关两步都调用 rs485.SendChannelWithVerify（发送后查询确认+失败重试，
// 次数/间隔用库里的 DefaultVerifyMaxAttempts / DefaultVerifyQueryDelay），
// 通道固定用 0x02（对应本文件 up05/down05 里"down"的 02 路）。
func runVerifiedChannel(addr string, scanner *bufio.Scanner, delaySeconds int, presetAddr int) {
	if presetAddr < 0 {
		fmt.Println("\n【带确认重试模式】请输入十进制设备地址（如 0、1、5），随后将执行 开→保持→关，每步都会查询继电器状态确认生效。")
	}
	deviceAddr := resolveAddress(scanner, presetAddr, promptForCardAddress)
	if deviceAddr < 0 {
		fmt.Println("✗ 已取消操作。")
		return
	}

	const channel = 0x02

	fmt.Printf("\n【verify】设备地址 0x%02X，通道 0x%02X：开始执行 开→保持%d秒→关（每步最多重试 %d 次确认）\n", deviceAddr, channel, delaySeconds, rs485.DefaultVerifyMaxAttempts)

	if err := rs485.SendChannelWithVerify(addr, byte(deviceAddr), channel, true, 0, 0); err != nil {
		fmt.Printf("✗ 开启失败：%v\n", err)
		return
	}
	fmt.Println("✓ 开启已确认生效。")

	for elapsed := 1; elapsed <= delaySeconds; elapsed++ {
		time.Sleep(1 * time.Second)
		fmt.Printf("  ⏱ 倒计时：还剩 %d 秒\n", delaySeconds-elapsed)
	}

	if err := rs485.SendChannelWithVerify(addr, byte(deviceAddr), channel, false, 0, 0); err != nil {
		fmt.Printf("✗ 关闭失败：%v\n", err)
		return
	}
	fmt.Println("✓ 关闭已确认生效。流程执行完毕。")
}

// runPresetCard 根据设备地址码从 motorReaderMap 找到对应读卡器，
// 连续读卡 cardModeDuration，期间每隔 cardModePollInterval 查询一次，
// 结束后打印命中/未命中次数汇总和读到的卡号列表。
//
// 🌟 这里改成直接调用 rs485 库导出的 rs485.QueryCardsRepeated（原来 trap_monitor.go
// 里维护着一份几乎一样的私有实现，现在删掉了，统一用库里这一份）。
// 保留原来的行为：CPH 协议里查询用的地址码，用的是 deviceAddr 本身，而不是
// motorReaderMap 里 ReaderInfo.Address 字段（这是原代码的既有行为，这次只是搬家，不改行为）。
func runPresetCard(presetName string, deviceAddr byte) {
	fmt.Printf("\n【预设：%s】连续查询地址码 0x%02X 对应读卡器的卡号列表：\n", presetName, deviceAddr)
	ip, port := defaultHost, defaultCardReaderPort
	if reader, ok := motorReaderMap[deviceAddr]; ok {
		ip, port = reader.IP, reader.Port
	} else {
		fmt.Printf("  ✗ motorReaderMap 里没有配置地址码 0x%02X 对应的读卡器信息。使用默认值 %s\n", deviceAddr, defaultTarget)
	}
	if _, err := rs485.QueryCardsRepeated(uint16(deviceAddr), ip, port, cardModeDuration, cardModePollInterval); err != nil {
		fmt.Printf("  ✗ 查询失败：%v\n", err)
	}
}

// runCardMode 是 card00/card01/card05 三个预设合并后的统一读卡模式：
// 用法与 mode 一致——先输入 card 进入本模式，再输入十进制设备地址，
// 回车后连续读卡 cardModeDuration（2 秒），期间每隔 cardModePollInterval（50ms）查询一次，
// 结束后打印命中/未命中次数汇总。
func runCardMode(scanner *bufio.Scanner, presetAddr int) {
	if presetAddr < 0 {
		fmt.Printf("\n【读卡模式】请输入十进制设备地址（如 0、1、5），随后将连续读卡 %v（每 %v 查询一次）。", cardModeDuration, cardModePollInterval)
	}
	deviceAddr := resolveAddress(scanner, presetAddr, promptForCardAddress)
	if deviceAddr < 0 {
		fmt.Println("✗ 已取消操作。")
		return
	}
	runPresetCard(fmt.Sprintf("card%d", deviceAddr), byte(deviceAddr))
}

// runPresetCardOnce 根据设备地址码从 motorReaderMap 找到对应读卡器，只发送并读取一次
// （不做轮询），结束后打印本次是否命中以及读到的卡号。
//
// 🌟 同样改成调用 rs485 库导出的 rs485.QueryCardsOnce（原来 trap_monitor.go 里的私有实现
// 已删除），行为保持不变。
func runPresetCardOnce(presetName string, deviceAddr byte) {
	fmt.Printf("\n【预设：%s】只读一次地址码 0x%02X 对应读卡器的卡号：\n", presetName, deviceAddr)
	ip, port := defaultHost, defaultCardReaderPort
	if reader, ok := motorReaderMap[deviceAddr]; ok {
		ip, port = reader.IP, reader.Port
	} else {
		fmt.Printf("  ✗ motorReaderMap 里没有配置地址码 0x%02X 对应的读卡器信息。使用默认值 %s\n", deviceAddr, defaultTarget)
	}
	if _, err := rs485.QueryCardsOnce(uint16(deviceAddr), ip, port); err != nil {
		fmt.Printf("  ✗ 查询失败：%v\n", err)
	}
}

// runCardModeOnce 是"只读一次"模式：用法与 runCardMode 一致——先输入 card1 进入本模式，
// 再输入十进制设备地址，回车后只发送并读取一次查询指令（不做 2 秒轮询），
// 结束后打印本次是否命中以及读到的卡号。
func runCardModeOnce(scanner *bufio.Scanner, presetAddr int) {
	if presetAddr < 0 {
		fmt.Println("\n【只读一次模式】请输入十进制设备地址（如 0、1、5），随后只发送并读取一次查询指令。")
	}
	deviceAddr := resolveAddress(scanner, presetAddr, promptForCardAddress)
	if deviceAddr < 0 {
		fmt.Println("✗ 已取消操作。")
		return
	}
	runPresetCardOnce(fmt.Sprintf("card1_%d", deviceAddr), byte(deviceAddr))
}

// promptForCardAddress 提示用户输入十进制设备地址（0~255，对应 motorReaderMap 的 key）。
func promptForCardAddress(scanner *bufio.Scanner) int {
	for {
		fmt.Print("请输入设备地址（十进制，如 0、1、5，或 -1 取消）> ")
		if !scanner.Scan() {
			fmt.Println("\n输入中断，程序退出。")
			os.Exit(0)
		}

		input := strings.TrimSpace(scanner.Text())
		if input == "-1" || input == "cancel" {
			return -1
		}

		addr, err := strconv.Atoi(input)
		if err != nil {
			fmt.Printf("✗ 输入格式有误：%v，请输入十进制数字（如 5）\n", err)
			continue
		}

		if addr < 0 || addr > 0xFF {
			fmt.Println("✗ 地址范围应为 0 ~ 255")
			continue
		}

		fmt.Printf("✓ 已选择设备地址：%d（0x%02X）\n", addr, addr)
		return addr
	}
}

// promptForMode 询问用户本轮要使用的模式。
//
// 🌟 支持在选模式的同一行里直接带上十进制设备地址，比如 "up 5"、"check 17"，
// 这样不用先回车确认模式、再回车输入地址，两步简化成一步；不带地址（比如只输入 "up"）
// 就还是老样子，回车后再单独提示输入地址。
// 返回值：mode 本身，以及这一行里解析出的地址（没有带、或带的不合法就是 -1，
// 表示调用方要走 resolveAddress 再单独提示用户输入）。
func promptForMode(scanner *bufio.Scanner) (runMode, int) {
	fmt.Println("\n----------------------------------------")
	fmt.Println("请选择本轮运行模式：")
	fmt.Println("  直接回车 或 1  → 组合指令模式（开启/关闭两组指令，自动 开→等待→关）")
	fmt.Println("  2 或 s         → 单个指令模式（输入一条指令，立即发送一次）")
	fmt.Println("  up             → 01 路 开→关：输入后再输入十进制设备地址（如 0/1/5），对该地址执行 开(XX 05 00 01 01 FF) → 关(XX 05 00 01 01 00)；也可以直接输入 \"up 5\" 一次带上地址")
	fmt.Println("  down           → 02 路 开→关：输入后再输入十进制设备地址（如 0/1/5），对该地址执行 开(XX 05 00 01 02 FF) → 关(XX 05 00 01 02 00)；也可以直接输入 \"down 5\" 一次带上地址")
	fmt.Println("  check          → 查询状态：输入后再输入十进制设备地址（如 0/1/5），对该地址发送查询状态指令 XX 02 00 02 00 05；也可以直接输入 \"check 5\" 一次带上地址")
	fmt.Println("  card           → 读卡模式：输入后再输入十进制设备地址（如 0/1/5），连续读卡2秒（每50ms查询一次）并汇总命中/未命中次数；也可以直接输入 \"card 5\" 一次带上地址")
	fmt.Println("  card1          → 只读一次模式：用法同 card，只发送并读取一次查询指令，不做2秒轮询；也可以直接输入 \"card1 5\" 一次带上地址")
	fmt.Println("  mode           → 设置RFID读写器工作模式（主动/被动/触发）- 支持自定义设备地址；也可以直接输入 \"mode 5\" 一次带上地址")
	fmt.Println("  verify         → 带确认重试的开→保持→关：输入后再输入十进制设备地址，每步发送指令都会查询继电器状态确认生效，不符合预期自动重发（最多5次）；也可以直接输入 \"verify 5\" 一次带上地址")
	fmt.Println("  q / quit / exit → 退出程序")
	fmt.Print("请选择 > ")

	if !scanner.Scan() {
		fmt.Println("\n输入中断，程序退出。")
		os.Exit(0)
	}

	fields := strings.Fields(scanner.Text())
	if len(fields) == 0 {
		return modeCombo, -1
	}
	choice := strings.ToLower(fields[0])

	presetAddr := -1
	if len(fields) >= 2 {
		if v, err := strconv.Atoi(fields[1]); err == nil && v >= 0 && v <= 0xFF {
			presetAddr = v
		} else {
			fmt.Printf("  提示：%q 不是合法的十进制地址（应为 0~255），本轮仍会单独提示输入地址。\n", fields[1])
		}
	}

	switch choice {
	case "1":
		return modeCombo, presetAddr
	case "2", "s", "single":
		return modeSingle, presetAddr
	case "up":
		return modeUp, presetAddr
	case "down":
		return modeDown, presetAddr
	case "check":
		return modeCheck, presetAddr
	case "card":
		return modeCard, presetAddr
	case "card1":
		return modeCard1, presetAddr
	case "mode", "workmode", "setmode":
		return modeSetWorkMode, presetAddr
	case "verify", "vf":
		return modeVerify, presetAddr
	case "q", "quit", "exit":
		return modeQuit, presetAddr
	default:
		fmt.Println("  提示：无法识别的选项，已按默认的组合指令模式处理。")
		return modeCombo, -1
	}
}

// resolveAddress 如果这一行选模式时已经带上了合法地址（presetAddr>=0，比如 "up 5"），
// 就直接用它、不再重复问一遍；否则照常调用 prompt（promptForCardAddress 或
// promptForDeviceAddress）询问用户输入。
func resolveAddress(scanner *bufio.Scanner, presetAddr int, prompt func(*bufio.Scanner) int) int {
	if presetAddr >= 0 {
		fmt.Printf("✓ 已选择设备地址：%d（0x%02X）\n", presetAddr, presetAddr)
		return presetAddr
	}
	return prompt(scanner)
}

// promptForCommand 反复提示用户输入一条十六进制指令，直到格式合法为止，返回已附加 CRC 的完整报文。
func promptForCommand(scanner *bufio.Scanner, label string) []byte {
	for {
		fmt.Printf("%s指令（十六进制）> ", label)
		if !scanner.Scan() {
			fmt.Println("\n输入中断，程序退出。")
			os.Exit(0)
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			fmt.Println("  提示：输入不能为空，请重新输入。")
			continue
		}

		payload, err := buildPayloadWithCRC(line)
		if err != nil {
			fmt.Printf("  ✗ 指令格式有误：%v\n", err)
			fmt.Println("  示例：05 05 00 01 01 FF  或  0505000101FF（固定 6 字节，CRC 自动追加）")
			continue
		}
		return payload
	}
}

// sendAndPrint 发送一条已带 CRC 的报文并打印发送内容与回包结果。
func sendAndPrint(addr, label string, payload []byte) {
	fmt.Printf("  → 目标 %s，发送%s %d 字节：%s\n", addr, label, len(payload), rs485.FormatHex(payload))

	resp, err := sendAndReceive(addr, payload)
	if err != nil {
		fmt.Printf("  ✗ 通信失败：%v\n", err)
		return
	}
	if len(resp) == 0 {
		fmt.Println("  ← 收到回包：（无数据，设备在超时时间内未响应）")
	} else {
		fmt.Printf("  ← 收到回包：%s\n", rs485.FormatHex(resp))
	}
}

// runOpenCloseSequence 发送开启指令，按秒倒计时等待，再发送关闭指令。
func runOpenCloseSequence(addr string, openPayload, closePayload []byte, delaySeconds int) {
	fmt.Printf("\n两组指令已录入完成，开始执行流程（%d 秒后自动发送关闭指令）...\n", delaySeconds)

	sendAndPrint(addr, "开启指令", openPayload)

	warnAt := warnAheadSeconds
	if warnAt >= delaySeconds {
		warnAt = delaySeconds - 1 // 避免延时过短时提示与发送同时刷屏
	}

	for elapsed := 1; elapsed <= delaySeconds; elapsed++ {
		time.Sleep(1 * time.Second)
		remaining := delaySeconds - elapsed
		fmt.Printf("  ⏱ 倒计时：还剩 %d 秒\n", remaining)
		if remaining > 0 && remaining <= warnAt {
			fmt.Println("  ⚠ 即将发送关闭指令")
		}
	}

	sendAndPrint(addr, "关闭指令", closePayload)
	fmt.Println("流程执行完毕。")
}

func printBanner(addr string, delaySeconds int) {
	fmt.Println("========================================")
	fmt.Println("  RS485 TCP 指令发送工具 + RFID读写器工作模式设置")
	fmt.Println("========================================")
	fmt.Printf("目标设备：%s\n", addr)
	fmt.Printf("关闭延时：%d 秒\n", delaySeconds)
	fmt.Println()
	fmt.Println("使用说明：")
	fmt.Println("  · 每一轮都会先让你选择模式（直接回车 = 组合指令模式）")
	fmt.Println("  · 组合指令模式：输入【开启】【关闭】两组指令（各 6 字节十六进制）")
	fmt.Println("    自动执行：发送开启 → 倒计时等待 → 发送关闭")
	fmt.Println("  · 单个指令模式：输入一条指令，立即发送一次")
	fmt.Println("  · up：01 路 开→关，输入后再输入十进制设备地址（任意地址均可）")
	fmt.Println("  · down：02 路 开→关，输入后再输入十进制设备地址（任意地址均可）")
	fmt.Println("  · check：查询状态，输入后再输入十进制设备地址（任意地址均可），发送查询状态指令 XX 02 00 02 00 05")
	fmt.Println("  · card：读卡模式，输入后再输入十进制设备地址（如 0/1/5），连续读卡2秒（每50ms查询一次）并汇总命中/未命中次数")
	fmt.Println("  · card1：只读一次模式，用法同 card，只发送并读取一次查询指令，不做2秒轮询")
	fmt.Println("  · verify：带确认重试的 开→保持→关，输入设备地址后每步发送指令都会查询继电器状态确认生效，不符合预期自动重发（最多5次）")
	fmt.Println("  · 【新】mode/workmode：设置RFID读写器工作模式，支持自定义设备地址")
	fmt.Println("    └─ 主动模式（0）：上电后自动盘寻，接收指令可继续或停止")
	fmt.Println("    └─ 被动模式（1）：上电后不盘寻，接收指令后盘寻一次并停止")
	fmt.Println("    └─ 触发模式（2）：只有触发线有信号时才读卡")
	fmt.Println("  · 组合/单个指令模式：程序会自动计算 CRC-16 并追加到末尾（共发送 8 字节），支持带空格：05 05 00 01 01 FF，也支持无空格：0505000101FF")
	fmt.Println("  · up/down/check/card/card1/verify/mode 里的设备地址统一用【十进制】输入（如 0、1、5、17）")
	fmt.Println("  · 【新】up/down/check/card/card1/verify/mode 都可以在选模式那一行直接带上地址，比如 \"up 5\"，")
	fmt.Println("    不用再回车确认模式后单独输一次地址；不带地址就还是老样子，回车后再提示输入")
	fmt.Println("  · 在模式选择处输入 quit / exit / q 可退出程序")
	fmt.Println("========================================")
}

func parseTarget(arg string) (host, port string) {
	if h, p, err := net.SplitHostPort(arg); err == nil {
		return h, p
	}
	return arg, defaultPort
}

// buildPayloadWithCRC 把用户输入的十六进制字符串（支持空格/逗号等分隔，也支持不分隔，
// 如 "05 05 00 01 01 FF" 或 "0505000101FF"）拼成完整报文：payloadLen 个数据字节 + 2 字节 CRC16。
func buildPayloadWithCRC(s string) ([]byte, error) {
	data, err := parseHexString(s)
	if err != nil {
		return nil, err
	}
	if len(data) != payloadLen {
		return nil, fmt.Errorf("必须为 %d 个字节（%d 个十六进制字符），当前 %d 个字节", payloadLen, payloadLen*2, len(data))
	}

	crc := rs485.Crc16Modbus(data)
	payload := make([]byte, 0, payloadLen+2)
	payload = append(payload, data...)
	payload = append(payload, byte(crc&0xFF), byte(crc>>8))
	return payload, nil
}

func parseHexString(s string) ([]byte, error) {
	cleaned := hexCleanRE.ReplaceAllString(strings.TrimSpace(s), "")
	if cleaned == "" {
		return nil, fmt.Errorf("输入为空")
	}
	if len(cleaned)%2 != 0 {
		return nil, fmt.Errorf("十六进制字符数必须为偶数（当前 %d 个）", len(cleaned))
	}
	for _, c := range cleaned {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return nil, fmt.Errorf("包含非法字符 %q，只能使用 0-9 和 A-F", string(c))
		}
	}
	return hex.DecodeString(cleaned)
}

func sendAndReceive(addr string, payload []byte) ([]byte, error) {
	conn, err := net.DialTimeout("tcp", addr, connectTimeout)
	if err != nil {
		return nil, fmt.Errorf("连接 %s 失败: %w", addr, err)
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(readTimeout)); err != nil {
		return nil, err
	}

	if _, err := conn.Write(payload); err != nil {
		return nil, fmt.Errorf("发送失败: %w", err)
	}

	var buf [4096]byte
	n, err := conn.Read(buf[:])
	if err != nil {
		if n > 0 {
			fmt.Printf("  ← 出错前收到 %d 字节：%s\n", n, rs485.FormatHex(buf[:n]))
		}
		if err == io.EOF {
			return nil, nil
		}
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return nil, nil
		}
		return nil, fmt.Errorf("读取回包失败: %w", err)
	}
	return append([]byte(nil), buf[:n]...), nil
}

// ==================== UHF RFID CPH 协议支持 ====================

// buildRFWorkModeFrame 构建设置RFID读写器工作模式的CPH协议帧。
// address: 读写器地址, workMode: 0=主动 1=被动 2=触发
//
// 🌟 组帧（帧头+类型+地址+帧码+参数长度+参数+校验和）本身直接调用 rs485 库导出的
// rs485.BuildCardRFFrame，不用再在这里手工拼字节。
func buildRFWorkModeFrame(address uint16, workMode byte) []byte {
	// Working Parameter TLV:
	// TLV Type=0x23, Length=0x0F, 然后是15字节的参数
	// 结构：Version(1) + RFPower(1) + IntervalTime(1) + WorkMode(1) + Membank(1) +
	//      StartAddr(1) + Length(1) + FilterTime(1) + AddrMSB(1) + AddrLSB(1) +
	//      Beep(1) + Record(1) + TriggerTime(1) + AntennaFlagMSB(1) + AntennaFlagLSB(1)
	params := []byte{
		0x23,     // TLV Type: Working Parameter
		0x0F,     // TLV Length: 15 bytes
		0x05,     // Version
		0x14,     // RF Power (20dbm)
		0x06,     // Inventory Interval Time (60ms = 6*10ms)
		workMode, // Work Mode: 0=主动 1=被动 2=触发
		0x01,     // Inventory Membank: 1=EPC
		0x00,     // Inventory start addr
		0x00,     // Inventory length
		0x00,     // Filter Time: 0 seconds
		0x00,     // Device Addr MSB
		0x00,     // Device Addr LSB
		0x01,     // Beep Switch: 1=开启
		0x00,     // Record Flag
		0x00,     // Trigger Time
		0x00,     // Antenna Flag MSB
		0x01,     // Antenna Flag LSB
	}

	const frameCodeSetWorkParams = 0x41
	return rs485.BuildCardRFFrame(rs485.RFFrameTypeCommand, address, frameCodeSetWorkParams, params)
}

// runSetWorkMode 交互式设置RFID读写器的工作模式。
func runSetWorkMode(addr string, scanner *bufio.Scanner, presetAddr int) {
	fmt.Println("\n【设置RFID读写器工作模式】")
	fmt.Println("支持三种模式：")
	fmt.Println("  0 = 主动模式   （上电后自动盘寻，接收指令可继续盘寻或停止）")
	fmt.Println("  1 = 被动模式   （上电后不盘寻，接收指令后盘寻一次并停止）")
	fmt.Println("  2 = 触发模式   （只有触发线上有触发信号时才会读卡）")

	// 获取设备地址
	deviceAddr := resolveAddress(scanner, presetAddr, promptForDeviceAddress)
	if deviceAddr < 0 {
		fmt.Println("✗ 已取消操作。")
		return
	}

	// 选择工作模式
	workMode := promptForWorkMode(scanner)
	if workMode < 0 {
		fmt.Println("✗ 已取消操作。")
		return
	}

	// 构建CPH协议帧
	frame := buildRFWorkModeFrame(uint16(deviceAddr), byte(workMode))

	fmt.Printf("\n→ 向设备 %d(0x%04X) 发送设置命令，工作模式 %d...\n", deviceAddr, deviceAddr, workMode)
	fmt.Printf("  发送帧（%d 字节）：%s\n", len(frame), rs485.FormatHex(frame))

	resp, err := sendAndReceive(addr, frame)
	if err != nil {
		fmt.Printf("✗ 通信失败：%v\n", err)
		return
	}

	if len(resp) == 0 {
		fmt.Println("← 收到回包：（无数据，设备在超时时间内未响应）")
		fmt.Println("⚠ 可能原因：")
		fmt.Println("  1. 设备地址不匹配")
		fmt.Println("  2. 网络连接问题")
		fmt.Println("  3. 读写器未回复或回复超时")
		return
	}

	fmt.Printf("← 收到回包（%d 字节）：%s\n", len(resp), rs485.FormatHex(resp))

	// 简单的响应校验：检查帧头和帧类型
	if len(resp) >= 3 && resp[0] == 'R' && resp[1] == 'F' && resp[2] == rs485.RFFrameTypeResponse {
		// 响应帧格式：RF + FrameType(1) + Address(2) + FrameCode(1) + ParamLen(2) + Params + Checksum
		if len(resp) >= 8 {
			paramLen := int(resp[6])<<8 | int(resp[7])
			if paramsEnd := 8 + paramLen; paramsEnd <= len(resp) {
				tlvs := rs485.ParseCardRFTLVs(resp[8:paramsEnd])
				if len(tlvs) > 0 && tlvs[0].Type == rs485.RFTLVStatus && len(tlvs[0].Value) > 0 {
					statusCode := tlvs[0].Value[0]
					if statusCode == 0x00 {
						modeStr := ""
						switch workMode {
						case 0:
							modeStr = "主动模式"
						case 1:
							modeStr = "被动模式"
						case 2:
							modeStr = "触发模式"
						}
						fmt.Printf("✓ 设置成功！设备 0x%04X 已设置为 %s\n", deviceAddr, modeStr)
						return
					}
					fmt.Printf("✗ 设备返回错误状态码：0x%02X\n", statusCode)
					printStatusCodeDescription(statusCode)
					return
				}
			}
		}
		fmt.Println("✓ 设备已接收命令（响应帧有效）")
		return
	}

	fmt.Println("⚠ 回包格式异常，无法确定执行结果")
}

// promptForDeviceAddress 提示用户输入十进制设备地址。
func promptForDeviceAddress(scanner *bufio.Scanner) int {
	for {
		fmt.Print("请输入设备地址（十进制，如 0、1、5，或 -1 取消）> ")
		if !scanner.Scan() {
			fmt.Println("\n输入中断，程序退出。")
			os.Exit(0)
		}

		input := strings.TrimSpace(scanner.Text())
		if input == "-1" || input == "cancel" {
			return -1
		}

		addr, err := strconv.Atoi(input)
		if err != nil {
			fmt.Printf("✗ 输入格式有误：%v，请输入十进制数字（如 5）\n", err)
			continue
		}

		if addr < 0 || addr > 0xFF {
			fmt.Println("✗ 地址范围应为 0 ~ 255")
			continue
		}

		fmt.Printf("✓ 已选择设备地址：%d（0x%04X）\n", addr, addr)
		return addr
	}
}

// promptForWorkMode 提示用户选择工作模式。
func promptForWorkMode(scanner *bufio.Scanner) int {
	for {
		fmt.Print("请选择工作模式（0=主动 1=被动 2=触发，或 -1 取消）> ")
		if !scanner.Scan() {
			fmt.Println("\n输入中断，程序退出。")
			os.Exit(0)
		}

		input := strings.TrimSpace(scanner.Text())
		if input == "-1" || input == "cancel" {
			return -1
		}

		mode, err := strconv.Atoi(input)
		if err != nil || mode < 0 || mode > 2 {
			fmt.Println("✗ 请输入 0、1 或 2")
			continue
		}

		modeStr := []string{"主动模式", "被动模式", "触发模式"}[mode]
		fmt.Printf("✓ 已选择：%s\n", modeStr)
		return mode
	}
}

// printStatusCodeDescription 根据错误码打印详细说明（参考CPH协议文档）。
func printStatusCodeDescription(code byte) {
	descriptions := map[byte]string{
		0x00: "SUCCESS - 命令成功完成",
		0x14: "Parameter unsupport - 不支持的参数",
		0x15: "Parameter len error - 参数长度有误",
		0x16: "Parameter context error - 参数内容有误",
		0x17: "Unsupport command - 不支持的命令",
		0x18: "Device Address error - 设备地址不符",
		0x20: "Check Sum error - 校验码错误",
		0x21: "Unsupport TLV Type - 不支持的TLV类型",
		0x22: "Flash Error - Flash写入错误",
		0xFF: "Internal Error - 内部错误",
	}

	if desc, ok := descriptions[code]; ok {
		fmt.Printf("  错误说明：%s\n", desc)
	} else {
		fmt.Printf("  未知错误码：0x%02X\n", code)
	}
}
