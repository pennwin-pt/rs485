package rs485

import (
	"fmt"
	"log"
	"math"
	"sync"
	"time"
)

// AgentSet 是「楼层编号 -> 该楼层专属 CommandAgent」的集合。
//
// 🌟 背景：一楼和二楼各自走一条独立的 RS485/TCP 总线（不同网关），彼此的指令
// 完全不会在物理链路上撞车，所以不需要、也不应该共用同一个 CommandAgent：
// 共用会导致一楼的发送队列（stopCh/startCh）和冲突避让状态（activeOps）
// 被二楼的任务牵连排队/错峰，白白浪费"两条总线可以完全并行"这个优势。
//
// AgentSet 只是「楼层号 -> *CommandAgent」的一层薄封装：main.go 按楼层各自
// New 一个 CommandAgent 装进这个 map，后续所有需要下发指令的地方都先按
// cellID/task 上带的楼层号 Get 出对应的 agent，再调用 Dispatch/DispatchQuery/
// DispatchSequence，用法与直接持有单个 *CommandAgent 完全一样。
type AgentSet map[int]*CommandAgent

// Get 返回指定楼层对应的 CommandAgent。
//
// 🚨 楼层号在 AgentSet 里找不到时会直接 panic：这说明调用方传入了一个系统里
// 根本没有配置总线的楼层号，属于初始化阶段的配置错误（main.go 组装 AgentSet
// 时漏配了某个楼层），应该在开发/联调阶段第一时间暴露出来，而不是静默拿到
// nil 指针、在某次硬件指令发送时才崩溃在不相关的地方。
func (s AgentSet) Get(floor int) *CommandAgent {
	agent, ok := s[floor]
	if !ok {
		panic(fmt.Sprintf("楼层 %d 没有配置对应的 CommandAgent（RS485 总线代理），请检查 main.go 里 AgentSet 的初始化", floor))
	}
	return agent
}

// DefaultMinGap 是两次「关闭」事件之间默认的最小安全间隔。
//
// 🌟 为什么需要这个间隔：SendFrame 每次都会重新建立一次 TCP 连接（拨号 + 写入），
// 不是零耗时的。如果两个设备的关闭时间几乎撞在同一瞬间，其中一个的关闭指令必然会
// 被 dispatchLoop 的单一发送协程排在后面、往后拖延，导致实际保持时长比配置的秒数
// 多出去一截。这个间隔就是用来在派发新任务时提前"避让"，宁可让新任务的开始时间
// 晚一点，也不让两个关闭指令抢在同一时刻发送。
// 如果现场网络延迟明显更大（比如网关响应慢），可以调大这个值。
const DefaultMinGap = 300 * time.Millisecond

// activeOp 描述当前已经发出"开"指令、还没发送"关"指令的一次操作。
type activeOp struct {
	deviceAddr byte
	channel    byte
	stopAt     time.Time
}

// CommandAgent 是向硬件下发升降指令的统一代理。
//
// 设计目标（对应实际的调度诉求）：
//   - 调用方只需要 Dispatch(deviceAddr, channel, holdSeconds)：告诉代理给哪个设备的
//     哪一路发指令、几秒后自动关闭；不需要自己 sleep、自己发关闭指令。
//   - Dispatch 发送完"开"指令就立刻返回（不等 holdSeconds 走完），调用方可以马上
//     派发下一个设备的任务，让多个设备的动作首尾重叠、整体提速。
//   - "关"指令拥有最高优先级：到点后会插队到还没来得及发送的"开"指令前面，
//     保证关闭时机尽量准点，不会被新任务的开指令挡住。
//   - 派发新任务时会预估这次任务的关闭时间会不会跟当前正在跑的其它任务的关闭时间
//     撞在一起（间隔小于 minGap），如果会，就把这次任务的开始时间顺延，
//     宁可晚开始，也不让两个关闭指令挤在同一瞬间发送。
//
// 🌟 所有指令最终都走同一条 RS485/TCP 链路，同一时刻只应该有一帧数据在发送，
// 所以内部用单一的调度协程把所有实际发送串行化，调用方不需要关心并发安全问题。
//
// 🌟 每个电机现在都有独立的网关 IP/端口（不再按设备地址区间共用一个网关地址），
// 所以 CommandAgent 自己不再持有任何网关地址：调用 Dispatch / DispatchSequence 时
// 由调用方把这个设备自己的网关地址（通常是 config.CellDeviceInfo.MotorIP:MotorPort）
// 一并传进来，跟 rs485.SendOpen/SendClose 的参数约定保持一致。
type CommandAgent struct {
	minGap time.Duration // 两次「关闭」事件之间的最小安全间隔

	mu        sync.Mutex
	activeOps []*activeOp // 当前已发"开"、还没发"关"的任务，用于新任务的冲突检测

	stopCh  chan func() // 高优先级队列：到点该发的"关"指令
	startCh chan func() // 普通优先级队列：可以发"开"指令了
}

// NewCommandAgent 构造一个硬件指令代理。minGap <= 0 时使用 DefaultMinGap。
func NewCommandAgent(minGap time.Duration) *CommandAgent {
	if minGap <= 0 {
		minGap = DefaultMinGap
	}
	a := &CommandAgent{
		minGap:  minGap,
		stopCh:  make(chan func(), 64),
		startCh: make(chan func(), 64),
	}
	go a.dispatchLoop()
	return a
}

// DefaultQueryReadTimeout 是 DispatchQuery 等待设备回包的读超时。
//
// 🌟 特意比 rs485.DefaultWriteTimeout（3 秒）短很多：查询指令现在跟开/关/复位指令
// 共用同一条发送队列（同一个 dispatchLoop 协程），一次查询要占用这个协程直到读到
// 回包或者超时为止。如果沿用 3 秒的超时，一旦某次查询恰好遇到网络问题迟迟收不到
// 回包，会让 dispatchLoop 卡住最长 3 秒，期间哪怕有到点该发的"关"指令也只能干等。
// 缩短超时可以让"读不到就尽快放弃"，把总线尽快让给后面排队的指令；
// 具体数值（当前 800ms）可以按现场网络实际情况调整。
const DefaultQueryReadTimeout = 800 * time.Millisecond

// DispatchQuery 派发一次「查询触发状态」任务，直接排进 CommandAgent 唯一的
// 发送队列（startCh）串行执行，返回设备回包。
//
// 🌟 之所以要经过这条队列而不是像以前那样直接调包内的裸函数发送：485 总线上同一
// 时刻只能有一帧数据在跑，查询指令和开/关/复位指令现在共用同一条总线，查询必须
// 排进 CommandAgent 唯一的发送队列，不能绕开它直接发，否则还是会跟 Dispatch/
// DispatchSequence 正在发的开/关指令撞车。
//
// 🌟 查询没有"保持时长"、也不需要跟其它任务的关闭时间错峰，所以不走
// reserveSafeStartTime 那套冲突检测/避让逻辑——直接排进 startCh 即可：前面没有
// 排队就立刻发送；有未发完的开/关指令就按 FIFO 跟在后面。到点该发的"关"指令
// （stopCh）仍然拥有最高优先级，可以插到查询前面——查询本身对时机不敏感，
// 被"关"指令抢先完全没问题。
//
// 🚨 调用会阻塞到这次查询真正执行完（发送 + 读回包，读超时见 DefaultQueryReadTimeout）
// 为止；这段时间里 dispatchLoop 也被这次查询占住，新到达的"关"指令只能先在
// stopCh 里排队等它处理完。
func (a *CommandAgent) DispatchQuery(addr string, deviceAddr byte) ([]byte, error) {
	var resp []byte
	var err error
	done := make(chan struct{})
	a.startCh <- func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("查询指令执行时发生异常: %v", r)
				log.Printf("[CommandAgent DispatchQuery] 任务执行抛出异常: %v", r)
			}
			close(done)
		}()
		resp, err = QueryDeviceTrigger(addr, deviceAddr, DefaultQueryReadTimeout)
	}
	<-done // 阻塞等待这次查询在 dispatchLoop 单线程队列里排队并串行执行完毕
	return resp, err
}

const MinSendInterval = 50 * time.Millisecond // 可调，按现场实测结果调整

// dispatchLoop 是唯一真正往硬件发送数据的协程，天然把所有发送串行化。
// 每一轮都先非阻塞地检查 stopCh：只要有"关"指令在排队，就优先处理，处理完继续检查
// （可能连续插队好几个），直到 stopCh 暂时空了，才会去看 startCh —— 这就是"插队机制"。
func (a *CommandAgent) dispatchLoop() {
	var lastSendAt time.Time
	throttle := func() {
		if gap := MinSendInterval - time.Since(lastSendAt); gap > 0 {
			time.Sleep(gap)
		}
		lastSendAt = time.Now()
	}

	for {
		select {
		case job := <-a.stopCh:
			throttle()
			job()
			continue
		default:
		}

		select {
		case job := <-a.stopCh:
			throttle()
			job()
		case job := <-a.startCh:
			throttle()
			job()
		}
	}
}

// Dispatch 派发一次「设备升降」任务：
//   - deviceAddr / channel：给哪个设备的哪一路发指令（channel 用 ChannelUp / ChannelDown）；
//   - holdSeconds：开启后保持多少秒再自动关闭，支持 1 位小数（内部会四舍五入收口到 1 位）；
//   - onStopped：设备"确认已停止"时的回调——也就是"关"指令真正被发送出去之后触发，
//     参数是发送"关"指令的结果（nil 表示发送成功）。onStopped 可以传 nil 表示不关心。
//
// 🌟 onStopped 永远在一个独立的新 goroutine 里调用（`go onStopped(err)`），
// 不会挤占 dispatchLoop 这条唯一的硬件发送队列——因为调用方的回调里可能会做
// 比较耗时的事情（比如按业务规则做"完成事件"限流、sleep 一段时间再更新状态），
// 如果直接在 dispatchLoop 里同步调用，会拖慢其它设备指令的发送时机。
//
// 调用会阻塞到"开"指令真正发送完成为止：通常很快就返回，只有在因为要避让其它任务的
// 关闭时间而需要顺延开始时间时才会多等一会儿；不会等待 holdSeconds 走完，
// 所以调用方可以在这之后立刻处理下一个设备。
//
// 返回值只反映"开"指令有没有发送成功。设备"是否已经真正停止"这件事，
// 必须通过 onStopped 回调才能确切知道——调用方如果需要"确认停止后才算这个任务完成"，
// 应该在 onStopped 里做状态流转，而不是看 Dispatch 的返回值。
func (a *CommandAgent) Dispatch(addr string, deviceAddr byte, channel byte, holdSeconds float64, onStopped func(err error)) error {
	hold := durationFromSeconds(roundToOneDecimal(holdSeconds))

	// 🌟 reserveSafeStartTime 现在是"边检查边登记"的原子操作：一旦确定某个开始时间不会
	// 和已有任务的关闭时间冲突，会在同一把锁内立刻把这次任务登记进 activeOps 再返回。
	// 这对并发场景（多个 goroutine 几乎同时调用 Dispatch）至关重要：如果检查和登记分成
	// 两步，两个几乎同时发起的调用可能都在对方登记之前完成检查，都误判"没有冲突"，
	// 导致它们的关闭时间还是会撞在一起——旧实现只在"派发循环同步调用 Dispatch"时
	// 因为天然串行而没有暴露这个竞态，一旦改成并发派发就会失效。
	//
	// 返回的 startAt 通常就是"现在"（没有冲突，立刻发送）；只有确实需要错峰时才会比
	// 现在晚一点点（错峰的量级是 minGap，不是整个 hold），下面按需 sleep 到这个时刻。
	op, startAt := a.reserveSafeStartTime(deviceAddr, channel, hold)
	if d := time.Until(startAt); d > 0 {
		time.Sleep(d)
	}

	errCh := make(chan error, 1)
	a.startCh <- func() {
		errCh <- SendOpen(addr, deviceAddr, channel)
	}
	if err := <-errCh; err != nil {
		if op != nil {
			// 开指令发送失败，这次任务根本不会真正启动，撤销预定，
			// 避免这个（设备、channel、错误的 stopAt）一直占着 activeOps 的名额。
			a.releaseActiveOp(op)
		}
		return err
	}

	if hold <= 0 {
		// 保持时长为 0（理论上不应该出现，调用方一般会校验 holdSeconds > 0），
		// 没有"关闭"这一步，直接视为"已停止"通知调用方，不往 activeOps 里塞脏数据。
		if onStopped != nil {
			go onStopped(nil)
		}
		return nil
	}

	// reserveSafeStartTime 登记时用的是"预估"的 stopAt（基于调用时刻），
	// 这里用"开"指令真正发送完成的时刻重新计算一次，让后续新任务的冲突检测更精确。
	a.mu.Lock()
	op.stopAt = time.Now().Add(hold)
	a.mu.Unlock()

	time.AfterFunc(hold, func() {
		a.stopCh <- func() {
			err := SendClose(addr, deviceAddr, channel)
			if err != nil {
				log.Printf("[硬件指令代理] 设备 0x%02X 关闭指令发送失败: %v", deviceAddr, err)
			}
			a.releaseActiveOp(op)
			if onStopped != nil {
				go onStopped(err)
			}
		}
	})

	return nil
}

// reserveSafeStartTime 找一个"从这里开始发'开'指令"的候选时间，使得对应的关闭时间
// 和当前所有正在跑的其它任务的关闭时间都错开至少 minGap；一旦找到，会在同一次加锁内
// 立刻把这次任务登记进 activeOps（op 的 stopAt 先用候选值占位，真正发送完成后
// Dispatch 会再用精确时间校正一次），然后把 (op, 对应的开始时间) 一起返回。
//
// 🌟 这里"检查冲突"和"登记预定"是同一次加锁内完成的原子操作——一旦某一轮发现没有
// 冲突，立刻在这把锁里把 activeOps 加进去再解锁返回，而不是把登记推迟到调用方后续
// 某个时间点（那样会留出一段"我看起来没冲突，但其实还没登记"的窗口，多个并发调用
// 可能会同时挤进这个窗口，都以为自己没冲突，实际上关闭时间还是会撞车）。
//
// 🚨 关键点（踩过的坑）：冲突时不能简单地把"开始时间"推到"冲突任务的关闭时间 + minGap"
// ——那样算出来的新关闭时间会是 冲突关闭时间 + minGap + hold，比冲突关闭时间整整晚了
// 一个 hold（往往有好几秒甚至几十秒），相当于让新任务平白多等了一整个 hold 时长。
// 真正需要错开 minGap 的是两个任务各自的【关闭时间】，所以要反过来推：
// 先定「候选关闭时间 = 冲突关闭时间 + minGap」，再倒推「候选开始时间 = 候选关闭时间 - hold」。
// 这样多个 hold 相同（比如同一批格子共用的 downSeconds）的任务，彼此的开始时间
// 只会依次错开 minGap 那么一点点，不会被拖成一个个串行等待整个 hold 的节奏。
//
// hold<=0（没有关闭动作，不需要参与冲突调度）时不登记进 activeOps，op 返回 nil，
// startAt 直接是调用时刻，不需要等待。
func (a *CommandAgent) reserveSafeStartTime(deviceAddr, channel byte, hold time.Duration) (*activeOp, time.Time) {
	candidateStart := time.Now()

	for {
		a.mu.Lock()
		candidateStop := candidateStart.Add(hold)

		var conflictStop time.Time
		for _, op := range a.activeOps {
			diff := op.stopAt.Sub(candidateStop)
			if diff < 0 {
				diff = -diff
			}
			if diff < a.minGap && op.stopAt.After(conflictStop) {
				conflictStop = op.stopAt
			}
		}

		if conflictStop.IsZero() {
			var op *activeOp
			if hold > 0 {
				op = &activeOp{
					deviceAddr: deviceAddr,
					channel:    channel,
					stopAt:     candidateStop,
				}
				a.activeOps = append(a.activeOps, op)
			}
			a.mu.Unlock()
			return op, candidateStart
		}
		a.mu.Unlock()

		// 反推：让新任务的关闭时间恰好落在"冲突关闭时间 + minGap"，
		// 而不是把开始时间本身推到那里。
		newStart := conflictStop.Add(a.minGap).Add(-hold)
		if !newStart.After(candidateStart) {
			// 防御性保护：理论上既然检测到冲突，反推出的 newStart 必然比当前
			// candidateStart 更晚；万一出现精度问题导致没有严格递增，
			// 这里保证 candidateStart 至少单调前进 minGap，避免死循环。
			newStart = candidateStart.Add(a.minGap)
		}
		candidateStart = newStart
	}
}

func (a *CommandAgent) releaseActiveOp(target *activeOp) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for i, op := range a.activeOps {
		if op == target {
			a.activeOps = append(a.activeOps[:i], a.activeOps[i+1:]...)
			return
		}
	}
}

func durationFromSeconds(seconds float64) time.Duration {
	return time.Duration(seconds * float64(time.Second))
}

// roundToOneDecimal 把秒数四舍五入保留小数点后 1 位，跟 config.RoundSeconds 的约定一致
// （holdSeconds 全项目统一只支持 1 位小数）。这里不直接依赖 internal/config 包，
// 避免底层的 rs485 包反过来依赖上层的 config 包，保持依赖方向单向、干净。
func roundToOneDecimal(seconds float64) float64 {
	return math.Round(seconds*10) / 10
}

// ---------------------------------------------------------------------
// 🌟 多段任务：DispatchSequence
//
// 背景：像"设备初始化"这种场景，单个设备需要严格按顺序执行两段动作——
// 先下降到最低（保持 downSeconds，确认关闭），再上升复位（保持 upSeconds，确认关闭）——
// 上一段必须"关"指令真正发送完成后，才能开始下一段的"开"指令，不能像激活那样
// 一个设备只有一段任务、发完"开"就不用管了。
//
// 但调用方不想为了这个"先后依赖"就退化成自己 sleep/自己拿 channel 阻塞等待
// （那样会让多个设备之间没法首尾重叠、整体变慢，等于放弃了 Dispatch 本来的调度能力）。
//
// DispatchSequence 就是把这种"设备内部严格串行、设备之间仍然可以首尾重叠"的需求，
// 建立在现有 Dispatch 之上：每一段都是一次完整的 Dispatch 调用，上一段的 onStopped
// 回调（本来就已经跑在独立 goroutine 里，不占用 dispatchLoop）里再去发起下一段的
// Dispatch，天然形成"确认上一段真正停止后，才安全地开始下一段"的链条，
// 复用的仍然是 Dispatch 内部的冲突避让（reserveSafeStartTime）和"关"指令插队优先级，
// 不需要新增任何并发原语，Dispatch 本身的实现完全不用改动。
// ---------------------------------------------------------------------

// SequenceStep 描述多段任务里的一段：走哪一路（ChannelUp/ChannelDown）、保持多少秒。
type SequenceStep struct {
	Channel     byte
	HoldSeconds float64
}

// DispatchSequence 派发一个"多段"任务：按 steps 顺序逐段执行"开 -> 保持 -> 关"，
// 前一段的"关"指令确认发送完成（且发送成功）后，才会开始下一段的"开"指令；
// 任意一段发送失败会立即中止后续段、直接把这个失败结果通过 onStopped 报给调用方；
// 只有最后一段的"关"指令确认发送完成后，才会调用 onStopped(nil) 通知"整个 sequence 已停止"。
//
// 🎯 典型用法（设备初始化）：
//
//	agent.DispatchSequence(addr, devAddr, []rs485.SequenceStep{
//	    {Channel: rs485.ChannelDown, HoldSeconds: downSeconds}, // 先降到最低
//	    {Channel: rs485.ChannelUp, HoldSeconds: devInfo.UpSeconds}, // 再升起复位
//	}, func(err error) {
//	    // err == nil 表示两段都已经确认真正停止（降完 + 升完），可以安全标记这个格子初始化成功
//	})
//
// 🌟 调用行为与单段的 Dispatch 完全一致：只阻塞到第一段"开"指令真正发送完成为止就返回，
// 不会等待整个 sequence 跑完——调用方可以立刻去派发下一个设备的 sequence，
// 多个设备的"降->升"周期可以互相首尾重叠，速度不会因为改成多段任务而退化成串行等待。
// 返回值同样只反映"第一段开指令有没有发送成功"；后续每一段是否成功，只能通过
// onStopped 拿到最终结果。
//
// 🚨 steps 为空时视为没有任何动作，直接在独立 goroutine 里调用 onStopped(nil)，
// 不会往硬件发送任何指令，也不会阻塞调用方。
func (a *CommandAgent) DispatchSequence(addr string, deviceAddr byte, steps []SequenceStep, onStopped func(err error)) error {
	if len(steps) == 0 {
		if onStopped != nil {
			go onStopped(nil)
		}
		return nil
	}
	return a.dispatchSequenceStep(addr, deviceAddr, steps, 0, onStopped)
}

// dispatchSequenceStep 是 DispatchSequence 的内部递归实现：派发 steps[idx] 这一段，
// 并在它的 onStopped 里（已经是 Dispatch 自己开的独立 goroutine，不占用 dispatchLoop）
// 决定是"继续派发下一段"还是"整个 sequence 到此结束、把结果报给外层 onStopped"。
func (a *CommandAgent) dispatchSequenceStep(addr string, deviceAddr byte, steps []SequenceStep, idx int, onStopped func(err error)) error {
	step := steps[idx]
	isLast := idx == len(steps)-1

	return a.Dispatch(addr, deviceAddr, step.Channel, step.HoldSeconds, func(err error) {
		if err != nil || isLast {
			// 要么这一段本身失败了（不该、也不能再继续后面的段），
			// 要么这已经是最后一段——两种情况都直接把结果报给外层调用方，sequence 结束。
			if onStopped != nil {
				onStopped(err)
			}
			return
		}

		// 这一段确认停止且成功、后面还有段：继续派发下一段。
		// 这里仍然在 Dispatch 为上一段开的独立 goroutine 里，dispatchSequenceStep
		// 内部调用 a.Dispatch 时哪怕因为要避让冲突而 time.Sleep，也只会让这个 goroutine
		// 多等一会儿，不会阻塞 dispatchLoop、不会影响其它设备指令的发送时机。
		if dispatchErr := a.dispatchSequenceStep(addr, deviceAddr, steps, idx+1, onStopped); dispatchErr != nil {
			// 下一段的"开"指令发送失败：这是同步返回的错误，Dispatch 内部不会再触发
			// 一次 onStopped，所以这里要主动把它报给外层，保证 onStopped 总是恰好被调用一次。
			if onStopped != nil {
				onStopped(dispatchErr)
			}
		}
	})
}
