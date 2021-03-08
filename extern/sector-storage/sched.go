package sectorstorage

import (
	"context"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

type schedPrioCtxKey int

var SchedPriorityKey schedPrioCtxKey
var DefaultSchedPriority = 0
var SelectorTimeout = 5 * time.Second
var InitWait = 3 * time.Second

var (
	SchedWindows = 2
)

func getPriority(ctx context.Context) int {
	sp := ctx.Value(SchedPriorityKey)
	if p, ok := sp.(int); ok {
		return p
	}

	return DefaultSchedPriority
}

func WithPriority(ctx context.Context, priority int) context.Context {
	return context.WithValue(ctx, SchedPriorityKey, priority)
}

const mib = 1 << 20

type WorkerAction func(ctx context.Context, w Worker) error

type WorkerSelector interface {
	Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, a *workerHandle) (bool, error) // true if worker is acceptable for performing a task

	Cmp(ctx context.Context, task sealtasks.TaskType, a, b *workerHandle) (bool, error) // true if a is preferred over b
}

type scheduler struct {
	workersLk sync.RWMutex
	workers   map[WorkerID]*workerHandle

	schedule       chan *workerRequest
	windowRequests chan *schedWindowRequest
	workerChange   chan struct{} // worker added / changed/freed resources
	workerDisable  chan workerDisableReq

	// owned by the sh.runSched goroutine
	//任务请求队列
	schedQueue  *requestQueue
	openWindows []*schedWindowRequest

	workTracker *workTracker

	info chan func(interface{})

	closing  chan struct{}
	closed   chan struct{}
	testSync chan struct{} // used for testing

	//////////////////////////
	//自定义功能 begin,blueforest 2021.2.22
	//sector和worker的hostname对应map
	sectorToHostnameLk sync.RWMutex
	sectorToHostname   map[abi.SectorID]string

	//自定义功能 end,blueforest
	//////////////////////////

}

type workerHandle struct {
	workerRpc Worker

	info storiface.WorkerInfo

	preparing *activeResources
	active    *activeResources

	lk sync.Mutex

	wndLk         sync.Mutex
	activeWindows []*schedWindow

	enabled bool

	// for sync manager goroutine closing
	cleanupStarted bool
	closedMgr      chan struct{}
	closingMgr     chan struct{}
}

type schedWindowRequest struct {
	worker WorkerID

	done chan *schedWindow
}

type schedWindow struct {
	allocated activeResources
	todo      []*workerRequest
}

type workerDisableReq struct {
	activeWindows []*schedWindow
	wid           WorkerID
	done          func()
}

type activeResources struct {
	memUsedMin uint64
	memUsedMax uint64
	gpuUsed    bool
	cpuUse     uint64

	cond *sync.Cond
}

type workerRequest struct {
	sector   storage.SectorRef
	taskType sealtasks.TaskType
	priority int // larger values more important
	sel      WorkerSelector

	prepare WorkerAction
	work    WorkerAction

	start time.Time

	index int // The index of the item in the heap.

	indexHeap int
	ret       chan<- workerResponse
	ctx       context.Context
}

type workerResponse struct {
	err error
}

func newScheduler() *scheduler {
	return &scheduler{
		workers: map[WorkerID]*workerHandle{},

		schedule:       make(chan *workerRequest),
		windowRequests: make(chan *schedWindowRequest, 20),
		workerChange:   make(chan struct{}, 20),
		workerDisable:  make(chan workerDisableReq),

		schedQueue: &requestQueue{},

		workTracker: &workTracker{
			done:    map[storiface.CallID]struct{}{},
			running: map[storiface.CallID]trackedWork{},
		},

		info: make(chan func(interface{})),

		closing: make(chan struct{}),
		closed:  make(chan struct{}),

		//////////////////////////
		//自定义功能 begin,blueforest 2021.2.22
		//sector和worker的hostname对应map
		sectorToHostname: map[abi.SectorID]string{},
		//自定义功能 end,blueforest
		//////////////////////////
	}
}

func (sh *scheduler) Schedule(ctx context.Context, sector storage.SectorRef, taskType sealtasks.TaskType, sel WorkerSelector, prepare WorkerAction, work WorkerAction) error {
	ret := make(chan workerResponse)

	select {
	case sh.schedule <- &workerRequest{
		sector:   sector,
		taskType: taskType,
		priority: getPriority(ctx),
		sel:      sel,

		prepare: prepare,
		work:    work,

		start: time.Now(),

		ret: ret,
		ctx: ctx,
	}:
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case resp := <-ret:
		return resp.err
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *workerRequest) respond(err error) {
	select {
	case r.ret <- workerResponse{err: err}:
	case <-r.ctx.Done():
		log.Warnf("request got cancelled before we could respond")
	}
}

type SchedDiagRequestInfo struct {
	Sector   abi.SectorID
	TaskType sealtasks.TaskType
	Priority int
}

type SchedDiagInfo struct {
	Requests    []SchedDiagRequestInfo
	OpenWindows []string
}

//计划处理程序
func (sh *scheduler) runSched() {
	defer close(sh.closed)

	iw := time.After(InitWait)
	var initialised bool

	for {
		var doSched bool
		var toDisable []workerDisableReq

		select {
		case <-sh.workerChange:
			doSched = true
		case dreq := <-sh.workerDisable:
			toDisable = append(toDisable, dreq)
			doSched = true
		case req := <-sh.schedule:
			sh.schedQueue.Push(req)
			doSched = true

			if sh.testSync != nil {
				sh.testSync <- struct{}{}
			}
		case req := <-sh.windowRequests:
			sh.openWindows = append(sh.openWindows, req)
			doSched = true
		case ireq := <-sh.info:
			ireq(sh.diag())

		case <-iw:
			initialised = true
			iw = nil
			doSched = true
		case <-sh.closing:
			sh.schedClose()
			return
		}

		if doSched && initialised {
			// First gather any pending tasks, so we go through the scheduling loop
			// once for every added task
		loop:
			for {
				select {
				case <-sh.workerChange:
				case dreq := <-sh.workerDisable:
					toDisable = append(toDisable, dreq)
				case req := <-sh.schedule:
					sh.schedQueue.Push(req)
					if sh.testSync != nil {
						sh.testSync <- struct{}{}
					}
				case req := <-sh.windowRequests:
					sh.openWindows = append(sh.openWindows, req)
				default:
					break loop
				}
			}

			for _, req := range toDisable {
				for _, window := range req.activeWindows {
					for _, request := range window.todo {
						sh.schedQueue.Push(request)
					}
				}

				openWindows := make([]*schedWindowRequest, 0, len(sh.openWindows))
				for _, window := range sh.openWindows {
					if window.worker != req.wid {
						openWindows = append(openWindows, window)
					}
				}
				sh.openWindows = openWindows

				sh.workersLk.Lock()
				sh.workers[req.wid].enabled = false
				sh.workersLk.Unlock()

				req.done()
			}

			sh.trySched()
		}

	}
}

func (sh *scheduler) diag() SchedDiagInfo {
	var out SchedDiagInfo

	for sqi := 0; sqi < sh.schedQueue.Len(); sqi++ {
		task := (*sh.schedQueue)[sqi]

		out.Requests = append(out.Requests, SchedDiagRequestInfo{
			Sector:   task.sector.ID,
			TaskType: task.taskType,
			Priority: task.priority,
		})
	}

	sh.workersLk.RLock()
	defer sh.workersLk.RUnlock()

	for _, window := range sh.openWindows {
		out.OpenWindows = append(out.OpenWindows, uuid.UUID(window.worker).String())
	}

	return out
}

func (sh *scheduler) trySched() {
	/*
		This assigns tasks to workers based on:
		- Task priority (achieved by handling sh.schedQueue in order, since it's already sorted by priority)
		- Worker resource availability
		- Task-specified worker preference (acceptableWindows array below sorted by this preference)
		- Window request age

		1. For each task in the schedQueue find windows which can handle them
		1.1. Create list of windows capable of handling a task
		1.2. Sort windows according to task selector preferences
		2. Going through schedQueue again, assign task to first acceptable window
		   with resources available
		3. Submit windows with scheduled tasks to workers

	*/

	sh.workersLk.RLock()
	defer sh.workersLk.RUnlock()

	windowsLen := len(sh.openWindows)
	queuneLen := sh.schedQueue.Len()

	log.Debugf("SCHED %d queued; %d open windows", queuneLen, windowsLen)

	if windowsLen == 0 || queuneLen == 0 {
		// nothing to schedule on
		return
	}

	windows := make([]schedWindow, windowsLen)
	acceptableWindows := make([][]int, queuneLen)

	// Step 1
	throttle := make(chan struct{}, windowsLen)

	var wg sync.WaitGroup
	wg.Add(queuneLen)
	for i := 0; i < queuneLen; i++ {
		throttle <- struct{}{}

		// 处理任务
		go func(sqi int) {
			defer wg.Done()
			defer func() {
				<-throttle
			}()

			task := (*sh.schedQueue)[sqi]
			needRes := ResourceTable[task.taskType][task.sector.ProofType]

			task.indexHeap = sqi
			for wnd, windowRequest := range sh.openWindows {
				worker, ok := sh.workers[windowRequest.worker]
				if !ok {
					log.Errorf("worker referenced by windowRequest not found (worker: %s)", windowRequest.worker)
					// TODO: How to move forward here?
					continue
				}

				if !worker.enabled {
					log.Debugw("skipping disabled worker", "worker", windowRequest.worker)
					continue
				}

				// TODO: allow bigger windows
				if !windows[wnd].allocated.canHandleRequest(needRes, windowRequest.worker, "schedAcceptable", worker.info.Resources) {
					continue
				}

				rpcCtx, cancel := context.WithTimeout(task.ctx, SelectorTimeout)
				ok, err := task.sel.Ok(rpcCtx, task.taskType, task.sector.ProofType, worker)
				cancel()
				if err != nil {
					log.Errorf("trySched(1) req.sel.Ok error: %+v", err)
					continue
				}

				if !ok {
					continue
				}

				{
					//////////////////////////
					//自定义功能 begin,blueforest 2021.2.22
					//任务分配，添加符合条件的worker
					//判断task的sector和worker是否匹配
					if !sh.canWorkerHandleRequest(windowRequest.worker, worker, task) {
						continue
					}
					//自定义功能 end,blueforest
					//////////////////////////
				}

				acceptableWindows[sqi] = append(acceptableWindows[sqi], wnd)
			}

			if len(acceptableWindows[sqi]) == 0 {
				return
			}

			// Pick best worker (shuffle in case some workers are equally as good)
			rand.Shuffle(len(acceptableWindows[sqi]), func(i, j int) {
				acceptableWindows[sqi][i], acceptableWindows[sqi][j] = acceptableWindows[sqi][j], acceptableWindows[sqi][i] // nolint:scopelint
			})
			sort.SliceStable(acceptableWindows[sqi], func(i, j int) bool {
				wii := sh.openWindows[acceptableWindows[sqi][i]].worker // nolint:scopelint
				wji := sh.openWindows[acceptableWindows[sqi][j]].worker // nolint:scopelint

				if wii == wji {
					// for the same worker prefer older windows
					return acceptableWindows[sqi][i] < acceptableWindows[sqi][j] // nolint:scopelint
				}

				wi := sh.workers[wii]
				wj := sh.workers[wji]

				rpcCtx, cancel := context.WithTimeout(task.ctx, SelectorTimeout)
				defer cancel()

				r, err := task.sel.Cmp(rpcCtx, task.taskType, wi, wj)
				if err != nil {
					log.Errorf("selecting best worker: %s", err)
				}
				return r
			})
		}(i)
	}

	wg.Wait()

	log.Debugf("SCHED windows: %+v", windows)
	log.Debugf("SCHED Acceptable win: %+v", acceptableWindows)

	// Step 2
	scheduled := 0
	rmQueue := make([]int, 0, queuneLen)

	for sqi := 0; sqi < queuneLen; sqi++ {
		task := (*sh.schedQueue)[sqi]
		needRes := ResourceTable[task.taskType][task.sector.ProofType]

		selectedWindow := -1
		for _, wnd := range acceptableWindows[task.indexHeap] {
			wid := sh.openWindows[wnd].worker
			wr := sh.workers[wid].info.Resources

			log.Debugf("SCHED try assign sqi:%d sector %d to window %d", sqi, task.sector.ID.Number, wnd)

			// TODO: allow bigger windows
			if !windows[wnd].allocated.canHandleRequest(needRes, wid, "schedAssign", wr) {
				continue
			}

			{
				//////////////////////////
				//自定义功能 begin,blueforest 2021.2.22
				//判断任务计数是否达到限制
				freecount := sh.getTaskFreeCount(wid, task.taskType)
				if freecount <= 0 {
					//log.Debugf("mydebug:任务数量达到上限:sector: %d,task_type:%v, worker:%v",
					//	task.sector.ID.Number, task.taskType, wid)
					continue
				}
				//自定义功能 end,blueforest
				//////////////////////////
			}

			log.Debugf("SCHED ASSIGNED sqi:%d sector %d task %s to window %d", sqi, task.sector.ID.Number, task.taskType, wnd)

			{
				//////////////////////////
				//自定义功能 begin,blueforest 2021.2.22
				//添加任务计数
				//log.Debugf("mydebug:增加任务计数:sector: %d,task_type:%v, worker:%v",
				//	task.sector.ID.Number, task.taskType, wid)
				sh.taskAddOne(wid, task.taskType)

				//添加sector和worker映射
				sh.setSectorToHostname(wid, task)

				//自定义功能 end,blueforest
				//////////////////////////
			}

			//更改资源分配
			windows[wnd].allocated.add(wr, needRes)
			// TODO: We probably want to re-sort acceptableWindows here based on new
			//  workerHandle.utilization + windows[wnd].allocated.utilization (workerHandle.utilization is used in all
			//  task selectors, but not in the same way, so need to figure out how to do that in a non-O(n^2 way), and
			//  without additional network roundtrips (O(n^2) could be avoided by turning acceptableWindows.[] into heaps))

			selectedWindow = wnd
			break
		}

		if selectedWindow < 0 {
			// all windows full
			continue
		}

		windows[selectedWindow].todo = append(windows[selectedWindow].todo, task)

		rmQueue = append(rmQueue, sqi)
		scheduled++
	}

	if len(rmQueue) > 0 {
		for i := len(rmQueue) - 1; i >= 0; i-- {
			sh.schedQueue.Remove(rmQueue[i])
		}
	}

	// Step 3

	if scheduled == 0 {
		return
	}

	scheduledWindows := map[int]struct{}{}
	for wnd, window := range windows {
		if len(window.todo) == 0 {
			// Nothing scheduled here, keep the window open
			continue
		}

		scheduledWindows[wnd] = struct{}{}

		window := window // copy
		select {
		case sh.openWindows[wnd].done <- &window:
		default:
			log.Error("expected sh.openWindows[wnd].done to be buffered")
		}
	}

	// Rewrite sh.openWindows array, removing scheduled windows
	newOpenWindows := make([]*schedWindowRequest, 0, windowsLen-len(scheduledWindows))
	for wnd, window := range sh.openWindows {
		if _, scheduled := scheduledWindows[wnd]; scheduled {
			// keep unscheduled windows open
			continue
		}

		newOpenWindows = append(newOpenWindows, window)
	}

	sh.openWindows = newOpenWindows
}

func (sh *scheduler) schedClose() {
	sh.workersLk.Lock()
	defer sh.workersLk.Unlock()
	log.Debugf("closing scheduler")

	for i, w := range sh.workers {
		sh.workerCleanup(i, w)
	}
}

func (sh *scheduler) Info(ctx context.Context) (interface{}, error) {
	ch := make(chan interface{}, 1)

	sh.info <- func(res interface{}) {
		ch <- res
	}

	select {
	case res := <-ch:
		return res, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (sh *scheduler) Close(ctx context.Context) error {
	close(sh.closing)
	select {
	case <-sh.closed:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

//==========================================================
//===== 自定义功能 begin,blueforest 2021.2.22 ==============
//==========================================================

//worker增加任务计数
func (sh *scheduler) taskAddOne(wid WorkerID, phaseTaskType sealtasks.TaskType) {
	//只对AP,PC1,TTFetch任务计数
	if phaseTaskType == sealtasks.TTAddPiece ||
		phaseTaskType == sealtasks.TTPreCommit1 ||
		phaseTaskType == sealtasks.TTFetch {
		if whl, ok := sh.workers[wid]; ok {
			whl.info.TaskResourcesLk.Lock()
			defer whl.info.TaskResourcesLk.Unlock()
			if counts, ok := whl.info.TaskResources[phaseTaskType]; ok {
				counts.RunCount++
				log.Debugf("mydebug:taskAddOne增加任务计数:task_type:%v,workerId:%v,limitcount:%v,runcount:%v",
					phaseTaskType, wid, counts.LimitCount, counts.RunCount)
			}
		}
	}
}

//worker扣减任务计数
func (sh *scheduler) taskReduceOne(wid WorkerID, phaseTaskType sealtasks.TaskType) {
	//只对AP,PC1,TTFetch任务计数
	if phaseTaskType == sealtasks.TTAddPiece ||
		phaseTaskType == sealtasks.TTPreCommit1 ||
		phaseTaskType == sealtasks.TTFetch {
		if whl, ok := sh.workers[wid]; ok {
			whl.info.TaskResourcesLk.Lock()
			defer whl.info.TaskResourcesLk.Unlock()
			if counts, ok := whl.info.TaskResources[phaseTaskType]; ok {
				counts.RunCount--
				log.Debugf("mydebug:taskAddOne扣减任务计数:task_type:%v,workerId:%v,limitcount:%v,runcount:%v",
					phaseTaskType, wid, counts.LimitCount, counts.RunCount)
			}
		}
	}
}

//worker获取任务计数
func (sh *scheduler) getTaskCount(wid WorkerID, phaseTaskType sealtasks.TaskType, typeCount string) int {
	//只对AP,PC1,TTFetch任务计数
	if phaseTaskType == sealtasks.TTAddPiece ||
		phaseTaskType == sealtasks.TTPreCommit1 ||
		phaseTaskType == sealtasks.TTFetch {
		if whl, ok := sh.workers[wid]; ok {
			if counts, ok := whl.info.TaskResources[phaseTaskType]; ok {
				whl.info.TaskResourcesLk.Lock()
				defer whl.info.TaskResourcesLk.Unlock()
				if typeCount == "limit" {
					return counts.LimitCount
				}
				if typeCount == "run" {
					return counts.RunCount
				}
			}
		}
		return 0
	}

	if typeCount == "limit" {
		//除AP,PC1,TTFetch外，其他任务类型都返回1
		return 1
	}
	if typeCount == "run" {
		//除AP,PC1,TTFetch外，其他任务类型都返回0
		return 0
	}
	return 1
}

//worker获取剩余任务数量
func (sh *scheduler) getTaskFreeCount(wid WorkerID, phaseTaskType sealtasks.TaskType) int {
	//只对AP和PC1任务计数,以及对miner的TTFetch计数
	if phaseTaskType == sealtasks.TTAddPiece ||
		phaseTaskType == sealtasks.TTPreCommit1 ||
		phaseTaskType == sealtasks.TTFetch {
		limitCount := sh.getTaskCount(wid, phaseTaskType, "limit") // json文件限制的任务数量
		runCount := sh.getTaskCount(wid, phaseTaskType, "run")     // 运行中的任务数量
		freeCount := limitCount - runCount

		//whl := sh.workers[wid]
		//log.Infof("mydebug:getTaskFreeCount:wid:%s,hostname:%v,take_type:%s,limit:%d,runCount:%d,free:%d",
		//	wid, whl.info.Hostname, phaseTaskType, limitCount, runCount, freeCount)

		if limitCount == 0 { // 0:禁止
			return 0
		}

		//AP要同时判断AP和PC1，避免有些worker分不到AP
		if phaseTaskType == sealtasks.TTAddPiece {
			// 同时判断PC1任务计数
			pc1limitCount := sh.getTaskCount(wid, sealtasks.TTPreCommit1, "limit")
			pc1RuncCount := sh.getTaskCount(wid, sealtasks.TTPreCommit1, "run")
			pc1FreeCount := pc1limitCount - pc1RuncCount

			if freeCount > 0 && pc1FreeCount > 0 { // 空闲数量不小于0，小于0也要校准为0
				return freeCount
			}
			return 0
		} else {
			if freeCount > 0 { // 空闲数量不小于0，小于0也要校准为0
				return freeCount
			}
			return 0
		}
	}
	//除AP,PC1,TTFetch外，其他任务类型都返回1
	return 1

	//if phaseTaskType == sealtasks.TTPreCommit2 || phaseTaskType == sealtasks.TTCommit1 {
	//	c2runCount := sh.getTaskCount(wid, sealtasks.TTCommit2, "run")
	//	if freeCount >= 0 && c2runCount <= 0 { // 需做的任务空闲数量不小于0，且没有c2任务在运行
	//		return freeCount
	//	}
	//	//log.Infof("mydebug:worker already doing C2 taskjob")
	//	return 0
	//}
	//
	//if phaseTaskType == sealtasks.TTCommit2 {
	//	p2runCount := sh.getTaskCount(wid, sealtasks.TTPreCommit2, "run")
	//	c1runCount := sh.getTaskCount(wid, sealtasks.TTCommit1, "run")
	//	if freeCount >= 0 && p2runCount <= 0 && c1runCount <= 0 { // 需做的任务空闲数量不小于0，且没有p2\c1任务在运行
	//		return freeCount
	//	}
	//	//log.Infof("mydebug:worker already doing P2C1 taskjob")
	//	return 0
	//}
	//
	//if phaseTaskType == sealtasks.TTFetch || phaseTaskType == sealtasks.TTFinalize ||
	//	phaseTaskType == sealtasks.TTUnseal || phaseTaskType == sealtasks.TTReadUnsealed { // 不限制
	//	return 1
	//}
	//
	//return 0
}

//设置sectorToHostname
func (sh *scheduler) setSectorToHostname(wid WorkerID, req *workerRequest) {

	log.Debugf("mydebug:setSectorToHostname:sector:%d,task_type:%v,wid:%v,hostname:%v",
		req.sector.ID.Number, req.taskType, wid, sh.workers[wid].info.Hostname)

	sh.sectorToHostnameLk.Lock()
	sh.sectorToHostname[req.sector.ID] = sh.workers[wid].info.Hostname
	sh.sectorToHostnameLk.Unlock()
}

//TTFinalize任务阶段，删除sectorToHostname里的sector
func (sh *scheduler) removeSectorToHostname(wid WorkerID, req *workerRequest) {
	//TTFinalize任务阶段，删除sectorToHostname里的sector
	if req.taskType == sealtasks.TTFinalize {
		log.Debugf("mydebug:removeSectorToHostname:sector:%d,task_type:%v,wid:%v,hostname:%v",
			req.sector.ID.Number, req.taskType, wid, sh.workers[wid].info.Hostname)

		secId := req.sector.ID
		sh.sectorToHostnameLk.Lock()
		delete(sh.sectorToHostname, secId)
		sh.sectorToHostnameLk.Unlock()
		//if _, ok := sh.sectorToHostname[secId]; ok {
		//	delete(sh.sectorToHostname, secId)
		//}
	}
}

//判断worker是否可以处理任务请求
func (sh *scheduler) canWorkerHandleRequest(wid WorkerID, whl *workerHandle, req *workerRequest) bool {
	//PC1,PC2的任务，确认当前的任务请求是否为worker的本机任务
	//只对PC1和PC2任务做hostname检查
	if req.taskType == sealtasks.TTPreCommit1 || req.taskType == sealtasks.TTPreCommit2 {
		//log.Debugf("mydebug:canWorkerHandleRequest:sector:%d,task_type:%v,wid:%v",
		//	req.sector.ID.Number, req.taskType, wid)

		sh.sectorToHostnameLk.RLock()
		v, ok := sh.sectorToHostname[req.sector.ID]
		sh.sectorToHostnameLk.RUnlock()

		if ok {
			if v == whl.info.Hostname {
				//sector和worker对应
				//log.Debugf("mydebug:canWorkerHandleRequest,匹配:sector:%d,task_type:%v,wid:%v,hostname:%v",
				//	req.sector.ID.Number, req.taskType, wid, v)
				return true
			} else {
				//log.Debugf("mydebug:canWorkerHandleRequest,不匹配:sector:%d,task_type:%v,wid:%v,need:%v,cur:%v",
				//	req.sector.ID.Number, req.taskType, wid, v, whl.info.Hostname)
				return false
			}
		} else {
			// 未找到记录，允许自由分配
			//log.Debugf("mydebug:canWorkerHandleRequest,未找到记录,允许运行:sector:%d,task_type:%v,wid:%v",
			//	req.sector.ID.Number, req.taskType, wid)
			return true
		}
	}

	return true
}

//==========================================================
//===== 自定义功能 end,blueforest 2021.2.22 ================
//==========================================================
