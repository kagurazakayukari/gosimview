// This file is part of GoSimView
// Copyright (C) 2026 KagurazakaYukari
// See LICENSE.md for license information.

package kn5conv

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
)

// PitLaneWindow 是维修区判定的降级方案：赛道内容有问题（缺 KN5、pit 网格不合规、
// 生成不出凸包）时改用它。做法是读 ai/pit_lane.ai 的折线，再加它在一圈 NSP 上覆盖的区间。
//
// 为什么不能只看 NSP 区间：维修区就在起终点线两侧（实测进站门 NSP≈0.99、出站门≈0.03），
// 而赛道上正常过线的车 NSP 同样是 0.99→0.01，只按区间判会让每圈过线的车集体误判进站。
// 所以必须再加“到维修区折线的横向距离”这一条，横向才是把维修道和主赛道分开的量。
type PitLaneWindow struct {
	Points [][2]float32 // 维修区折线（x, z）

	hasWindow bool
	lo, hi    float32 // 覆盖区间；wrap 为真时表示 [lo,1]∪[0,hi]
	wrap      bool
}

// PitLaneTubeM 车到维修区折线的横向容差（米）：自定义阈值，未按赛道逐个标定，
// 维修区车道宽度因赛道而异，暂统一取 12 米。
const PitLaneTubeM = 12.0

// LoadPitLaneWindow 读取 fast_lane.ai（提供每点的 NSP）与 pit_lane.ai（提供折线），
// 构建降级判据；任一文件缺失即返回错误，调用方保持“不判维修区”的现状。
func LoadPitLaneWindow(gamePath, trackName, configName string) (*PitLaneWindow, error) {
	_, layout, err := resolveTrackDirs(gamePath, trackName, configName)
	if err != nil {
		return nil, err
	}
	details, err := parseFastLaneAI(filepath.Join(layout, "ai", "fast_lane.ai"))
	if err != nil {
		return nil, err
	}
	pitPts, err := parsePitLaneAI(filepath.Join(layout, "ai", "pit_lane.ai"))
	if err != nil {
		return nil, err
	}
	if len(details) < 2 || len(pitPts) < 2 {
		return nil, fmt.Errorf("赛车线或维修区折线不足两个点")
	}
	for _, d := range details {
		if !finitePitValue(d.Position[0]) || !finitePitValue(d.Position[2]) || !finitePitValue(d.Node) || d.Node < 0 || d.Node > 1 {
			return nil, fmt.Errorf("赛车线坐标或 NSP 无效")
		}
	}
	w := &PitLaneWindow{Points: make([][2]float32, 0, len(pitPts))}
	nsps := make([]float32, 0, len(pitPts))
	for _, p := range pitPts {
		if !finitePitValue(p[0]) || !finitePitValue(p[2]) {
			return nil, fmt.Errorf("维修区折线含非有限坐标")
		}
		w.Points = append(w.Points, [2]float32{p[0], p[2]})
		nsps = append(nsps, nearestNode(details, p[0], p[2]))
	}
	w.setWindow(nsps)
	if w.lo == w.hi {
		return nil, fmt.Errorf("维修区 NSP 窗口退化，覆盖区间为空: %g", w.lo)
	}
	return w, nil
}

// resolveTrackDirs 解析赛道内容目录，Writer 的凸包路径与降级判据共用：
// 先 <game>/content/tracks/<track>/<config>，再游戏根目录同级的 content/tracks/...
// （原版日志里的 “main content folder”）。trackDir 供 models_<config>.ini 与 KN5 使用，
// layoutDir 供 ai/ 与 data/ 使用；只判断该目录存在，不要求 fast_lane.ai —— 缺它时
// 凸包路径仍要能选到带 models 的目录。
func resolveTrackDirs(gamePath, trackName, configName string) (string, string, error) {
	trackDirs := []string{
		filepath.Join(gamePath, "content", "tracks", trackName),
		filepath.Join(filepath.Dir(filepath.Clean(gamePath)), "content", "tracks", trackName),
	}
	for _, trackDir := range trackDirs {
		layoutDir := filepath.Join(trackDir, configName)
		if st, err := os.Stat(layoutDir); err == nil && st.IsDir() {
			return trackDir, layoutDir, nil
		}
	}
	return "", "", fmt.Errorf("找不到赛道内容目录: %s/%s", trackName, configName)
}

// nearestNode 取距离 (x,z) 最近的赛车线点的 NSP。
func nearestNode(details []TrackDetail, x, z float32) float32 {
	best, bestDist := float32(0), math.Inf(1)
	for _, d := range details {
		dx, dz := float64(d.Position[0])-float64(x), float64(d.Position[2])-float64(z)
		if dd := dx*dx + dz*dz; dd < bestDist {
			bestDist, best = dd, d.Node
		}
	}
	return best
}

// setWindow 用最大间隙法求维修区覆盖的 NSP 区间：最大间隙落在环绕处说明区间不跨线，
// 否则说明维修区跨了起终点线，区间要按 [hi..1]∪[0..lo] 处理。
func (w *PitLaneWindow) setWindow(nsps []float32) {
	if len(nsps) == 0 {
		return
	}
	sort.Slice(nsps, func(i, j int) bool { return nsps[i] < nsps[j] })
	n := len(nsps)
	bestGap, bestIdx := float32(-1), -1
	for i := 0; i+1 < n; i++ {
		if g := nsps[i+1] - nsps[i]; g > bestGap {
			bestGap, bestIdx = g, i
		}
	}
	wrapGap := nsps[0] + 1 - nsps[n-1]
	if bestIdx < 0 || wrapGap >= bestGap {
		w.lo, w.hi, w.wrap = nsps[0], nsps[n-1], false
	} else {
		w.lo, w.hi, w.wrap = nsps[bestIdx+1], nsps[bestIdx], true
	}
	w.hasWindow = true
}

// Contains 判定“车在维修区”：NSP 落在覆盖区间内，且到维修区折线的横向距离不超过容差。
func (w *PitLaneWindow) Contains(x, z, nsp float32) bool {
	if w == nil || !w.hasWindow || len(w.Points) < 2 || !finitePitValue(x) || !finitePitValue(z) || !finitePitValue(nsp) || nsp < 0 || nsp > 1 {
		return false
	}
	if w.hasWindow {
		if w.wrap {
			if nsp < w.lo && nsp > w.hi {
				return false
			}
		} else if nsp < w.lo || nsp > w.hi {
			return false
		}
	}
	// 到折线段的最短距离；仅测采样点会在稀疏长线段中部漏判。
	for i := 1; i < len(w.Points); i++ {
		a, b := w.Points[i-1], w.Points[i]
		ax, az := float64(a[0]), float64(a[1])
		dx, dz := float64(b[0])-ax, float64(b[1])-az
		along := float64(0)
		if length2 := dx*dx + dz*dz; length2 > 0 {
			along = ((float64(x)-ax)*dx + (float64(z)-az)*dz) / length2
			along = max(0, min(1, along))
		}
		xdiff, zdiff := float64(x)-(ax+along*dx), float64(z)-(az+along*dz)
		if xdiff*xdiff+zdiff*zdiff <= PitLaneTubeM*PitLaneTubeM {
			return true
		}
	}
	return false
}

func finitePitValue(v float32) bool {
	return !math.IsNaN(float64(v)) && !math.IsInf(float64(v), 0)
}
