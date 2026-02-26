// This file is part of GoSimView
// Copyright (C) 2026 KagurazakaYukari
//
// This program is dual-licensed under the GNU Affero General Public License v3.0
// and a commercial license. See LICENSE.md for details.

package kn5conv

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
)

// readPitBoundary reads pit boundary data from a PN file (each line contains x and z coordinates)
func readPitBoundary(filePath string) ([][3]float32, error) {
	// 读取文件内容
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("读取PN文件失败: %w", err)
	}

	// 按行解析数据
	lines := strings.Split(string(data), "\n")
	boundary := make([][3]float32, 0, len(lines))

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// 解析x和z坐标（忽略y，默认设为0.0）
		parts := strings.Fields(line)
		if len(parts) < 2 {
			return nil, fmt.Errorf("PN文件格式错误，行内容: %s", line)
		}

		x, err := strconv.ParseFloat(parts[0], 32)
		if err != nil {
			return nil, fmt.Errorf("解析X坐标失败: %w, 行内容: %s", err, line)
		}

		z, err := strconv.ParseFloat(parts[1], 32)
		if err != nil {
			return nil, fmt.Errorf("解析Z坐标失败: %w, 行内容: %s", err, line)
		}

		boundary = append(boundary, [3]float32{float32(x), 0.0, float32(z)})
	}

	return boundary, nil
}

// --------------------------- 核心数据结构定义 ---------------------------
// KN5Model 封装KN5文件的所有数据
type KN5Model struct {
	Textures  []string
	Materials []*KN5Material
	Nodes     []*KN5Node
}

// KN5Material 对应 Python 的 kn5Material 类
type KN5Material struct {
	Name               string
	Shader             string
	KSAmbient          float32
	KSDiffuse          float32
	KSSpecular         float32
	KSSpecularEXP      float32
	DiffuseMult        float32
	NormalMult         float32
	UseDetail          float32
	DetailUVMultiplier float32
	TxDiffuse          string
	TxNormal           string
	TxDetail           string
	TxDetailR          string
	TxDetailG          string
	TxDetailB          string
	TxDetailA          string
	TxDetailNM         string
	TxMask             string
	ShaderProps        string
	KSEmissive         float32
	KSAlphaRef         float32
}

// NewKN5Material 初始化 KN5Material
func NewKN5Material() *KN5Material {
	return &KN5Material{
		KSAmbient:          0.6,
		KSDiffuse:          0.6,
		KSSpecular:         0.9,
		KSSpecularEXP:      1.0,
		DiffuseMult:        1.0,
		NormalMult:         1.0,
		UseDetail:          0.0,
		DetailUVMultiplier: 1.0,
		KSEmissive:         0.0,
		KSAlphaRef:         1.0,
	}
}

// KN5Node 固定大小数值类型（避免binary.Read错误）
type KN5Node struct {
	Name        string
	Parent      int32         // 改为int32
	TMatrix     [4][4]float32 // 4x4 变换矩阵
	HMatrix     [4][4]float32 // 4x4 层次矩阵
	MeshIndex   int32         // 改为int32
	Type        int32         // 核心：从int改为int32
	MaterialID  int32         // 改为int32
	Translation [3]float32    // 平移
	Rotation    [3]float32    // 欧拉角
	Scaling     [3]float32    // 缩放
	VertexCount int32         // 改为int32
	Indices     []uint16      // 索引
	Position    []float32     // 顶点位置 (x,y,z)*n
	Normal      []float32     // 法线 (x,y,z)*n
	Texture0    []float32     // UV (u,v)*n
}

// NewKN5Node 初始化 KN5Node
func NewKN5Node() *KN5Node {
	return &KN5Node{
		Name:       "Default",
		Parent:     -1,
		TMatrix:    identity4x4(),
		HMatrix:    identity4x4(),
		MeshIndex:  -1,
		Type:       1,
		MaterialID: -1,
	}
}

// --------------------------- 基础工具函数 ---------------------------
// readString 读取指定长度的UTF-8字符串
func readString(r io.Reader, length int) (string, error) {
	buf := make([]byte, length)
	n, err := r.Read(buf)
	if err != nil {
		return "", fmt.Errorf("读取字符串失败: %w", err)
	}
	if n < length {
		return "", fmt.Errorf("读取字符串长度不足（期望%d，实际%d）", length, n)
	}
	return string(buf), nil
}

// identity4x4 返回4x4单位矩阵
func identity4x4() [4][4]float32 {
	return [4][4]float32{
		{1, 0, 0, 0},
		{0, 1, 0, 0},
		{0, 0, 1, 0},
		{0, 0, 0, 1},
	}
}

// matrixMult 4x4矩阵乘法（保留，HMatrix计算使用）
func matrixMult(ma, mb [4][4]float32) [4][4]float32 {
	var res [4][4]float32
	for i := 0; i < 4; i++ {
		for j := 0; j < 4; j++ {
			res[i][j] = ma[i][0]*mb[0][j] + ma[i][1]*mb[1][j] + ma[i][2]*mb[2][j] + ma[i][3]*mb[3][j]
		}
	}
	return res
}

// --------------------------- INI配置解析函数 ---------------------------
// parseModelsIni 解析models ini，提取所有[MODEL_X]的FILE字段（KN5文件名）
func parseModelsIni(iniPath string) ([]string, error) {
	// 验证INI文件存在
	if _, err := os.Stat(iniPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("models ini不存在: %s", iniPath)
	}

	// 打开INI文件
	file, err := os.Open(iniPath)
	if err != nil {
		return nil, fmt.Errorf("打开models ini失败: %w", err)
	}
	defer file.Close()

	// 正则匹配[MODEL_X]段和FILE=行
	modelSectionRegex := regexp.MustCompile(`^\[MODEL_\d+\]$`)
	fileLineRegex := regexp.MustCompile(`^FILE=(.+)$`)

	var (
		kn5Files   []string
		inModelSec bool // 是否在[MODEL_X]段内
	)

	// 逐行解析
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, ";") || strings.HasPrefix(line, "#") {
			continue // 跳过空行和注释
		}

		// 匹配[MODEL_X]段开头
		if modelSectionRegex.MatchString(line) {
			inModelSec = true
			continue
		}

		// 非MODEL段，重置标记
		if strings.HasPrefix(line, "[") {
			inModelSec = false
			continue
		}

		// 在MODEL段内，匹配FILE=行
		if inModelSec {
			if match := fileLineRegex.FindStringSubmatch(line); len(match) == 2 {
				kn5FileName := strings.TrimSpace(match[1])
				if strings.HasSuffix(strings.ToLower(kn5FileName), ".kn5") {
					kn5Files = append(kn5Files, kn5FileName)
					logrus.Infof("从INI解析到KN5文件: %s", kn5FileName)
				} else {
					logrus.Warnf("FILE字段不是KN5文件，跳过: %s", kn5FileName)
				}
			}
		}
	}

	// 检查扫描错误
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("读取models ini失败: %w", err)
	}

	// 去重（避免INI中重复配置）
	uniqueFiles := make([]string, 0)
	seen := make(map[string]bool)
	for _, f := range kn5Files {
		if !seen[f] {
			seen[f] = true
			uniqueFiles = append(uniqueFiles, f)
		}
	}

	if len(uniqueFiles) == 0 {
		return nil, fmt.Errorf("models ini中未找到有效的KN5文件配置")
	}

	return uniqueFiles, nil
}

// --------------------------- Pit边界生成核心函数 ---------------------------
// parsePitLaneAI 解析pit_lane.ai文件获取所有AI点
func parsePitLaneAI(filePath string) ([][3]float32, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("打开pit_lane.ai失败: %w", err)
	}
	defer file.Close()

	// 读取头部
	var header [4]int32
	if err := binary.Read(file, binary.LittleEndian, &header); err != nil {
		return nil, fmt.Errorf("读取头部失败: %w", err)
	}
	length := int(header[1])

	// 读取rawIdeal数据
	rawIdeal := make([][5]float32, length)
	for i := 0; i < length; i++ {
		var data [5]float32
		if err := binary.Read(file, binary.LittleEndian, &data); err != nil {
			return nil, fmt.Errorf("读取rawIdeal失败: %w", err)
		}
		rawIdeal[i] = data
	}

	// 读取extraCount
	var extraCount int32
	if err := binary.Read(file, binary.LittleEndian, &extraCount); err != nil {
		return nil, fmt.Errorf("读取extraCount失败: %w", err)
	}

	// 提取所有AI点
	aiPoints := make([][3]float32, length)
	for i := 0; i < length; i++ {
		aiPoints[i] = [3]float32{rawIdeal[i][0], rawIdeal[i][1], rawIdeal[i][2]}
	}

	return aiPoints, nil
}

// --------------------------- Pit边界生成核心函数 ---------------------------
// generatePitBoundary 生成Pit边界
func generatePitBoundary(model *KN5Model, aiPoints [][3]float32, cacheDir, kn5FileName, surfaceName, tbFileName string, generatePNOnly bool) ([][3]float32, bool, error) {

	// 1. 筛选pit相关节点（名称包含pit，不区分大小写）
	var pitVertices [][3]float32
	for _, node := range model.Nodes {
		if strings.Contains(strings.ToUpper(node.Name), surfaceName) && node.VertexCount > 0 {
			// 提取顶点数据（x,y,z）
			for i := 0; i < len(node.Position); i += 3 {
				if i+2 >= len(node.Position) {
					// break // 移除break，确保处理所有文件
				}
				x, y, z := node.Position[i], node.Position[i+1], node.Position[i+2]
				pitVertices = append(pitVertices, [3]float32{x, y, z})
			}
		}
	}

	if len(pitVertices) == 0 {
		logrus.Warnf("KN5文件%s中未找到pit相关节点或顶点数据，跳过", kn5FileName)
		return nil, false, nil // 跳过无PIT数据的文件，不终止批量处理
	}

	// 2. 计算2D凸包（XZ平面）
	boundary := computeConvexHull(pitVertices)

	// 3. 创建输出目录
	boundaryPath := filepath.Join(cacheDir, "tracks")
	if err := os.MkdirAll(boundaryPath, 0755); err != nil {
		return nil, false, fmt.Errorf("创建边界目录失败: %v", err)
	}

	// 4. 生成PN文件存储所有pit节点xz坐标
	pnFileName := "pit_node_" + tbFileName + ".pn"
	pnFilePath := filepath.Join(boundaryPath, pnFileName)
	fPN, err := os.Create(pnFilePath)
	if err != nil {
		return nil, false, fmt.Errorf("创建pit节点文件失败: %v", err)
	}
	defer fPN.Close()
	writerPN := bufio.NewWriter(fPN)
	defer writerPN.Flush()

	// 5. 写入所有pit节点的xz坐标
	for _, v := range pitVertices {
		if _, err := fmt.Fprintf(writerPN, "%.3f %.3f\n", v[0], v[2]); err != nil {
			return nil, false, fmt.Errorf("写入pit节点失败: %w", err)
		}
	}
	logrus.Infof("成功生成Pit节点文件: %s", pnFileName)
	if !generatePNOnly {
		// 6. 查找AI线与KN5边界的交点
		var intersections [][3]float32
		for i := 0; i < len(aiPoints)-1; i++ {
			for j := 0; j < len(boundary)-1; j++ {
				if intersect, ok := lineIntersection(aiPoints[i], aiPoints[i+1], boundary[j], boundary[j+1]); ok {
					intersections = append(intersections, intersect)
					if len(intersections) >= 2 {
						break
					}
				}
			}
			if len(intersections) >= 2 {
				break
			}
		}

		// 7. 处理交点并生成最终4个点
		if len(intersections) >= 2 {
			// 寻找每个交点最近的AI线段
			findNearestAISegment := func(point [3]float32) (p1, p2 [3]float32) {
				minDist := float32(math.MaxFloat32)
				for i := 0; i < len(aiPoints)-1; i++ {
					a := aiPoints[i]
					b := aiPoints[i+1]
					// 计算点到线段的距离
					dist := distancePointToSegment(point, a, b)
					if dist < minDist {
						minDist = dist
						p1, p2 = a, b
					}
				}
				return
			}

			// 获取每个交点对应的最近AI线段
			seg1P1, seg1P2 := findNearestAISegment(intersections[0])
			seg2P1, seg2P2 := findNearestAISegment(intersections[1])

			// 使用最近AI线段计算垂直扩展点
			expand1Left, expand1Right := expandPerpendicular(seg1P1, seg1P2, 12)
			expand2Left, expand2Right := expandPerpendicular(seg2P1, seg2P2, 12)
			finalPoints := [][3]float32{expand1Left, expand1Right, expand2Left, expand2Right}

			// 生成PB文件（文件名=KN5文件名.pb）
			pbFileName := "pit_boundary_" + tbFileName + ".pb"
			filePath := filepath.Join(boundaryPath, pbFileName)
			f, err := os.Create(filePath)
			if err != nil {
				return nil, false, fmt.Errorf("创建边界文件失败: %v", err)
			}
			defer f.Close()
			writer := bufio.NewWriter(f)
			defer writer.Flush()

			// 写入最终4个点
			for _, p := range finalPoints {
				if _, err := fmt.Fprintf(writer, "%.3f %.3f ", p[0], p[2]); err != nil {
					return nil, false, fmt.Errorf("写入扩展点失败: %w", err)
				}
			}
			logrus.Infof("成功生成4个扩展点并创建PB文件: %s", pbFileName)
		}
	} else {
		return nil, false, fmt.Errorf("未找到足够的交点，无法生成维修区边界")
	}
	return boundary, true, nil
}

// TrackDetail 赛道详细信息结构
type Section struct {
	IN   float64
	OUT  float64
	TEXT string
}

type TrackDetail struct {
	ID        int32
	Position  [3]float32
	Node      float32
	Distance  float32
	Direction float32
	WallLeft  [3]float32
	WallRight [3]float32
}

// parseFastLaneAI 解析fast_lane.ai文件获取完整赛道数据
// generateSVGFile 生成赛道SVG地图文件
func generateSVGFile(cacheDir, trackName, configName string, trackDetails []TrackDetail, aiPoints [][3]float32, sections []Section) error {
	svgDir := filepath.Join(cacheDir, "live-track-maps")
	if err := os.MkdirAll(svgDir, 0755); err != nil {
		logrus.Errorf("创建SVG目录失败: %v", err)
		return err
	}

	svgFileName := fmt.Sprintf("map_%s_%s.svg", trackName, configName)
	svgFilePath := filepath.Join(svgDir, svgFileName)

	if _, err := os.Stat(svgFilePath); err == nil {
		logrus.Infof("SVG文件已存在: %s", svgFilePath)
		return nil
	} else if os.IsNotExist(err) {
		if err := generateTrackSVG(svgFilePath, trackDetails, aiPoints, sections); err != nil {
			logrus.Errorf("生成SVG文件失败: %v", err)
			return err
		}
		logrus.Infof("成功生成SVG文件: %s", svgFilePath)
	} else {
		logrus.Errorf("检查SVG文件状态失败: %v", err)
		return err
	}
	return nil
}

func parseFastLaneAI(filePath string) ([]TrackDetail, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("打开fast_lane.ai失败: %w", err)
	}
	defer file.Close()

	var header [4]int32
	if err := binary.Read(file, binary.LittleEndian, &header); err != nil {
		return nil, fmt.Errorf("读取头部失败: %w", err)
	}
	pointCount := int(header[1])

	rawIdeal := make([][5]interface{}, pointCount)
	for i := 0; i < pointCount; i++ {
		var data [5]interface{}
		// 读取X, Y, Z坐标
		var x, y, z float32
		if err := binary.Read(file, binary.LittleEndian, &x); err != nil {
			return nil, fmt.Errorf("读取X坐标失败: %w", err)
		}
		if err := binary.Read(file, binary.LittleEndian, &y); err != nil {
			return nil, fmt.Errorf("读取Y坐标失败: %w", err)
		}
		if err := binary.Read(file, binary.LittleEndian, &z); err != nil {
			return nil, fmt.Errorf("读取Z坐标失败: %w", err)
		}
		data[0], data[1], data[2] = x, y, z

		// 读取距离和ID
		var distance float32
		var id int32
		if err := binary.Read(file, binary.LittleEndian, &distance); err != nil {
			return nil, fmt.Errorf("读取距离失败: %w", err)
		}
		if err := binary.Read(file, binary.LittleEndian, &id); err != nil {
			return nil, fmt.Errorf("读取ID失败: %w", err)
		}
		data[3], data[4] = distance, id
		rawIdeal[i] = data
	}

	// 读取额外细节数据
	extraCount := int(header[3])
	details := make([]TrackDetail, extraCount)
	for i := 0; i < extraCount; i++ {
		var rawDetail [18]float32
		for j := 0; j < 18; j++ {
			if err := binary.Read(file, binary.LittleEndian, &rawDetail[j]); err != nil {
				return nil, fmt.Errorf("读取细节数据失败: %w", err)
			}
		}

		prevRawIdeal := rawIdeal[i-1]
		if i == 0 {
			prevRawIdeal = rawIdeal[len(rawIdeal)-1]
		}

		// 计算方向
		prevPos := [3]float32{prevRawIdeal[0].(float32), prevRawIdeal[1].(float32), prevRawIdeal[2].(float32)}
		currPos := [3]float32{rawIdeal[i][0].(float32), rawIdeal[i][1].(float32), rawIdeal[i][2].(float32)}
		dx := currPos[0] - prevPos[0]
		dz := currPos[2] - prevPos[2]
		direction := -math.Atan2(float64(dz), float64(dx)) * 180 / math.Pi

		// 计算墙体位置
		wallLeftOffset := rawDetail[5]
		wallRightOffset := rawDetail[6]
		wallLeft := slideVec2(currPos, direction, wallLeftOffset, 90)
		wallRight := slideVec2(currPos, direction, wallRightOffset, -90)

		details[i] = TrackDetail{
			ID:        rawIdeal[i][4].(int32),
			Position:  currPos,
			Node:      rawIdeal[i][3].(float32) / float32(header[2]),
			Distance:  rawIdeal[i][3].(float32),
			Direction: float32(direction),
			WallLeft:  wallLeft,
			WallRight: wallRight,
		}
	}

	return details, nil
}

// slideVec2 计算偏移后的坐标
func slideVec2(pos [3]float32, angle float64, offset float32, angleOffset float64) [3]float32 {
	angleRad := (angle + angleOffset) * math.Pi / 180
	return [3]float32{
		pos[0] + float32(math.Cos(angleRad))*offset,
		pos[1],
		pos[2] - float32(math.Sin(angleRad))*offset,
	}
}

// generateTrackSVG 生成赛道SVG地图（包含完整赛道细节）
func parseSectionIni(filePath string) ([]Section, error) {
	sections := []Section{}
	currentSection := Section{}
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	inSection := false

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if len(line) == 0 {
			continue
		}

		// 检测section头部
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			if inSection {
				sections = append(sections, currentSection)
				currentSection = Section{}
			}
			inSection = true
			continue
		}

		if !inSection {
			continue
		}

		// 解析键值对
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		switch strings.ToUpper(key) {
		case "IN":
			currentSection.IN, _ = strconv.ParseFloat(value, 64)
		case "OUT":
			currentSection.OUT, _ = strconv.ParseFloat(value, 64)
		case "TEXT":
			currentSection.TEXT = value
		}
	}

	// 添加最后一个section
	if inSection {
		sections = append(sections, currentSection)
	}

	return sections, scanner.Err()
}

func generateTrackSVG(filePath string, trackDetails []TrackDetail, aiPoints [][3]float32, sections []Section) error {
	// 创建SVG文件
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("创建SVG文件失败: %w", err)
	}
	defer file.Close()

	// 写入SVG头部
	writer := bufio.NewWriter(file)
	defer writer.Flush()

	// 收集所有关键坐标点（车道、墙体、边界）
	if len(trackDetails) == 0 || len(aiPoints) == 0 {
		return fmt.Errorf("trackDetails或aiPoints为空，无法生成SVG")
	}
	allPoints := make([][3]float32, 0, len(trackDetails)*2+len(aiPoints))
	for _, detail := range trackDetails {
		allPoints = append(allPoints, detail.Position)
		allPoints = append(allPoints, detail.WallLeft)
		allPoints = append(allPoints, detail.WallRight)
	}
	allPoints = append(allPoints, aiPoints...)

	// 计算SVG视图范围
	minX, maxX, minZ, maxZ := float32(math.MaxFloat32), float32(-math.MaxFloat32), float32(math.MaxFloat32), float32(-math.MaxFloat32)
	for _, p := range allPoints {
		if p[0] < minX {
			minX = p[0]
		}
		if p[0] > maxX {
			maxX = p[0]
		}
		if p[2] < minZ {
			minZ = p[2]
		}
		if p[2] > maxZ {
			maxZ = p[2]
		}
	}

	// 添加边距
	margin := (maxX - minX) * 0.1
	width, height := maxX-minX+margin*2, maxZ-minZ+margin*2

	// SVG头部
	// 合并SVG根元素，添加必要属性
	// 计算动态缩放比例和偏移量
	maxWidth := 800.0  // 最大宽度
	maxHeight := 650.0 // 最大高度

	// 计算缩放比例（取宽度和高度缩放中的较小值）
	scaleX := maxWidth / float64(width)
	scaleY := maxHeight / float64(height)
	scale := math.Min(scaleX, scaleY)

	// 计算实际视图框尺寸
	viewBoxWidth := width * float32(scale)
	viewBoxHeight := height * float32(scale)

	// 计算偏移量（使地图居中）
	dataXOffset := minX - margin
	dataYOffset := minZ - margin

	fmt.Fprintf(writer, "<svg viewBox=\"0 0 %.2f %.2f\" data-map-version=\"1.4\" data-x-offset=\"%.2f\" data-scale=\"%.4f\" data-y-offset=\"%.2f\" style=\"max-width: %.0fpx; max-height: %.0fpx;\">\n", viewBoxWidth, viewBoxHeight, dataXOffset, scale, dataYOffset, maxWidth, maxHeight)

	// 生成pitlane polyline
	if len(aiPoints) > 0 {
		var pitlanePoints strings.Builder
		for _, p := range aiPoints {
			pitlanePoints.WriteString(fmt.Sprintf("%.2f,%.2f ", p[0], p[2]))
		}
		fmt.Fprintf(writer, "  <polyline id=\"pitlane\" points=\"%s\" style=\"fill:none; stroke:white; stroke-width:8\"/>", pitlanePoints.String())
	}

	// 添加侧道多边形 (遍历所有wallLeft和wallRight点)
	if len(trackDetails) > 0 {
		// 生成sidelane1 (所有wallLeft)
		var sidelane1Points strings.Builder
		for _, detail := range trackDetails {
			sidelane1Points.WriteString(fmt.Sprintf("%.2f,%.2f ", detail.WallLeft[0], detail.WallLeft[2]))
		}
		fmt.Fprintf(writer, "  <polygon id=\"sidelane1\" points=\"%s\" fill=\"rgba(255,255,255,0.1)\"/>", sidelane1Points.String())

		// 生成sidelane2 (所有wallRight)
		var sidelane2Points strings.Builder
		for _, detail := range trackDetails {
			sidelane2Points.WriteString(fmt.Sprintf("%.2f,%.2f ", detail.WallRight[0], detail.WallRight[2]))
		}
		fmt.Fprintf(writer, "  <polygon id=\"sidelane2\" points=\"%s\" fill=\"rgba(255,255,255,0.1)\"/>", sidelane2Points.String())
	}

	// 添加终点线
	if len(trackDetails) > 0 {
		firstNode := trackDetails[0]
		lastNode := trackDetails[len(trackDetails)-1]

		// 计算第一个节点左右墙中心点
		firstCenterX := (firstNode.WallLeft[0] + firstNode.WallRight[0]) / 2
		firstCenterZ := (firstNode.WallLeft[2] + firstNode.WallRight[2]) / 2

		// 计算最后一个节点左右墙中心点
		lastCenterX := (lastNode.WallLeft[0] + lastNode.WallRight[0]) / 2
		lastCenterZ := (lastNode.WallLeft[2] + lastNode.WallRight[2]) / 2

		// 计算finishPos为两个中心点的中点
		finishPosX := (firstCenterX + lastCenterX) / 2
		finishPosZ := (firstCenterZ + lastCenterZ) / 2
		finishPos := [3]float32{finishPosX, 0, finishPosZ}

		// 计算旋转角度使长边垂直于赛道方向
		scale := 0.5
		rotateAngle := lastNode.Direction - 90
		fmt.Fprintf(writer, "  <g id=\"finishline\" transform=\"translate(%.2f %.2f) rotate(%.2f %.2f %.2f) scale(%.2f)\">", float64(finishPos[0])-8*scale, float64(finishPos[2])-12*scale, rotateAngle, 8*scale, 12*scale, scale)
		fmt.Fprintf(writer, "    <rect x=\"0\" y=\"0\" width=\"%.0f\" height=\"%.0f\" style=\"fill:white\"/>", 8*scale, 8*scale)
		fmt.Fprintf(writer, "    <rect x=\"%.0f\" y=\"0\" width=\"%.0f\" height=\"%.0f\" style=\"fill:black\"/>", 8*scale, 8*scale, 8*scale)
		fmt.Fprintf(writer, "    <rect x=\"0\" y=\"%.0f\" width=\"%.0f\" height=\"%.0f\" style=\"fill:black\"/>", 8*scale, 8*scale, 8*scale)
		fmt.Fprintf(writer, "    <rect x=\"%.0f\" y=\"%.0f\" width=\"%.0f\" height=\"%.0f\" style=\"fill:white\"/>", 8*scale, 8*scale, 8*scale, 8*scale)
		fmt.Fprintf(writer, "    <rect x=\"0\" y=\"%.0f\" width=\"%.0f\" height=\"%.0f\" style=\"fill:white\"/>", 16*scale, 8*scale, 8*scale)
		fmt.Fprintf(writer, "    <rect x=\"%.0f\" y=\"%.0f\" width=\"%.0f\" height=\"%.0f\" style=\"fill:black\"/>", 8*scale, 16*scale, 8*scale, 8*scale)
		fmt.Fprintf(writer, "  </g>")

		// 添加赛道方向指示
		fmt.Fprintf(writer, "  <polygon id=\"trackdirection\" transform=\"translate(%.2f %.2f) rotate(%.2f %.2f %.2f) scale(%.2f)\" points=\"%.0f,%.0f %.0f,%.0f %.0f,%.0f\" fill=\"white\"/>", float64(finishPos[0])-8*scale, float64(finishPos[2])-12*scale, rotateAngle, 8*scale, 12*scale, scale, 48*scale, 0*scale, 80*scale, 8*scale, 48*scale, 16*scale)
	}

	for _, section := range sections {
		// 查找IN到OUT范围内的节点
		var sectionPoints strings.Builder
		startIdx := -1
		endIdx := -1

		for i, detail := range trackDetails {
			if float64(detail.Node) >= section.IN && startIdx == -1 {
				startIdx = i
			}
			if float64(detail.Node) >= section.OUT && endIdx == -1 {
				endIdx = i
				break
			}
		}

		if startIdx != -1 && endIdx != -1 {
			for i := startIdx; i <= endIdx; i++ {
				if i > startIdx {
					sectionPoints.WriteString(" ")
				}
				pos := trackDetails[i].Position
				sectionPoints.WriteString(fmt.Sprintf("%.2f,%.2f", pos[0], pos[2]))
			}

			// 添加section数据到SVG
			fmt.Fprintf(writer, "  <polyline class=\"section\" points=\"%s\" data-text=\"%s\"/>", sectionPoints.String(), section.TEXT)
		}
	}
	// SVG尾部
	fmt.Fprintln(writer, "</svg>")

	return nil
}

// computeConvexHull 简化的2D凸包计算（XZ平面，Graham扫描算法）
func computeConvexHull(points [][3]float32) [][3]float32 {
	if len(points) <= 3 {
		return points
	}

	// 转换为2D点（忽略Y坐标）
	var points2D []struct{ x, z float32 }
	for _, p := range points {
		points2D = append(points2D, struct{ x, z float32 }{p[0], p[2]})
	}

	// 按X升序、Z升序排序
	sort.Slice(points2D, func(i, j int) bool {
		return points2D[i].x < points2D[j].x || (points2D[i].x == points2D[j].x && points2D[i].z < points2D[j].z)
	})

	// 构建下凸包和上凸包
	var lower, upper []struct{ x, z float32 }
	for _, p := range points2D {
		for len(lower) >= 2 && cross(lower[len(lower)-2], lower[len(lower)-1], p) <= 0 {
			lower = lower[:len(lower)-1]
		}
		lower = append(lower, p)
	}

	for i := len(points2D) - 1; i >= 0; i-- {
		p := points2D[i]
		for len(upper) >= 2 && cross(upper[len(upper)-2], upper[len(upper)-1], p) <= 0 {
			upper = upper[:len(upper)-1]
		}
		upper = append(upper, p)
	}

	// 合并并去重
	full := append(lower[:len(lower)-1], upper[:len(upper)-1]...)
	unique := make(map[[2]float32]bool)
	var result [][3]float32
	for _, p := range full {
		key := [2]float32{p.x, p.z}
		if !unique[key] {
			unique[key] = true
			result = append(result, [3]float32{p.x, 0, p.z}) // Y坐标置0
		}
	}

	return result
}

// --------------------------- KN5文件读取函数 ---------------------------
// readKN5 读取单个KN5文件并封装为KN5Model
func readKN5(filePath string) (*KN5Model, error) {
	// 打开KN5文件（os.File实现了io.ReadSeeker）
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("打开KN5文件失败: %w", err)
	}
	defer file.Close()

	// 读取头部
	var header [10]byte
	if _, err := file.Read(header[:]); err != nil {
		return nil, fmt.Errorf("读取KN5头部失败: %w", err)
	}
	version := binary.LittleEndian.Uint32(header[6:10])

	// 版本>5跳过4字节
	if version > 5 {
		var skip4 [4]byte
		if _, err := file.Read(skip4[:]); err != nil {
			return nil, fmt.Errorf("跳过版本扩展字节失败: %w", err)
		}
	}

	// 读取纹理（仅读取名称，跳过数据）
	var texCount int32
	if err := binary.Read(file, binary.LittleEndian, &texCount); err != nil {
		return nil, fmt.Errorf("读取纹理数量失败: %w", err)
	}

	textures := make([]string, 0, texCount)
	for t := 0; t < int(texCount); t++ {
		var texType int32
		if err := binary.Read(file, binary.LittleEndian, &texType); err != nil {
			return nil, fmt.Errorf("读取纹理类型[%d]失败: %w", t, err)
		}

		var texNameLen int32
		if err := binary.Read(file, binary.LittleEndian, &texNameLen); err != nil {
			return nil, fmt.Errorf("读取纹理名称长度[%d]失败: %w", t, err)
		}

		texName, err := readString(file, int(texNameLen))
		if err != nil {
			return nil, fmt.Errorf("读取纹理名称[%d]失败: %w", t, err)
		}
		textures = append(textures, texName)

		var texSize int32
		if err := binary.Read(file, binary.LittleEndian, &texSize); err != nil {
			return nil, fmt.Errorf("读取纹理大小[%d]失败: %w", t, err)
		}

		// 跳过纹理数据（使用os.File的Seek方法）
		if _, err := file.Seek(int64(texSize), io.SeekCurrent); err != nil {
			return nil, fmt.Errorf("跳过纹理数据[%d]失败: %w", t, err)
		}
	}

	// 读取材质（仅读取基础信息，跳过属性）
	var matCount int32
	if err := binary.Read(file, binary.LittleEndian, &matCount); err != nil {
		return nil, fmt.Errorf("读取材质数量失败: %w", err)
	}

	materials := make([]*KN5Material, 0, matCount)
	for m := 0; m < int(matCount); m++ {
		newMat := NewKN5Material()

		var matNameLen int32
		if err := binary.Read(file, binary.LittleEndian, &matNameLen); err != nil {
			return nil, fmt.Errorf("读取材质名称长度[%d]失败: %w", m, err)
		}
		matName, err := readString(file, int(matNameLen))
		if err != nil {
			return nil, fmt.Errorf("读取材质名称[%d]失败: %w", m, err)
		}
		newMat.Name = matName

		var shaderNameLen int32
		if err := binary.Read(file, binary.LittleEndian, &shaderNameLen); err != nil {
			return nil, fmt.Errorf("读取着色器名称长度[%d]失败: %w", m, err)
		}
		shaderName, err := readString(file, int(shaderNameLen))
		if err != nil {
			return nil, fmt.Errorf("读取着色器名称[%d]失败: %w", m, err)
		}
		newMat.Shader = shaderName

		// 跳过ashort（int16）
		var ashort int16
		if err := binary.Read(file, binary.LittleEndian, &ashort); err != nil {
			return nil, fmt.Errorf("读取材质短整数[%d]失败: %w", m, err)
		}

		// 版本>4跳过azero（int32）
		if version > 4 {
			var azero int32
			if err := binary.Read(file, binary.LittleEndian, &azero); err != nil {
				return nil, fmt.Errorf("读取版本扩展字段[%d]失败: %w", m, err)
			}
		}

		// 跳过材质属性
		var propCount int32
		if err := binary.Read(file, binary.LittleEndian, &propCount); err != nil {
			return nil, fmt.Errorf("读取材质属性数量[%d]失败: %w", m, err)
		}
		for p := 0; p < int(propCount); p++ {
			// 跳过属性名称
			var propNameLen int32
			if err := binary.Read(file, binary.LittleEndian, &propNameLen); err != nil {
				return nil, fmt.Errorf("读取属性名称长度[%d][%d]失败: %w", m, p, err)
			}
			if _, err := file.Seek(int64(propNameLen), io.SeekCurrent); err != nil {
				return nil, fmt.Errorf("跳过属性名称[%d][%d]失败: %w", m, p, err)
			}

			// 跳过属性值（float32）
			if _, err := file.Seek(4, io.SeekCurrent); err != nil {
				return nil, fmt.Errorf("跳过属性值[%d][%d]失败: %w", m, p, err)
			}

			// 跳过36字节扩展
			if _, err := file.Seek(36, io.SeekCurrent); err != nil {
				return nil, fmt.Errorf("跳过属性扩展[%d][%d]失败: %w", m, p, err)
			}
		}

		// 跳过纹理采样器
		var texSamplerCount int32
		if err := binary.Read(file, binary.LittleEndian, &texSamplerCount); err != nil {
			return nil, fmt.Errorf("读取采样器数量[%d]失败: %w", m, err)
		}
		for t := 0; t < int(texSamplerCount); t++ {
			// 跳过采样器名称
			var sampleNameLen int32
			if err := binary.Read(file, binary.LittleEndian, &sampleNameLen); err != nil {
				return nil, fmt.Errorf("读取采样器名称长度[%d][%d]失败: %w", m, t, err)
			}
			if _, err := file.Seek(int64(sampleNameLen), io.SeekCurrent); err != nil {
				return nil, fmt.Errorf("跳过采样器名称[%d][%d]失败: %w", m, t, err)
			}

			// 跳过采样器槽位（int32）
			if _, err := file.Seek(4, io.SeekCurrent); err != nil {
				return nil, fmt.Errorf("跳过采样器槽位[%d][%d]失败: %w", m, t, err)
			}

			// 跳过纹理名称
			var texNameLen int32
			if err := binary.Read(file, binary.LittleEndian, &texNameLen); err != nil {
				return nil, fmt.Errorf("读取纹理名称长度[%d][%d]失败: %w", m, t, err)
			}
			if _, err := file.Seek(int64(texNameLen), io.SeekCurrent); err != nil {
				return nil, fmt.Errorf("跳过纹理名称[%d][%d]失败: %w", m, t, err)
			}
		}
		materials = append(materials, newMat)
	}

	// 读取节点（核心：仅保留顶点位置数据）
	nodes := make([]*KN5Node, 0)
	nodes, err = readNodes(file, nodes, -1)
	if err != nil {
		return nil, fmt.Errorf("读取节点失败: %w", err)
	}

	// 封装为KN5Model
	return &KN5Model{
		Textures:  textures,
		Materials: materials,
		Nodes:     nodes,
	}, nil
}

// readNodes 递归读取节点（参数改为io.ReadSeeker，支持Seek）
func readNodes(r io.ReadSeeker, nodeList []*KN5Node, parentID int32) ([]*KN5Node, error) {
	newNode := NewKN5Node()
	newNode.Parent = parentID

	// 读取节点类型（核心修复：Type是int32）
	if err := binary.Read(r, binary.LittleEndian, &newNode.Type); err != nil {
		return nil, fmt.Errorf("读取节点类型失败: %w", err)
	}

	// 读取节点名称长度（int32）
	var nameLen int32
	if err := binary.Read(r, binary.LittleEndian, &nameLen); err != nil {
		return nil, fmt.Errorf("读取节点名称长度失败: %w", err)
	}
	name, err := readString(r, int(nameLen))
	if err != nil {
		return nil, fmt.Errorf("读取节点名称失败: %w", err)
	}
	newNode.Name = name

	// 读取子节点数量（int32）
	var childrenCount int32
	if err := binary.Read(r, binary.LittleEndian, &childrenCount); err != nil {
		return nil, fmt.Errorf("读取子节点数量失败: %w", err)
	}

	// 跳过abyte（1字节）
	var abyte [1]byte
	if _, err := r.Read(abyte[:]); err != nil {
		return nil, fmt.Errorf("读取abyte失败: %w", err)
	}

	switch newNode.Type {
	case 1: // dummy节点：读取矩阵，跳过其他
		for i := 0; i < 4; i++ {
			for j := 0; j < 4; j++ {
				if err := binary.Read(r, binary.LittleEndian, &newNode.TMatrix[i][j]); err != nil {
					return nil, fmt.Errorf("读取dummy矩阵[%d][%d]失败: %w", i, j, err)
				}
			}
		}

	case 2, 3: // mesh/animated mesh节点：核心读取顶点位置
		// 跳过bcd字节（3字节）
		var bcd [3]byte
		if _, err := r.Read(bcd[:]); err != nil {
			return nil, fmt.Errorf("读取bcd字节失败: %w", err)
		}

		// 读取顶点数量（int32）
		if err := binary.Read(r, binary.LittleEndian, &newNode.VertexCount); err != nil {
			return nil, fmt.Errorf("读取顶点数量失败: %w", err)
		}

		// 读取顶点位置（核心数据）
		newNode.Position = make([]float32, 0, newNode.VertexCount*3)
		for v := 0; v < int(newNode.VertexCount); v++ {
			var pos [3]float32
			if err := binary.Read(r, binary.LittleEndian, &pos); err != nil {
				return nil, fmt.Errorf("读取顶点位置[%d]失败: %w", v, err)
			}
			newNode.Position = append(newNode.Position, pos[:]...)

			// 跳过法线、UV、切线等非核心数据
			skipLen := int64(12 + 8 + 12) // 法线12 + UV8 + 切线12
			if newNode.Type == 3 {
				skipLen = 12 + 8 + 44 // animated mesh跳过更多（切线+权重44）
			}
			if _, err := r.Seek(skipLen, io.SeekCurrent); err != nil {
				return nil, fmt.Errorf("跳过顶点非核心数据[%d]失败: %w", v, err)
			}
		}

		// 跳过索引、材质ID等
		var indexCount int32
		if err := binary.Read(r, binary.LittleEndian, &indexCount); err != nil {
			return nil, fmt.Errorf("读取索引数量失败: %w", err)
		}
		skipLen := int64(indexCount*2) + 4 // 索引（uint16*N） + 材质ID（int32）
		if newNode.Type == 2 {
			skipLen += 29 // mesh额外跳过29字节
		} else {
			skipLen += 12 // animated mesh额外跳过12字节
		}
		if _, err := r.Seek(skipLen, io.SeekCurrent); err != nil {
			return nil, fmt.Errorf("跳过索引/材质ID失败: %w", err)
		}

		// animated mesh额外跳过骨骼数据
		if newNode.Type == 3 {
			var boneCount int32
			if err := binary.Read(r, binary.LittleEndian, &boneCount); err != nil {
				return nil, fmt.Errorf("读取骨骼数量失败: %w", err)
			}
			for b := 0; b < int(boneCount); b++ {
				var boneNameLen int32
				if err := binary.Read(r, binary.LittleEndian, &boneNameLen); err != nil {
					return nil, fmt.Errorf("读取骨骼名称长度[%d]失败: %w", b, err)
				}
				if _, err := r.Seek(int64(boneNameLen)+64, io.SeekCurrent); err != nil { // 骨骼名称 + 64字节矩阵
					return nil, fmt.Errorf("跳过骨骼数据[%d]失败: %w", b, err)
				}
			}
		}
	}

	// 计算层次矩阵（保留，不影响功能）
	if parentID < 0 {
		newNode.HMatrix = newNode.TMatrix
	} else {
		newNode.HMatrix = matrixMult(newNode.TMatrix, nodeList[parentID].HMatrix)
	}

	nodeList = append(nodeList, newNode)
	currentID := int32(len(nodeList) - 1)

	// 递归读取子节点
	for c := 0; c < int(childrenCount); c++ {
		var err error
		nodeList, err = readNodes(r, nodeList, currentID)
		if err != nil {
			return nil, fmt.Errorf("读取子节点[%d]失败: %w", c, err)
		}
	}

	return nodeList, nil
}

// --------------------------- 批量处理函数 ---------------------------
// distance2D 计算两点在XZ平面上的欧几里得距离
func distance2D(a, b [3]float32) float32 {
	dx := a[0] - b[0]
	dz := a[2] - b[2]
	return float32(math.Sqrt(float64(dx*dx + dz*dz)))
}

// --------------------------- 批量处理函数 ---------------------------
// BatchProcess 批量处理指定目录下的models_gp.ini中的KN5文件
func BatchProcess(gameDir, cacheDir, trackName, configName string) ([][3]float32, error) {

	// 1. 拼接models_gp.ini路径
	iniPath := filepath.Join(gameDir, "content", "tracks", trackName, "models_"+configName+".ini")
	surfacePath := filepath.Join(gameDir, "content", "tracks", trackName, configName, "data", "surfaces.ini")
	sectionPath := filepath.Join(gameDir, "content", "tracks", trackName, configName, "data", "sections.ini")
	// 2. 解析INI文件，提取KN5文件列表
	logrus.Infof("开始解析配置文件: %s", iniPath)
	kn5FileNames, err := parseModelsIni(iniPath)
	if err != nil {
		return nil, fmt.Errorf("解析models ini失败: %w", err)
	}

	logrus.Infof("开始解析配置文件: %s", surfacePath)
	surfaceName, err := parseSurfaceIni(surfacePath)
	if err != nil {
		return nil, fmt.Errorf("解析surfaces ini失败: %w", err)
	}

	logrus.Infof("开始解析配置文件: %s", sectionPath)
	sections, err := parseSectionIni(sectionPath)
	if err != nil {
		return nil, fmt.Errorf("解析sections ini失败: %w", err)
	}

	// 解析fast_lane.ai获取赛道详细信息
	fastLanePath := filepath.Join(gameDir, "content", "tracks", trackName, configName, "ai", "fast_lane.ai")
	trackDetails, err := parseFastLaneAI(fastLanePath)
	if err != nil {
		return nil, fmt.Errorf("解析fast_lane.ai失败: %w", err)
	}

	// 解析pit_lane.ai获取AI路径点
	pitLanePath := filepath.Join(gameDir, "content", "tracks", trackName, configName, "ai", "pit_lane.ai")
	aiPoints, err := parsePitLaneAI(pitLanePath)
	if err != nil {
		return nil, fmt.Errorf("解析pit_lane.ai失败: %w", err)
	}

	var boundary [][3]float32
	// 检查PB和PN文件是否存在
	tbFileName := trackName + "_" + configName
	pbFileName := "pit_boundary_" + tbFileName + ".pb"
	pbFilePath := filepath.Join(cacheDir, "tracks", pbFileName)
	pnFileName := "pit_node_" + tbFileName + ".pn"
	pnFilePath := filepath.Join(cacheDir, "tracks", pnFileName)

	pbExists := false
	pnExists := false

	_, err = os.Stat(pbFilePath)
	if err == nil {
		pbExists = true
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("检查PB文件时发生错误: %v", err)
	}

	_, err = os.Stat(pnFilePath)
	if err == nil {
		pnExists = true
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("检查PN文件时发生错误: %v", err)
	}

	// 根据存在情况决定是否处理
	if pnExists {
		logrus.Infof("PN文件已存在，读取边界数据: %s", tbFileName)
		// 读取PN文件获取边界数据
		boundary, err = readPitBoundary(pnFilePath)
		if err != nil {
			return nil, fmt.Errorf("读取PN文件失败: %w", err)
		}
	}

	generatePNOnly := pbExists && !pnExists

	// 3. 遍历每个KN5文件处理
	successCount := 0
	failCount := 0
	for _, kn5FileName := range kn5FileNames {
		// 拼接KN5文件完整路径
		kn5FilePath := filepath.Join(gameDir, "content", "tracks", trackName, kn5FileName)
		logrus.Infof("开始处理KN5文件: %s", kn5FilePath)

		// 验证KN5文件存在
		if _, err := os.Stat(kn5FilePath); os.IsNotExist(err) {
			logrus.Errorf("KN5文件不存在，跳过: %s", kn5FilePath)
			failCount++
			continue
		}

		// 读取KN5文件
		model, err := readKN5(kn5FilePath)
		if err != nil {
			logrus.Errorf("读取KN5文件失败，跳过: %s, 错误: %v", kn5FilePath, err)
			failCount++
			continue
		}

		// 生成Pit边界
		var success bool = false
		boundary, success, err = generatePitBoundary(model, aiPoints, cacheDir, kn5FileName, surfaceName, tbFileName, generatePNOnly)

		if err != nil {
			logrus.Errorf("生成Pit边界失败，跳过: %s, 错误: %v", kn5FilePath, err)
			continue
		}
		if !success {
			continue
		}
		successCount++
		logrus.Infof("KN5文件%s处理完成", kn5FileName)

		// 生成SVG文件
		if err := generateSVGFile(cacheDir, trackName, configName, trackDetails, aiPoints, sections); err != nil {
			logrus.Errorf("生成SVG文件失败: %v", err)
		}
		break
	}
	return boundary, nil
}

// --------------------------- 命令行解析 ---------------------------
// parseSurfaceIni 解析surfaces.ini文件，提取IS_PITLANE=1的SURFACE KEY
func parseSurfaceIni(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("无法打开surfaces.ini文件: %w", err)
	}
	defer file.Close()

	var currentSection string
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		// 跳过空行和注释
		if line == "" || strings.HasPrefix(line, ";") {
			continue
		}

		// 检测INI section头
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			currentSection = strings.Trim(line, "[]")
			continue
		}

		// 在当前section中查找IS_PITLANE=1
		if currentSection != "" && strings.Contains(strings.ToUpper(line), "IS_PITLANE=1") {
			// 重置扫描器，重新扫描当前section查找KEY
			_, err := file.Seek(0, 0)
			if err != nil {
				return "", fmt.Errorf("文件指针重置失败: %w", err)
			}
			scanner = bufio.NewScanner(file)
			foundSection := false

			for scanner.Scan() {
				sectionLine := strings.TrimSpace(scanner.Text())
				if strings.HasPrefix(sectionLine, "["+currentSection+"]") {
					foundSection = true
					continue
				}

				if foundSection && strings.HasPrefix(sectionLine, "KEY=") {
					keyValue := strings.TrimPrefix(sectionLine, "KEY=")
					if keyValue != "" {
						return keyValue, nil
					}
					return "", fmt.Errorf("在section %s中找到IS_PITLANE=1，但未找到有效的KEY", currentSection)
				}

				// 如果进入下一个section，停止查找
				if foundSection && strings.HasPrefix(sectionLine, "[") {
					break
				}
			}

			return "", fmt.Errorf("在section %s中找到IS_PITLANE=1，但未找到KEY", currentSection)
		}
	}

	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("文件扫描错误: %w", err)
	}

	return "", fmt.Errorf("未找到IS_PITLANE=1的section")
}

// 计算两条线段的交点
func lineIntersection(p1, p2, p3, p4 [3]float32) ([3]float32, bool) {
	x1, z1 := p1[0], p1[2]
	x2, z2 := p2[0], p2[2]
	x3, z3 := p3[0], p3[2]
	x4, z4 := p4[0], p4[2]

	denom := (x1-x2)*(z3-z4) - (z1-z2)*(x3-x4)
	if denom == 0 {
		return [3]float32{}, false
	}
	tNum := (x1-x3)*(z3-z4) - (z1-z3)*(x3-x4)
	uNum := -((x1-x2)*(z1-z3) - (z1-z2)*(x1-x3))
	t := tNum / denom
	u := uNum / denom

	if t >= 0 && t <= 1 && u >= 0 && u <= 1 {
		x := x1 + t*(x2-x1)
		z := z1 + t*(z2-z1)
		return [3]float32{x, 0, z}, true
	}
	return [3]float32{}, false
}

// 计算垂直方向向量并扩展点
func expandPerpendicular(p1, p2 [3]float32, distance float32) ([3]float32, [3]float32) {
	dirX := p2[0] - p1[0]
	dirZ := p2[2] - p1[2]
	length := float32(math.Sqrt(float64(dirX*dirX + dirZ*dirZ)))
	if length == 0 {
		return p1, p2
	}

	perpX := -dirZ / length
	perpZ := dirX / length

	left := [3]float32{p1[0] + perpX*distance, 0, p1[2] + perpZ*distance}
	right := [3]float32{p1[0] - perpX*distance, 0, p1[2] - perpZ*distance}
	return left, right
}

// ... 计算叉积（判断点的转向）
func cross(o, a, b struct{ x, z float32 }) float32 {
	return (a.x-o.x)*(b.z-o.z) - (a.z-o.z)*(b.x-o.x)
}

// distancePointToSegment calculates the shortest distance from point p to the line segment ab
func distancePointToSegment(p, a, b [3]float32) float32 {
	// Vector AB
	ab := [3]float32{b[0] - a[0], b[1] - a[1], b[2] - a[2]}
	// Vector AP
	ap := [3]float32{p[0] - a[0], p[1] - a[1], p[2] - a[2]}

	// Dot product of AP and AB
	dot := ap[0]*ab[0] + ap[1]*ab[1] + ap[2]*ab[2]
	if dot <= 0 {
		// Closest point is A
		return distance2D(p, a)
	}

	// Squared length of AB
	abLenSq := ab[0]*ab[0] + ab[1]*ab[1] + ab[2]*ab[2]
	if dot >= abLenSq {
		// Closest point is B
		return distance2D(p, b)
	}

	// Closest point is projection on the segment
	t := dot / abLenSq
	closest := [3]float32{
		a[0] + t*ab[0],
		a[1] + t*ab[1],
		a[2] + t*ab[2],
	}
	return distance2D(p, closest)
}
