# UDP 数据包格式说明

本文档描述了 Writer 模块通过连接服务器能够获得的 UDP 数据包格式以及内部字段。

## 1. 事件类型 (Event)

UDP 消息通过事件类型进行区分，主要分为接收事件和发送事件：

### 1.1 接收事件
| 事件类型 | 值 | 描述 |
|---------|-----|------|
| EventCollisionWithCar | 10 | 车辆碰撞事件 |
| EventCollisionWithEnv | 11 | 环境碰撞事件 |
| EventNewSession | 50 | 新会话事件 |
| EventNewConnection | 51 | 新连接事件 |
| EventConnectionClosed | 52 | 连接关闭事件 |
| EventCarUpdate | 53 | 车辆更新事件 |
| EventCarInfo | 54 | 车辆信息事件 |
| EventEndSession | 55 | 会话结束事件 |
| EventVersion | 56 | 版本信息事件 |
| EventChat | 57 | 聊天消息事件 |
| EventClientLoaded | 58 | 客户端加载完成事件 |
| EventSessionInfo | 59 | 会话信息事件 |
| EventError | 60 | 错误事件 |
| EventLapCompleted | 73 | 圈速完成事件 |
| EventClientEvent | 130 | 客户端事件 |

### 1.2 发送事件
| 事件类型 | 值 | 描述 |
|---------|-----|------|
| EventRealtimeposInterval | 200 | 实时位置更新间隔 |
| EventGetCarInfo | 201 | 获取车辆信息 |
| EventSendChat | 202 | 发送聊天消息 |
| EventBroadcastChat | 203 | 广播聊天消息 |
| EventGetSessionInfo | 204 | 获取会话信息 |
| EventSetSessionInfo | 205 | 设置会话信息 |
| EventKickUser | 206 | 踢用户 |
| EventNextSession | 207 | 下一会话 |
| EventRestartSession | 208 | 重启会话 |
| EventAdminCommand | 209 | 管理命令 |

## 2. 数据结构定义

### 2.1 基础数据类型
- `CarID`: uint8 - 车辆ID
- `DriverGUID`: string - 驾驶员GUID
- `SessionType`: uint8 - 会话类型（0: Booking, 1: Practice, 2: Qualifying, 3: Race）

### 2.2 向量结构体
```go
type Vec struct {
    X float32 `json:"X"` // X坐标
    Y float32 `json:"Y"` // Y坐标
    Z float32 `json:"Z"` // Z坐标
}
```

## 3. 主要消息结构体

### 3.1 LapCompleted (圈速完成数据)
**事件类型**: EventLapCompleted (73)
```go
type LapCompleted struct {
    CarID     CarID  `json:"CarID"`     // 车辆ID
    LapTime   uint32 `json:"LapTime"`   // 圈速（毫秒）
    Cuts      uint8  `json:"Cuts"`      // 切弯次数
    CarsCount uint8  `json:"CarsCount"` // 车辆数量
    Cars      []*LapCompletedCar `json:"Cars"` // 所有车辆圈速信息
}

type LapCompletedCar struct {
    CarID     CarID  `json:"CarID"`     // 车辆ID
    LapTime   uint32 `json:"LapTime"`   // 圈速（毫秒）
    Laps      uint16 `json:"Laps"`      // 已完成圈数
    Completed uint8  `json:"Completed"` // 完成状态
}
```

### 3.2 CollisionWithCar (车辆碰撞数据)
**事件类型**: EventCollisionWithCar (10)
```go
type CollisionWithCar struct {
    CarID       CarID   `json:"CarID"`       // 车辆ID
    OtherCarID  CarID   `json:"OtherCarID"`  // 另一辆车ID
    ImpactSpeed float32 `json:"ImpactSpeed"` // 碰撞速度
    WorldPos    Vec     `json:"WorldPos"`    // 世界坐标位置
    RelPos      Vec     `json:"RelPos"`      // 相对坐标位置
}
```

### 3.3 CollisionWithEnvironment (环境碰撞数据)
**事件类型**: EventCollisionWithEnv (11)
```go
type CollisionWithEnvironment struct {
    CarID       CarID   `json:"CarID"`       // 车辆ID
    ImpactSpeed float32 `json:"ImpactSpeed"` // 碰撞速度
    WorldPos    Vec     `json:"WorldPos"`    // 世界坐标位置
    RelPos      Vec     `json:"RelPos"`      // 相对坐标位置
}
```

### 3.4 SessionCarInfo (会话车辆信息)
**事件类型**: 动态设置
```go
type SessionCarInfo struct {
    CarID      CarID      `json:"CarID"`      // 车辆ID
    DriverName string     `json:"DriverName"` // 驾驶员名称
    DriverGUID DriverGUID `json:"DriverGUID"` // 驾驶员GUID
    CarModel   string     `json:"CarModel"`   // 车辆模型
    CarSkin    string     `json:"CarSkin"`    // 车辆涂装
    DriverInitials string `json:"DriverInitials"` // 驾驶员缩写
    CarName        string `json:"CarName"`        // 车辆名称
    EventType      Event  `json:"EventType"`      // 事件类型
}
```

### 3.5 Chat (聊天消息)
**事件类型**: EventChat (57)
```go
type Chat struct {
    CarID      CarID      `json:"CarID"`      // 发送者车辆ID
    Message    string     `json:"Message"`    // 聊天内容
    DriverGUID DriverGUID `json:"DriverGUID"` // 发送者GUID
    DriverName string     `json:"DriverName"` // 发送者名称
    Time       time.Time  `json:"Time"`       // 发送时间
}
```

### 3.6 CarInfo (车辆信息)
**事件类型**: EventCarInfo (54)
```go
type CarInfo struct {
    CarID       CarID      `json:"CarID"`       // 车辆ID
    IsConnected bool       `json:"IsConnected"` // 连接状态
    CarModel    string     `json:"CarModel"`    // 车辆模型
    CarSkin     string     `json:"CarSkin"`     // 车辆涂装
    DriverName  string     `json:"DriverName"`  // 驾驶员名称
    DriverTeam  string     `json:"DriverTeam"`  // 驾驶员团队
    DriverGUID  DriverGUID `json:"DriverGUID"`  // 驾驶员GUID
}
```

### 3.7 ServerError (服务器错误)
**事件类型**: EventError (60)
```go
type ServerError struct {
    error // 包含错误信息
}
```

### 3.8 ClientEvent (客户端事件)
**事件类型**: EventClientEvent (130)
```go
// ClientEvent 结构体定义在消息处理逻辑中，用于处理客户端事件
// 具体字段取决于事件内容
```

### 3.9 CarUpdate (车辆更新数据 - 遥测数据)
**事件类型**: EventCarUpdate (53)
```go
type CarUpdate struct {
    CarID               CarID   `json:"CarID"`               // 车辆ID
    Pos                 Vec     `json:"Pos"`                 // 位置坐标
    Velocity            Vec     `json:"Velocity"`            // 速度向量
    Gear                uint8   `json:"Gear"`                // 当前档位（0: 空挡, 1-6: 前进档, -1: 倒挡）
    EngineRPM           uint16  `json:"EngineRPM"`           // 发动机转速
    NormalisedSplinePos float32 `json:"NormalisedSplinePos"` // 标准化样条位置（0.0-1.0，表示在赛道上的位置）
}
```

### 3.10 EndSession (会话结束)
**事件类型**: EventEndSession (55)
```go
type EndSession string // 会话结束信息
```

### 3.11 Version (版本信息)
**事件类型**: EventVersion (56)
```go
type Version uint8 // 版本号
```

### 3.12 ClientLoaded (客户端加载完成)
**事件类型**: EventClientLoaded (58)
```go
type ClientLoaded CarID // 已加载的客户端车辆ID
```

### 3.13 SessionInfo (会话信息)
**事件类型**: EventSessionInfo (59)
```go
type SessionInfo struct {
    Version             uint8       `json:"Version"`             // 版本号
    SessionIndex        uint8       `json:"SessionIndex"`        // 会话索引
    CurrentSessionIndex uint8       `json:"CurrentSessionIndex"` // 当前会话索引
    SessionCount        uint8       `json:"SessionCount"`        // 会话数量
    ServerName          string      `json:"ServerName"`          // 服务器名称
    Track               string      `json:"Track"`               // 赛道名称
    TrackConfig         string      `json:"TrackConfig"`         // 赛道配置
    Name                string      `json:"Name"`                // 会话名称
    Type                SessionType `json:"Type"`                // 会话类型
    Time                uint16      `json:"Time"`                // 会话时间（分钟）
    Laps                uint16      `json:"Laps"`                // 圈数
    WaitTime            uint16      `json:"WaitTime"`            // 等待时间（秒）
    AmbientTemp         uint8       `json:"AmbientTemp"`         // 环境温度（摄氏度）
    RoadTemp            uint8       `json:"RoadTemp"`            // 赛道温度（摄氏度）
    WeatherGraphics     string      `json:"WeatherGraphics"`     // 天气图形
    ElapsedMilliseconds int32       `json:"ElapsedMilliseconds"` // 已流逝毫秒数
    EventType           Event       `json:"EventType"`           // 事件类型
}
```

## 4. 发送消息结构体

### 4.1 EnableRealtimePosInterval (实时位置更新间隔)
**事件类型**: EventRealtimeposInterval (200)
```go
type EnableRealtimePosInterval struct {
    Type     uint8  // 事件类型
    Interval uint16 // 更新间隔（毫秒）
}
```

**用法说明**：
- **构造函数**：`NewEnableRealtimePosInterval(interval int) EnableRealtimePosInterval`
- **参数**：`interval` - 实时位置更新间隔（毫秒）
- **示例**：
  ```go
  // 设置100ms的实时位置更新间隔
  msg := udp.NewEnableRealtimePosInterval(100)
  client.SendMessage(msg)
  ```

### 4.2 GetCarInfo (获取车辆信息请求)
**事件类型**: EventGetCarInfo (201)
```go
type GetCarInfo struct {
    // 无额外字段，仅用于触发服务器返回车辆信息
}
```

**用法说明**：
- **示例**：
  ```go
  // 请求服务器返回所有车辆信息
  client.SendMessage(udp.GetCarInfo{})
  ```

### 4.3 SendChat (发送聊天消息)
**事件类型**: EventSendChat (202)
```go
type SendChat struct {
    EventType    uint8  // 事件类型
    CarID        uint8  // 发送者车辆ID
    Len          uint8  // 消息长度
    UTF32Encoded []byte // UTF32编码的消息内容
}
```

**用法说明**：
- **构造函数**：`NewSendChat(carID CarID, data string) (*SendChat, error)`
- **参数**：
  - `carID` - 发送者车辆ID
  - `data` - 聊天内容（自动过滤非ASCII字符）
- **示例**：
  ```go
  // 发送聊天消息
  msg, err := udp.NewSendChat(1, "Hello, world!")
  if err == nil {
      client.SendMessage(msg)
  }
  ```

### 4.4 BroadcastChat (广播聊天消息)
**事件类型**: EventBroadcastChat (203)
```go
type BroadcastChat struct {
    EventType    uint8  // 事件类型
    Len          uint8  // 消息长度
    UTF32Encoded []byte // UTF32编码的消息内容
}
```

**用法说明**：
- **构造函数**：`NewBroadcastChat(data string) (*BroadcastChat, error)`
- **参数**：`data` - 广播内容（自动过滤非ASCII字符）
- **示例**：
  ```go
  // 发送广播消息
  msg, err := udp.NewBroadcastChat("Welcome to the server!")
  if err == nil {
      client.SendMessage(msg)
  }
  ```

### 4.5 GetSessionInfo (获取会话信息请求)
**事件类型**: EventGetSessionInfo (204)
```go
type GetSessionInfo struct {
    // 无额外字段，仅用于触发服务器返回会话信息
}
```

**用法说明**：
- **示例**：
  ```go
  // 请求服务器返回当前会话信息
  client.SendMessage(udp.GetSessionInfo{})
  ```

### 4.6 SetSessionInfo (设置会话信息请求)
**事件类型**: EventSetSessionInfo (205)
```go
type SetSessionInfo struct {
    Version             uint8       // 版本号
    SessionIndex        uint8       // 会话索引
    CurrentSessionIndex uint8       // 当前会话索引
    SessionCount        uint8       // 会话数量
    Name                string      // 会话名称
    Type                SessionType // 会话类型
    Time                uint16      // 会话时间（分钟）
    Laps                uint16      // 圈数
    WaitTime            uint16      // 等待时间（秒）
    AmbientTemp         uint8       // 环境温度（摄氏度）
    RoadTemp            uint8       // 赛道温度（摄氏度）
    WeatherGraphics     string      // 天气图形
}
```

**用法说明**：
- **示例**：
  ```go
  // 设置会话信息
  msg := udp.SetSessionInfo{
      Name:  "Practice Session",
      Type:  udp.SessionTypePractice,
      Time:  30, // 30分钟
      Laps:  0,  // 无圈数限制
  }
  client.SendMessage(msg)
  ```

### 4.7 KickUser (踢用户)
**事件类型**: EventKickUser (206)
```go
type KickUser struct {
    EventType uint8 // 事件类型
    CarID     uint8 // 要踢的用户车辆ID
}
```

**用法说明**：
- **构造函数**：`NewKickUser(carID uint8) *KickUser`
- **参数**：`carID` - 要踢的用户车辆ID
- **示例**：
  ```go
  // 踢出车辆ID为2的用户
  msg := udp.NewKickUser(2)
  client.SendMessage(msg)
  ```

### 4.8 NextSession (下一会话请求)
**事件类型**: EventNextSession (207)
```go
type NextSession struct {
    // 无额外字段，请求进入下一会话
}
```

**用法说明**：
- **示例**：
  ```go
  // 请求进入下一会话
  client.SendMessage(&udp.NextSession{})
  ```

### 4.9 RestartSession (重启会话请求)
**事件类型**: EventRestartSession (208)
```go
type RestartSession struct {
    // 无额外字段，请求重启当前会话
}
```

**用法说明**：
- **示例**：
  ```go
  // 请求重启当前会话
  client.SendMessage(&udp.RestartSession{})
  ```

### 4.10 AdminCommand (管理命令)
**事件类型**: EventAdminCommand (209)
```go
type AdminCommand struct {
    EventType    uint8  // 事件类型
    Len          uint8  // 命令长度
    UTF32Encoded []byte // UTF32编码的命令内容
}
```

**用法说明**：
- **构造函数**：`NewAdminCommand(data string) (*AdminCommand, error)`
- **参数**：`data` - 管理命令内容
- **示例**：
  ```go
  // 发送管理命令
  msg, err := udp.NewAdminCommand("/kick 2")
  if err == nil {
      client.SendMessage(msg)
  }
  ```
