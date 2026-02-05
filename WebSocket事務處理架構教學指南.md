# Unity 6 WebSocket 事務處理架構教學指南

**目標受眾：** C# 資深開發者、Unity 網路工程師  
**難度等級：** Master Engineer  
**更新日期：** 2026 年 2 月

---

## 目錄

1. [概述](#概述)
2. [協議層設計](#協議層設計)
3. [架構層：事務分發器](#架構層事務分發器)
4. [業務處理層：請求與回應模式](#業務處理層請求與回應模式)
5. [執行緒模型與上下文切換](#執行緒模型與上下文切換)
6. [完整流程概覽](#完整流程概覽)
7. [Protobuf 整合架構](#protobuf-整合架構)
8. [Protobuf 使用實戰](#protobuf-使用實戰)

---

## 概述

### 核心挑戰

在處理 WebSocket 這類「全雙工、長連接」協議時，核心挑戰在於如何將單一的字節流（Byte Stream）拆解並路由到具體的業務邏輯中。

這不單是 if-else 的問題，而是關乎以下四大層面：

- **協議設計 (Protocol Design)** — 如何定義「信封」結構
- **序列化策略 (Serialization)** — 如何高效序列化/反序列化
- **記憶體管理 (Allocation)** — 避免 GC 壓力
- **執行緒調度 (Thread Dispatching)** — 主線程/背景線程協調

---

## 協議層設計

### 「信封」結構 (The Packet Structure)

Socket 傳輸的是 Raw Bytes。為了區分「移動請求」還是「聊天訊息」，必須在應用層定義一個包頭（Packet Header）。

最經典且高效的設計模式是：**Length + OpCode + Payload**

| 欄位名稱 | 字節數 | 說明 |
|---------|-------|------|
| **Length** | 4 bytes | 封包總長度，用於解決 TCP 黏包/拆包 (Sticky/Split packets) 問題 |
| **OpCode** | 2-4 bytes | 操作碼 (Operation Code)，「事務分類」的核心 ID |
| **Payload** | N bytes | 實際業務數據（建議使用 Protobuf、MessagePack 或 FlatBuffers，避免 JSON 帶來的 GC 壓力） |

### 協議定義範例 (C#)

```csharp
public enum OpCode : ushort
{
    Heartbeat = 0x001,
    LoginRequest = 0x101,
    LoginResponse = 0x102,
    PlayerMove = 0x201,
    // ... 更多業務類型
}
```

---

## 架構層：事務分發器

### 設計原則

在資深工程師的架構中，不會在 Socket 接收迴圈中寫巨大的 switch-case。需要一個 **O(1) 複雜度** 的路由機制。

### 核心策略：字典映射 (Dictionary Mapping)

利用 `Dictionary<OpCode, Handler>` 來儲存路由表。

**優點：**
- 擴充性強，業務邏輯解耦
- 查詢性能 O(1)

**Unity 6 優化建議：**
- 考慮 IL2CPP 和 AOT (Ahead-of-Time) 編譯
- 避免過度使用 Reflection（反射）
- 建議使用 靜態註冊 或 Source Generators

### 實作範例

```csharp
// 定義處理器的委派 (Delegate)
// 使用 Span<byte> 或 ReadOnlyMemory<byte> 實現 Zero-Copy 解析
public delegate UniTask PacketHandler(ReadOnlyMemory<byte> payload);

public class NetworkDispatcher
{
    private readonly Dictionary<OpCode, PacketHandler> _handlers = new();

    public void Register(OpCode opCode, PacketHandler handler)
    {
        _handlers[opCode] = handler;
    }

    public async UniTask DispatchAsync(OpCode opCode, ReadOnlyMemory<byte> payload)
    {
        if (_handlers.TryGetValue(opCode, out var handler))
        {
            await handler(payload);
        }
        else
        {
            Debug.LogWarning($"Unhandled OpCode: {opCode}");
        }
    }
}
```

---

## 業務處理層：請求與回應模式

### 設計背景

WebSocket 是非同步的。當你發送一個請求（如 LoginRequest），伺服器可能在 100ms 後才回傳 LoginResponse。  
在程式碼中，我們希望寫起來像同步代碼一樣直觀（Linear code flow）。

### 解決方案：TaskCompletionSource (TCS)

使用 `TaskCompletionSource` 來實現 Awaitable Request。

#### 設計思路

1. 客戶端發送請求，生成唯一的 RpcId
2. 創建 `TaskCompletionSource` 並存入字典 `_pendingRequests`
3. `await` 這個 Task
4. 當 Socket 收到回應時，根據 RpcId 找到對應的 TCS，設定結果 (SetResult)

#### Master Engineer 的實作

```csharp
public class RpcManager
{
    // 追蹤等待中的請求: <RpcId, TaskCompletionSource>
    private Dictionary<int, TaskCompletionSource<IResponse>> _pending = new();
    private int _idCounter = 0;

    public async UniTask<TResponse> CallAsync<TResponse>(OpCode op, IRequest request) 
        where TResponse : class
    {
        int rpcId = ++_idCounter;
        var tcs = new TaskCompletionSource<IResponse>();
        _pending[rpcId] = tcs;

        try
        {
            // 1. 發送封包 (包含 rpcId)
            SendPacket(op, rpcId, request);

            // 2. 設定超時 (Timeout)
            var timeoutTask = UniTask.Delay(TimeSpan.FromSeconds(5));
            var completedTask = await UniTask.WhenAny(tcs.Task, timeoutTask);

            if (completedTask == timeoutTask)
            {
                throw new TimeoutException("RPC Timeout");
            }

            // 3. 轉型並返回
            return (TResponse)await tcs.Task;
        }
        finally
        {
            _pending.Remove(rpcId);
        }
    }

    // 當收到 Server 回應時被調用
    public void OnResponseReceived(int rpcId, IResponse response)
    {
        if (_pending.TryGetValue(rpcId, out var tcs))
        {
            tcs.TrySetResult(response);
        }
    }
}
```

---

## 執行緒模型與上下文切換

### 核心問題

這是 Unity 開發者最常踩的坑：

- WebSocket 的接收通常發生在 **背景執行緒 (Background Thread/ThreadPool)**
- 但 Unity 的 API（如 `Transform.position`、UI）只能在 **主執行緒 (Main Thread)** 存取

### 解決方案

#### 方案 1：SynchronizationContext

在接收到封包後，使用 `UniTask.SwitchToMainThread()` 切換上下文。

#### 方案 2：Command Queue（推薦）

將解析後的業務物件放入 `ConcurrentQueue`，並在 Unity 的 `Update()` 迴圈中取出執行。

**優勢：**
- 確保嚴格的順序性
- 容易 Debug
- 不依賴 SynchronizationContext

### 實作範例

```csharp
// 主線程輪詢模式 (Polling)
private ConcurrentQueue<Action> _executionQueue = new();

// 網路線程收到訊息
private void OnPacketReceived(OpCode op, byte[] data)
{
    // 解析數據但不執行邏輯
    var packet = Deserialize(op, data); 
    
    // 封裝成 Action 丟回主線程
    _executionQueue.Enqueue(() => BusinessLogic(packet));
}

// Unity MonoBehaviour Update
void Update()
{
    while (_executionQueue.TryDequeue(out var action))
    {
        action.Invoke(); // 安全地執行 Unity API
    }
}
```

---

## 完整流程概覽

### 事務處理流程表

| 階段 | 職責 | 關鍵技術點 |
|------|------|---------|
| 1. **Ingest** | 讀取 Bytes，處理黏包/拆包 | Span\<T\>, Circular Buffer |
| 2. **Decode** | 讀取 Header，識別 OpCode | Binary Reading (Little/Big Endian) |
| 3. **Route** | 根據 OpCode 查找 Handler | Dictionary, Strategy Pattern |
| 4. **Deserialize** | 將 Body 轉為 C# 物件 | Protobuf, MessagePack (避免 JSON) |
| 5. **Sync** | 調度回 Unity Main Thread | UniTask, ConcurrentQueue |
| 6. **Execute** | 執行具體業務邏輯 | Managers, Systems, ECS |

### 重要提示

在 **Unity 6** 中，務必利用 **UniTask** 取代原生的 Task，以獲得：
- Zero Allocation 的優勢
- 防止 WebGL 平台上的線程問題
- 更好的效能

---

## Protobuf 整合架構

### 為什麼選擇 Protobuf？

Protobuf（Protocol Buffers）是工業級標準，優勢包括：
- **序列化體積** — 比 JSON 小 3-10 倍
- **解析速度** — 遠快於 JSON
- **跨語言兼容性** — C# Unity Client ↔ Go/C++ Server

### 核心挑戰

如何將 OpCode（數值）與 Protobuf 生成的 C# Class（類型）進行高效綁定，並自動完成反序列化。

### 泛型適配器模式

為了避免手寫醜陋的 switch-case，使用泛型和介面封裝。

```csharp
using Google.Protobuf;
using System;
using System.Collections.Generic;

// 定義一個非泛型的介面，用於在 Dictionary 中儲存
public interface IPacketHandler
{
    void Handle(ReadOnlySpan<byte> data);
}

// 泛型實作：自動處理反序列化
public class PacketHandler<TMsg> : IPacketHandler where TMsg : IMessage<TMsg>, new()
{
    // Protobuf 官方提供的解析器
    private readonly MessageParser<TMsg> _parser;
    // 業務邏輯的回調 (Action)
    private readonly Action<TMsg> _handlerLogic;

    public PacketHandler(Action<TMsg> handlerLogic)
    {
        _parser = new MessageParser<TMsg>(() => new TMsg());
        _handlerLogic = handlerLogic;
    }

    public void Handle(ReadOnlySpan<byte> data)
    {
        // 1. 反序列化 (Deserialize)
        // 注意：使用 Span 減少記憶體複製 (Unity 2021.2+ / .NET Standard 2.1)
        TMsg msg = _parser.ParseFrom(data);
        
        // 2. 執行業務邏輯
        _handlerLogic(msg);
    }
}
```

### 網路管理器整合

```csharp
public class NetworkManager : MonoBehaviour
{
    // 路由表：OpCode -> 處理器
    private Dictionary<ushort, IPacketHandler> _router = new();

    void Awake()
    {
        // 註冊業務邏輯
        RegisterHandler<LoginRequest>(OpCode.LoginRequest, OnLoginRequest);
        RegisterHandler<PlayerMove>(OpCode.PlayerMove, OnPlayerMove);
    }

    // 泛型註冊方法 (API 入口)
    public void RegisterHandler<T>(OpCode opCode, Action<T> callback) 
        where T : IMessage<T>, new()
    {
        ushort key = (ushort)opCode;
        if (_router.ContainsKey(key))
        {
            Debug.LogError($"OpCode {opCode} already registered!");
            return;
        }
        
        // 創建適配器並存入字典
        _router.Add(key, new PacketHandler<T>(callback));
    }

    // 當 Socket 收到完整封包時調用
    public void OnPacketReceived(ushort opCode, byte[] payload)
    {
        if (_router.TryGetValue(opCode, out var handler))
        {
            // 將 byte[] 轉為 Span 傳遞，零拷貝
            handler.Handle(new ReadOnlySpan<byte>(payload));
        }
        else
        {
            Debug.LogWarning($"Unhandled OpCode: {opCode}");
        }
    }

    // --- 具體的業務邏輯 ---
    
    private void OnLoginRequest(LoginRequest req)
    {
        Debug.Log($"Login Request: User={req.Username}");
        // 呼叫 LoginSystem...
    }

    private void OnPlayerMove(PlayerMove req)
    {
        // 同步玩家位置
        // transform.position = new Vector3(req.Position.X, ...);
    }
}
```

### OneOf vs Header + Body 模式比較

#### Envelope 模式定義 (.proto)

```protobuf
message GamePacket {
  int32 rpc_id = 1;
  oneof payload {
    LoginRequest login_req = 2;
    PlayerMove move = 3;
    Heartbeat heartbeat = 4;
  }
}
```

#### 比較分析

| 特性 | Header + Raw Body（推薦） | OneOf Envelope |
|------|---------------------------|----------------|
| 路由效率 | 極高（直接讀取 OpCode 數字跳轉） | 低（需解析整個外層包） |
| 靈活性 | 高（OpCode 可動態擴展） | 低（新增協議需修改外層 proto） |
| 序列化成本 | 低（只序列化 Body） | 中（多一層外殼） |
| 適用場景 | 即時戰鬥、MMO（性能敏感） | 回合制、簡單應用、HTTP API |

**結論：** 在 Unity 6 + Socket 場景下，堅持使用 Header (OpCode) + Body 的方式，性能最好且與 `Dictionary<OpCode, Handler>` 架構完美契合。

---

## Protobuf 使用實戰

### 核心流程圖

```
1. 撰寫 .proto 檔  →  2. 編譯 (protoc)  →  3. 導入 Unity  →  4. 使用
```

### 第一步：環境準備 (Setup)

#### 1.1 下載編譯器 (protoc)

1. 前往 [Google Protobuf GitHub Releases](https://github.com/protocolbuffers/protobuf/releases)
2. 下載 `protoc-xx.x-win64.zip` (或適合你 OS 的版本)
3. 解壓縮，將 `bin/protoc.exe` 放到專案根目錄下的 `Tools/Protobuf/`

#### 1.2 安裝 Unity Runtime Library

**方法 A（推薦 - NuGet）：**
- 使用 NuGetForUnity 搜尋並安裝 `Google.Protobuf`

**方法 B（手動）：**
- 從 NuGet 下載 `Google.Protobuf` package
- 將 `lib/netstandard2.0` 或 `2.1` 裡的 `Google.Protobuf.dll` 拖進 Unity 的 `Assets/Plugins`

**Unity 6 注意事項：** 確保下載的 DLL 版本與 .proto 語法版本相容（通常建議都用最新版）。

### 第二步：定義協議 (The Contract)

在專案根目錄建立 `Protocol` 資料夾，新建 `game_protocol.proto`：

```protobuf
syntax = "proto3"; // 指定使用 proto3 語法
package Game.Data; // 這會變成 C# 的 namespace

// 優化：生成 C# 時的額外設定
option csharp_namespace = "Game.Data"; 

// 定義一個測試訊息
message LoginRequest {
  string username = 1;
  string password = 2;
  int32 client_version = 3;
}

message LoginResponse {
  bool success = 1;
  string error_message = 2;
  int64 server_time = 3;
}
```

### 第三步：自動化編譯腳本 (Automation)

資深開發者不手動敲指令。在專案根目錄建立 `build_proto.bat`：

```batch
@echo off
echo Compiling Protobuf...

:: 設定路徑變數
set PROTOC_PATH=./Tools/Protobuf/bin/protoc.exe
set PROTO_DIR=./Protocol
set OUT_DIR=./Assets/Scripts/Generated/Protobuf

:: 確保輸出目錄存在
if not exist "%OUT_DIR%" mkdir "%OUT_DIR%"

:: 執行編譯
"%PROTOC_PATH%" --proto_path="%PROTO_DIR%" --csharp_out="%OUT_DIR%" "%PROTO_DIR%/*.proto"

echo Done!
pause
```

**操作：** 雙擊 `.bat` 檔。你會發現 `Assets/Scripts/Generated/Protobuf` 裡多了 `GameProtocol.cs`。

### 第四步：在 Unity 中使用 (Coding)

#### 4.1 序列化 (Object → Bytes)

```csharp
using Game.Data; // 引用生成的 namespace
using Google.Protobuf; // 引用 Runtime

public byte[] CreateLoginPacket()
{
    // 建立物件
    var loginReq = new LoginRequest
    {
        Username = "MasterEngineer",
        Password = "SecretPassword123",
        ClientVersion = 100
    };

    // 轉為 byte array
    byte[] data = loginReq.ToByteArray();
    
    // 或者 (Unity 6 高效寫法) 寫入現有的 Buffer 避免 GC
    // loginReq.WriteTo(outputStream);
    
    return data;
}
```

#### 4.2 反序列化 (Bytes → Object)

```csharp
public void OnPacketReceived(byte[] data)
{
    // 方法 1: 直接 Parse
    LoginRequest req = LoginRequest.Parser.ParseFrom(data);
    
    Debug.Log($"User: {req.Username}, Ver: {req.ClientVersion}");

    // 方法 2: 在 Dispatcher 中，通常是泛型處理
}
```

### 第五步：Master Engineer 的注意事項

#### 版本控制 (Git)

- **.proto 檔** — 必須上傳 Git
- **protoc.exe 工具** — 建議上傳 Git（讓團隊成員都能編譯）
- **生成的 .cs 檔** — 可上傳或由 CI/CD 自動生成，但為了開發方便通常會上傳

#### 記憶體優化 (Allocation)

- 盡量避免頻繁呼叫 `ToByteArray()`，因為每次都會 `new byte[]`
- 使用 `IMessage.WriteTo(CodedOutputStream)` 直接寫入共用的 Socket Send Buffer

```csharp
public class NetworkSender
{
    private ClientSocket _socket;
    // 重用緩衝區，避免每次發送都 new
    private byte[] _sendBuffer = new byte[4096]; 

    public void Send<T>(OpCode opCode, T message) where T : IMessage<T>
    {
        int headerSize = 4; // 假設 Header 長度 (Length + OpCode)
        
        // 1. 計算 Protobuf 實際大小
        int bodySize = message.CalculateSize();
        int totalSize = headerSize + bodySize;

        // 擴容檢查
        if (totalSize > _sendBuffer.Length) 
            Array.Resize(ref _sendBuffer, totalSize);

        using (var output = new CodedOutputStream(_sendBuffer))
        {
            // 2. 寫入 Header (簡化演示)
            output.WriteRawBytes(BitConverter.GetBytes((ushort)opCode));

            // 3. 寫入 Protobuf Body (直接寫入 Buffer，不產生新 array)
            message.WriteTo(output);
        }

        // 4. 發送 socket (注意只發送有效長度)
        _socket.Send(_sendBuffer, 0, totalSize);
    }
}
```

#### 空值處理 (Null Safety)

- Proto3 沒有 null 的概念
- `string` 預設是 `""`，`int` 預設是 `0`
- 不要判斷 `if (req.Username == null)`，改為 `if (req.Username.Length == 0)`

---

## 最佳實踐總結

### 架構層面

1. ✅ 使用 **Dictionary<OpCode, Handler>** 實現 O(1) 路由
2. ✅ 採用 **Protobuf** 進行序列化，避免 JSON
3. ✅ 使用 **UniTask** 而非原生 Task
4. ✅ 實作 **RpcManager** 來處理 Request-Response

### 記憶體層面

1. ✅ 重用 Buffer，避免頻繁 new
2. ✅ 使用 `Span<T>` 和 `ReadOnlyMemory<T>` 進行 Zero-Copy 操作
3. ✅ 使用 `ConcurrentQueue` 進行執行緒間通信

### 執行緒層面

1. ✅ 後台線程只進行解析，不執行 Unity API
2. ✅ 使用 **Command Queue** 模式將業務邏輯回送主線程
3. ✅ 實作 **SynchronizationContext** 或 **UniTask** 進行上下文切換

### 維護層面

1. ✅ 使用自動化腳本編譯 Protobuf
2. ✅ .proto 檔納入版本控制
3. ✅ 分離協議定義與業務邏輯

---

## 參考資源

- [Google Protobuf 官方文檔](https://developers.google.com/protocol-buffers)
- [Cysharp UniTask](https://github.com/Cysharp/UniTask)
- [Unity 網路最佳實踐](https://docs.unity3d.com/Manual/UNet.html)

---

**文件版本：** 1.0  
**最後更新：** 2026 年 2 月 5 日
