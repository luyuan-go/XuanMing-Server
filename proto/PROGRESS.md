- **同日四修:砍掉 locator 的 owner_epoch 层(用户拍板「按最标准的去做」)**。
  保留连接三元组(assignment_id + admission_id + admission_seq)比对 —— 那是本投影真正要解决的
  「同一个 Pod 上哪条物理连接是当前这条」;**删掉跨 assignment 全序**:`resolveAuthoritativeHubFence`、
  `HubOwnerAuthority`、`data/owner_client.go`、`locator.owner_addr` 配置与接线、Lua 里的
  owner_epoch 比较分支、`HubPresenceFence` 的 `OwnerEpoch`/`OwnerOperationID` 字段、
  以及随之失去消费点的 `HubInstanceUID`/`HubInstanceEpoch` 死字段。
  **判据**:①locator 是 presence 投影不是 owner 权威(§9.22),该条还明写每玩家 owner_epoch 的
  线性一致 authority「尚未实现」,在这里自建一个分散版本正是它禁止的;②代价具体 ——
  带 fence 的 HUB 写会**实时查 owner 服务**且 `hubOwner==nil` 直接 `ErrUnavailable`,
  等于给「玩家进大厅写位置」加了个会 fail-closed 的跨服务强依赖,用关键路径可用性换一个
  不归自己管的判定;③客户端本就只有三元组,owner_epoch 只能服务端自己去查,协议上也不对称。
  新语义:同 assignment 内按 seq 单调 + admission_id 防 ABA;**跨 assignment 一律接受**,
  归属由 hub_allocator 的 assignment/placement 权威决定。
  proto 里那段「跨 assignment 必须实时查询 owner authority」的注释是随该层一起加的(与更早的
  「本投影不得反向充当 owner authority」相反),已按实际行为改回并写明取舍理由。
  测试:删掉整份 owner 专用测试(356 行,场景本身就是「owner 变了」,改造只会留半吊子),
  新增「跨 assignment 接受」「同 assignment 内旧 admission / 同序 ABA 必须拒、新 seq 接受」两组;
  README / 配置模板同步。locator 全部 4 个包 + team + offlinewatch **9 个包测试全绿**,
  owner 残留符号 0。
