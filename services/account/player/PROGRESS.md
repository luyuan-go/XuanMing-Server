  - **并发编辑者交叉影响(本轮实测,非本批次所致)**:①`player.proto` 的 `instance_id` 一度使 player
    模块编译失败(pb 未重生),该编辑者已于同日重生 pb,现 player build+vet+test 全绿;②`pkg/configtable`
    的 item 行校验被新增了「装备缩放X/Y/Z 必须 > 0」(**无 `isEquipType` 守卫,对非装备行也强制**)与
    `identify_pool_id` 一致性两条,使 `TestValidateItemRow` / `TestItemTableSlotQueries` / `TestLoad*`
    一批**该编辑者自己的**用例转红;任务域用例已按新规则补齐 fixture 转绿,缩放校验是否应只对装备生效
    需该编辑者定夺(已记入 INC-20260811-001 行动项 A-9)。
