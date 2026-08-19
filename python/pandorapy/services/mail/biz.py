"""mail 业务逻辑层 —— 对应 Go 侧 internal/biz/mail.go。

附件有三种形态,发放路径完全不同,**混了就是资产事故**:

    stack     无唯一 ID 的可堆叠物品     → inventory.Grant(按 config_id + count 入包)
    instance  有唯一 ID 的铸造凭证       → inventory.GrantInstances(按 count **逐件铸造**)
    transfer  既存实例的托管转移         → inventory.ClaimTransfers(托管行**只改归属**)

★ 三条最容易在移植中丢掉的不变量:

1. **未识别形态 → 整封 fail-closed**(§9.21 滚动升级版本偏斜)
   新版本写入了旧 reader 不认识的附件形态时,**整封拒发、保持未领**,
   绝不"跳过不认识的、发认识的" —— 那样新形态附件被静默吞掉,
   而邮件被标成已领,资产永久消失。

2. **transfer 没有空领豁免**(`allow_noop_grant` 对它**不放行**)
   空领 = 邮件标已领而托管行原地不动 → 实例资产静默滞留 escrow。
   宁可领取报错保持可重领。stack/instance 允许空领是因为它们是"铸造",
   没发出去就是没发;transfer 是"搬运",托管行里的资产已经从发送方扣走了。

3. **claim 记录必须在发放**之后**
   反过来会让"记了 claim 但发放失败"变成永久丢失(下次重领被 claim 表挡住)。
   顺序对了之后,"发放成功但记 claim 失败"由 inventory 的幂等键兜底,不会重发。
"""

from __future__ import annotations

from pandora.mail.v1 import mail_pb2

from pandorapy import errcode
from pandorapy import log as plog


def partition_attachments(atts) -> tuple[list, list, list, int]:
    """按 oneof 形态分三类,并统计**未识别**的个数。

    ★ 未识别不是"忽略",是必须上报的信号 —— 见模块头 ①。
    用 WhichOneof 而不是逐个 HasField:新增形态时这里自然落进 unknown,
    而逐个 HasField 的写法容易被"顺手加一个分支"改成静默跳过。
    """
    stack, inst, transfer, unknown = [], [], [], 0
    for att in atts:
        kind = att.WhichOneof("body")
        if kind == "stack":
            stack.append(att)
        elif kind == "instance":
            inst.append(att)
        elif kind == "transfer":
            transfer.append(att)
        else:
            unknown += 1
    return stack, inst, transfer, unknown


def expand_instance_config_ids(atts) -> list[int]:
    """把实例型附件按 count **逐件展开**(count 份 → count 个元素)。

    count=0 防御性视为 1 件(发送侧已校验 >=1)。
    逐件展开是因为 instance 语义 = 每件都是独立实例,不能合并成一条 count=N。
    """
    out: list[int] = []
    for att in atts:
        if att.WhichOneof("body") != "instance":
            continue  # 调用方已分组,正常不会到这
        n = att.instance.count or 1
        out.extend([att.instance.item_config_id] * n)
    return out


class MailUsecase:
    """mail 业务逻辑核心。对应 Go 的 biz.MailUsecase。"""

    __slots__ = ("_repo", "_granter", "_inst_granter", "_xfer_claimer", "_cfg")

    def __init__(self, repo, granter, inst_granter, xfer_claimer, cfg) -> None:
        self._repo = repo
        self._granter = granter  # stack 形态,可为 None
        self._inst_granter = inst_granter  # instance 形态,可为 None
        self._xfer_claimer = xfer_claimer  # transfer 形态,可为 None(但领取时必须有)
        self._cfg = cfg

    async def claim_mail(self, player_id: int, mail_id: int, now_ms: int) -> list:
        """领取附件。幂等键保证重复领取 / 重试不重发(资产不变量 §9.7)。"""
        payload = await self._repo.get_claimable_payload(player_id, mail_id, now_ms)
        if payload is None:
            raise errcode.PandoraError(
                errcode.ErrMailNotFound, "mail %d not found or not claimable", mail_id
            )

        rec = mail_pb2.MailContentStorageRecord()
        try:
            rec.ParseFromString(payload)
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "decode mail %d: %s", mail_id, exc
            ) from exc

        if not rec.attachments:
            raise errcode.PandoraError(
                errcode.ErrMailNoAttachment, "mail %d has no attachment", mail_id
            )

        claimed, intent_open = await self._repo.get_claim_state(player_id, mail_id)
        if claimed:
            # 已领:返回附件视图 + 明确错误码(客户端据此显示"已领过",不是失败)。
            raise _already_claimed(mail_id, list(rec.attachments))
        if intent_open:
            # DS 三段式领取意图已创建(bag phase 2):本邮件只能经 bag journal 链终结。
            # 旧直连链在此**互斥拒** —— 若继续走 inventory 发放,与已/将落库的 journal 双发。
            raise errcode.PandoraError(
                errcode.ErrMailClaimInProgress,
                "mail %d claim in progress via bag journal",
                mail_id,
            )

        stack_atts, inst_atts, xfer_atts, unknown = partition_attachments(rec.attachments)

        # ★ ① 滚动版本偏斜的 fail-closed 分支(§9.21)。
        # 这是**必须被运维发现**的信号(说明有旧副本在读新数据),但只返回业务码时
        # access log 只有泛化失败,定位不到是哪封 / 几个未知形态 → 必须留 WARN。
        if unknown > 0:
            plog.get().warning(
                "mail_claim_unknown_attachment",
                player_id=player_id,
                mail_id=mail_id,
                unknown=unknown,
                hint="疑似滚动升级版本偏斜:旧副本读到新增附件形态,整封 fail-closed 保持未领",
            )
            raise errcode.PandoraError(
                errcode.ErrMailAttachmentUnsupported,
                "mail %d has %d unrecognized attachment kind(s)",
                mail_id,
                unknown,
            )

        # 发放顺序 stack → instance → transfer,**各用独立幂等键**。
        # 任一步失败,下次重领靠各自的幂等键去重,已发的不重发。
        if stack_atts:
            key = f"mail:{mail_id}:{player_id}"
            if self._granter is not None:
                await self._granter.grant(player_id, stack_atts, key)
            elif not self._cfg.allow_noop_grant:
                raise errcode.PandoraError(errcode.ErrInternal, "inventory granter unavailable")

        if inst_atts:
            # 幂等键优先用发送侧写入的(跨重发稳定);没有才现造。
            key = rec.instance_grant_key or f"mail_inst:{mail_id}:{player_id}"
            if self._inst_granter is not None:
                await self._inst_granter.grant_instances(
                    player_id, expand_instance_config_ids(inst_atts), key
                )
            elif not self._cfg.allow_noop_grant:
                raise errcode.PandoraError(errcode.ErrInternal, "instance granter unavailable")

        if xfer_atts:
            # ★ ② transfer **无空领豁免**(allow_noop_grant 不放行)。
            # 空领 = 邮件标已领而托管行原地滞留,实例资产静默丢失;
            # 宁可领取报错保持可重领。
            if self._xfer_claimer is None:
                raise errcode.PandoraError(errcode.ErrInternal, "transfer claimer unavailable")
            key = f"mail_xfer:{mail_id}:{player_id}"
            await self._xfer_claimer.claim_transfers(player_id, xfer_atts, key)

        # ★ ③ 入库成功后**再**记 claim。
        # 此处即便失败,下次重领被 inventory 的幂等键去重,不会重发。
        await self._repo.record_claim(player_id, mail_id)

        # 个人邮件置 claimed(系统/公会靠 player_mail_claim 表)。
        try:
            await self._repo.set_personal_status(player_id, mail_id, "claimed")
        except Exception as exc:  # noqa: BLE001
            # 权威是 claim 表,列表侧幂等纠正;但持续失败会让 player_mail.status 与权威
            # 长期漂移(客户端显示未领实际已领),零可观测无法排查 → DEBUG 留证。
            plog.get().debug(
                "mail_set_personal_status_failed",
                player_id=player_id,
                mail_id=mail_id,
                target_status="claimed",
                err=str(exc),
            )
        return list(rec.attachments)

    async def delete_mail(self, player_id: int, mail_id: int) -> None:
        await self._repo.delete_personal(player_id, mail_id)


def _already_claimed(mail_id: int, attachments: list) -> errcode.PandoraError:
    """已领取 —— 带上附件视图,客户端可以显示"你领过这些"。"""
    err = errcode.PandoraError(
        errcode.ErrMailAlreadyClaimed, "mail %d already claimed", mail_id
    )
    err.attachments = attachments  # type: ignore[attr-defined]
    return err
