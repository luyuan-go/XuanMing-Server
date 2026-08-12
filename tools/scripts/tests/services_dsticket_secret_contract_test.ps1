[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$ProjectRoot = (Resolve-Path "$PSScriptRoot/../../..").Path
$ServicesKustomizeDir = Join-Path $ProjectRoot 'deploy/k8s/services'
$SignerServices = @('login', 'matchmaker', 'matchmaker-pve', 'hub-allocator')
$AllServices = @(
    'login', 'player', 'data-service', 'friend', 'chat', 'guild', 'mail', 'player-locator',
    'leaderboard', 'team', 'matchmaker', 'matchmaker-pve', 'trade', 'dialogue', 'mission', 'push', 'owner',
    'inventory', 'auction', 'ds-allocator', 'hub-allocator', 'battle-result'
)

function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw "ASSERT FAILED:$Message" }
}

function Assert-Throws([scriptblock]$Action, [string]$Message) {
    try { & $Action } catch { return }
    throw "ASSERT FAILED:应抛错但成功:$Message"
}

function Get-ServicesContractRows([string]$Manifest) {
    $tmp = Join-Path ([System.IO.Path]::GetTempPath()) ('pandora-services-contract-' + [guid]::NewGuid().ToString('N') + '.yaml')
    try {
        [System.IO.File]::WriteAllText($tmp, $Manifest, [System.Text.UTF8Encoding]::new($false))
        $jsonPath = '{.kind}{"\t"}{.metadata.name}{"\t"}{.spec.template.spec.securityContext.runAsNonRoot}{"\t"}{.spec.template.spec.securityContext.runAsUser}{"\t"}{.spec.template.spec.securityContext.runAsGroup}{"\t"}{.spec.template.spec.securityContext.fsGroup}{"\t"}{.spec.template.spec.securityContext.fsGroupChangePolicy}{"\t"}{.spec.template.spec.containers[*].name}{"\t"}{.spec.template.spec.containers[*].volumeMounts[?(@.name=="dsticket")].mountPath}{"\t"}{.spec.template.spec.containers[*].volumeMounts[?(@.name=="dsticket")].readOnly}{"\t"}{.spec.template.spec.volumes[?(@.name=="dsticket")].secret.secretName}{"\t"}{.spec.template.spec.volumes[?(@.name=="dsticket")].secret.defaultMode}{"\t"}{.spec.template.spec.containers[*].volumeMounts[?(@.name=="dsticket-jwks")].mountPath}{"\t"}{.spec.template.spec.containers[*].volumeMounts[?(@.name=="dsticket-jwks")].readOnly}{"\t"}{.spec.template.spec.volumes[?(@.name=="dsticket-jwks")].configMap.name}{"\t"}{.spec.template.spec.volumes[?(@.name=="dsticket-jwks")].configMap.defaultMode}{"\n"}'
        $lines = @(& kubectl create --dry-run=client --validate=false -f $tmp -o "jsonpath=$jsonPath" 2>&1)
        if ($LASTEXITCODE -ne 0) {
            throw "kubectl client parse 失败:$($lines -join [Environment]::NewLine)"
        }
        return @($lines | ForEach-Object { $_.ToString() })
    } finally {
        Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue
    }
}

function Assert-ServicesDSTicketSecretContract([string]$Manifest) {
    $deployments = @{}
    foreach ($row in (Get-ServicesContractRows -Manifest $Manifest)) {
        if ([string]::IsNullOrWhiteSpace($row)) { continue }
        $fields = @([regex]::Split($row, "`t"))
        if ($fields.Count -ne 16) { throw "services contract 列数=$($fields.Count)，应为 16:$row" }
        if ($fields[0] -cne 'Deployment') { continue }
        $name = [string]$fields[1]
        if ($deployments.ContainsKey($name)) { throw "重复 Deployment/$name。" }
        $deployments[$name] = $fields
    }

    Assert-True ($deployments.Count -eq $AllServices.Count) "Deployment 数=$($deployments.Count)，应为 $($AllServices.Count)"
    foreach ($service in $AllServices) {
        Assert-True ($deployments.ContainsKey($service)) "缺 Deployment/$service"
        $fields = $deployments[$service]
        if ($SignerServices -contains $service) {
            Assert-True ([string]$fields[2] -ceq 'true') "$service runAsNonRoot 必须为 true"
            Assert-True ([string]$fields[3] -ceq '10001') "$service runAsUser 必须为 10001"
            Assert-True ([string]$fields[4] -ceq '10001') "$service runAsGroup 必须为 10001"
            Assert-True ([string]$fields[5] -ceq '10001') "$service fsGroup 必须为 10001"
            Assert-True ([string]$fields[6] -ceq 'OnRootMismatch') "$service fsGroupChangePolicy 必须为 OnRootMismatch"
            Assert-True ([string]$fields[7] -ceq $service) "$service 主容器名漂移:$($fields[7])"
            Assert-True ([string]$fields[8] -ceq '/run/secrets/pandora-dsticket') "$service 私钥挂载路径错误:$($fields[8])"
            Assert-True ([string]$fields[9] -ceq 'true') "$service 私钥卷必须 readOnly"
            Assert-True ([string]$fields[10] -ceq 'pandora-dsticket-signer-r1') "$service 私钥 Secret 名错误:$($fields[10])"
            # Kubernetes JSON 输出把 YAML 0440 解析为十进制 288。
            Assert-True ([string]$fields[11] -ceq '288') "$service private.pem 模式必须为 0440(288)，实际=$($fields[11])"
        } else {
            foreach ($index in 8..11) {
                Assert-True ([string]::IsNullOrWhiteSpace([string]$fields[$index])) "$service 非签发方不得挂载 dsticket 私钥"
            }
        }
        if ($service -ceq 'login') {
            Assert-True ([string]$fields[12] -ceq '/run/config/pandora-dsticket') 'login JWKS 挂载路径错误'
            Assert-True ([string]$fields[13] -ceq 'true') 'login JWKS 卷必须 readOnly'
            Assert-True ([string]$fields[14] -ceq 'pandora-dsticket-jwks-r1') 'login 必须挂载 revisioned JWKS ConfigMap'
            Assert-True ([string]$fields[15] -ceq '292') 'login JWKS 文件模式必须为 0444(292)'
        } else {
            foreach ($index in 12..15) {
                Assert-True ([string]::IsNullOrWhiteSpace([string]$fields[$index])) "$service 不得误挂 Login-only JWKS 诊断卷"
            }
        }
    }
}

$renderedLines = @(& kubectl kustomize $ServicesKustomizeDir 2>&1)
if ($LASTEXITCODE -ne 0) { throw "kubectl kustomize services 失败:$($renderedLines -join [Environment]::NewLine)" }
$manifest = ($renderedLines | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine

Assert-ServicesDSTicketSecretContract -Manifest $manifest
Assert-Throws {
    Assert-ServicesDSTicketSecretContract -Manifest ($manifest.Replace('fsGroup: 10001', 'fsGroup: 10002'))
} '签发方 fsGroup 漂移必须阻断'
Assert-Throws {
    Assert-ServicesDSTicketSecretContract -Manifest ($manifest.Replace('defaultMode: 288', 'defaultMode: 256'))
} '签发私钥回退为 0400 必须阻断'
Assert-Throws {
    Assert-ServicesDSTicketSecretContract -Manifest ($manifest.Replace('pandora-dsticket-jwks-r1', 'pandora-dsticket-jwks-r2'))
} 'Login JWKS revision 漂移必须阻断'
Assert-Throws {
    Assert-ServicesDSTicketSecretContract -Manifest ($manifest.Replace('pandora-dsticket-signer-r1', 'pandora-dsticket'))
} '签发私钥回退 legacy 非 revisioned Secret 必须阻断'
Assert-Throws {
    Assert-ServicesDSTicketSecretContract -Manifest ($manifest.Replace('pandora-dsticket-signer-r1', 'pandora-dsticket-signer-r2'))
} 'base signer/JWKS revision 分裂必须阻断'

$matchmakerSource = Get-Content -LiteralPath (Join-Path $ProjectRoot 'services/matchmaking/matchmaker/cmd/matchmaker/main.go') -Raw
Assert-True ($matchmakerSource.Contains('ds_allocator_requires_ds_ticket_v2')) `
    '真实 ds_allocator 链必须 fail-closed 要求 Model-B RS256 signer'
# 2026-08-11 修正死契约:本条原先是**字面量禁用** `-not $matchmakerSource.Contains('legacySigner')`,
# 写于 3ba27c3c(2026-07-13);而 2f369c22(2026-08-04)引入了 Windows 本机联调档
# `match.ds_local_profile=local-off-v1` —— 它**显式声明**、与 v2 私钥互斥、构造时打 WARN,
# 是被设计文档认可的例外(hub_allocator / ds_allocator 在 mode=local 下只接受 legacy,
# 强签 RS256 会让 DS 把每个玩家拒在 PreLogin)。字面量禁用于是恒红三周,
# 而它真正要守的性质其实一直成立。
#
# 要守的从来不是"这个标识符不许出现",而是**不得静默回退**:legacy signer 只能在显式
# 声明本机档时构造,且未声明时必须 fail-closed 退出。故改为断言这三件结构事实。
$legacyAssignments = [regex]::Matches($matchmakerSource, '(?m)^\s*legacySigner\s*=\s*s\s*$').Count
Assert-True ($legacyAssignments -eq 1) `
    "matchmaker 只允许有一处 legacy signer 赋值(实为 $legacyAssignments 处);多处 = 存在未经审视的第二条回退路径"
Assert-True ($matchmakerSource -match '(?s)case\s+cfg\.Match\.DSLocalProfile\s*==\s*auth\.DSLocalProfileOffV1:.*?legacySigner\s*=\s*s') `
    'legacy HS256 signer 只能在显式声明 match.ds_local_profile=local-off-v1 的分支里构造'
Assert-True ($matchmakerSource -match '(?s)legacySigner\s*=\s*s.*?default:.*?ds_allocator_requires_ds_ticket_v2') `
    '未声明本机档时必须走 default 分支 fail-closed 退出,不得静默回退 legacy'
Assert-True ($matchmakerSource.Contains('ds_ticket_profile_conflict')) `
    'v2 私钥与本机档必须互斥拒启(同时配置 = 姿态自相矛盾,不得靠优先级猜)'
# 2026-08-11 同一处死契约:原断言写死实参字面量 `..., nil, v2Signer`(3ba27c3c,2026-07-13),
# 而 2f369c22(2026-08-04)把第二个实参从字面 nil 改成变量 legacySigner —— 在生产档它**就是**
# nil(只在 local-off-v1 分支被赋值,default 分支 fail-closed 退出,上面三条断言已钉死),
# 语义完全等价,断言却因为比对字面量而恒红。
# 改为断言**注入顺序与两个 signer 都到位**:v2Signer 必须被注入真实分配器,
# 且 legacy 位在 v2 位之前(顺序写反 = 把 HS256 票当 RS256 用)。
Assert-True ($matchmakerSource -match 'NewGrpcDSAllocator\(cfg\.Match\.DSAllocatorAddr,\s*(?:nil|legacySigner),\s*v2Signer,') `
    'matchmaker 必须按 (legacy, v2) 顺序把 RS256 signer 注入真实 DS 分配器'
$dsAuthSource = Get-Content -LiteralPath (Join-Path $ProjectRoot 'pkg/middleware/dsauth.go') -Raw
Assert-True ($dsAuthSource.Contains('(*auth.DSCallbackSigner, error)') -and
    $dsAuthSource.Contains('(*auth.DSCallbackVerifier, error)') -and
    $dsAuthSource.Contains('auth.NewDSCallbackSigner') -and $dsAuthSource.Contains('auth.NewDSCallbackVerifier')) `
    'DS callback 配置工厂必须只暴露专用信任域类型'
Assert-True (-not $dsAuthSource.Contains('func NewDSCallbackSignerFromConf(cfg config.DSAuthConf) (*auth.Signer')) `
    'DS callback 工厂不得回退宽泛玩家令牌 Signer 类型'

Write-Host 'services_dsticket_secret_contract_test: PASS' -ForegroundColor Green
