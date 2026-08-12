<#
.SYNOPSIS
  锁定 Login 编号改名与 Player 段位分池的 protobuf expand 兼容形态。

.DESCRIPTION
  本测试只检查协议源码与 Go/C++ 生成物的直接契约。跨历史版本兼容性仍由
  `buf breaking` 负责；这里防止后续生成或手改再次删除旧 RPC/字段、复用旧编号，
  或只更新一种语言的生成物。
#>
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$ProjectRoot = (Resolve-Path "$PSScriptRoot/../../..").Path

function Assert-Pattern {
    param(
        [Parameter(Mandatory)][string]$Path,
        [Parameter(Mandatory)][string]$Pattern,
        [Parameter(Mandatory)][string]$Contract
    )

    $content = [System.IO.File]::ReadAllText((Join-Path $ProjectRoot $Path))
    if (-not [System.Text.RegularExpressions.Regex]::IsMatch(
        $content,
        $Pattern,
        [System.Text.RegularExpressions.RegexOptions]::Multiline
    )) {
        throw "协议兼容契约失败: $Contract ($Path)"
    }
}

# Login expand：26201c1 已发布 register_no #13 / GetRegisterNo；新名只能用新编号/RPC 扩展。
Assert-Pattern 'proto/pandora/login/v1/login.proto' 'rpc\s+GetRegisterNo\s*\(' '旧 GetRegisterNo RPC 必须保留'
Assert-Pattern 'proto/pandora/login/v1/login.proto' 'uint64\s+register_no\s*=\s*13\s*\[deprecated\s*=\s*true\]' 'register_no 必须保留在 #13'
Assert-Pattern 'proto/pandora/login/v1/login.proto' 'uint64\s+player_no\s*=\s*14\s*;' 'player_no 必须使用新编号 #14'

# Player expand：旧客户端仍读单值 mmr #4；新客户端读分池列表 #52。
Assert-Pattern 'proto/pandora/player/v1/player.proto' 'int32\s+mmr\s*=\s*4\s*\[deprecated\s*=\s*true\]' 'PlayerProfile.mmr #4 只能 deprecated，不能删除'
Assert-Pattern 'proto/pandora/player/v1/player.proto' 'repeated\s+PlayerRating\s+ratings\s*=\s*52\s*;' 'PlayerProfile.ratings 必须保持 #52'

# Go / C++ 生成物必须与同一协议形态同步。
Assert-Pattern 'proto/gen/go/pandora/login/v1/login.pb.go' 'protobuf:"varint,13,opt,name=register_no,json=registerNo,proto3"' 'Go LoginResponse.register_no #13'
Assert-Pattern 'proto/gen/go/pandora/login/v1/login.pb.go' 'protobuf:"varint,14,opt,name=player_no,json=playerNo,proto3"' 'Go LoginResponse.player_no #14'
Assert-Pattern 'proto/gen/go/pandora/player/v1/player.pb.go' 'protobuf:"varint,4,opt,name=mmr,proto3"' 'Go PlayerProfile.mmr #4'
Assert-Pattern 'proto/gen/go/pandora/player/v1/player.pb.go' 'protobuf:"bytes,52,rep,name=ratings,proto3"' 'Go PlayerProfile.ratings #52'
Assert-Pattern 'proto/gen/cpp/pandora/login/v1/login.pb.h' 'kRegisterNoFieldNumber\s*=\s*13' 'C++ LoginResponse.register_no #13'
Assert-Pattern 'proto/gen/cpp/pandora/login/v1/login.pb.h' 'kPlayerNoFieldNumber\s*=\s*14' 'C++ LoginResponse.player_no #14'
Assert-Pattern 'proto/gen/cpp/pandora/player/v1/player.pb.h' 'kMmrFieldNumber\s*=\s*4' 'C++ PlayerProfile.mmr #4'
Assert-Pattern 'proto/gen/cpp/pandora/player/v1/player.pb.h' 'kRatingsFieldNumber\s*=\s*52' 'C++ PlayerProfile.ratings #52'

Write-Host '[OK] proto expand compatibility contract' -ForegroundColor Green
