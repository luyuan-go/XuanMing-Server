// Pandora 后端【dev 快照轨】流水线:每次提交自动 → 全模块 build/test → 快照镜像发布到 snapshots\。
// 无版本号(git sha 命名),激进清理。发布正式版走 Jenkinsfile.release(手动 + 语义版本)。
// 与客户端 Tool/Build/Jenkinsfile(dev 快照)共用制品根,分仓 snapshots\ / releases\;
// 布局与两轨规则见 docs/design/release-pipeline.md。
pipeline {
    // 构建机需具备:Go 1.26.5(宿主交叉编译)、Docker Desktop、pwsh、git。
    agent { label 'windows' }

    options {
        timestamps()
        disableConcurrentBuilds()
        buildDiscarder(logRotator(numToKeepStr: '30'))
    }

    triggers {
        // 哈希错峰轮询,与客户端流水线一致。
        pollSCM('H/5 * * * *')
    }

    parameters {
        booleanParam(
            // 测试全绿后是否构建并发布业务镜像离线包(publish_offline_images.ps1)。
            name: 'PUBLISH_IMAGES',
            defaultValue: true,
            description: 'Build the 21 business images and publish the offline tar to the artifact directory.'
        )
        string(
            name: 'ARTIFACT_ROOT_OVERRIDE',
            defaultValue: '',
            trim: true,
            description: 'Optional artifact root override. Empty preserves the agent PANDORA_ARTIFACT_ROOT and then falls back to F:\\work\\artifacts.'
        )
        booleanParam(
            // 离线 tar 是给无网机器 docker load 用的,k8s 拉不了 —— 不推 registry 则 dev 集群
            // 永远拉不到本次构建的镜像。只推本机 dev registry;线上发布【不走这里】,见下方 stage 注释。
            name: 'PUSH_REGISTRY',
            defaultValue: true,
            description: 'Also push the business images to the local dev registry so the dev k8s cluster can pull them. Dev only - production releases go through start.ps1 -Mode online (digest-pinned).'
        )
        string(
            name: 'REGISTRY_HOST',
            defaultValue: 'localhost:5000',
            trim: true,
            description: 'Target registry for PUSH_REGISTRY. Must be a local dev registry (localhost / 127.0.0.1); the publish script rejects remote hosts by design.'
        )
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Build & Test') {
            steps {
                bat 'pwsh -NoProfile -ExecutionPolicy Bypass -File tools\\scripts\\ci_db.ps1 -Action Up -StateFile run\\ci-db-state.json -RunId "%BUILD_TAG%"'
                bat 'pwsh -NoProfile -ExecutionPolicy Bypass -File tools\\scripts\\ci_backend.ps1 -RequireDbTests -CiDbStateFile run\\ci-db-state.json'
            }
        }

        stage('Publish Offline Images') {
            when {
                expression { return params.PUBLISH_IMAGES }
            }
            steps {
                script {
                    // 发布脚本自带:clean 工作区强制、git sha 版本戳、不可变 + 原子发布、
                    // 同 sha 已发布则 -SkipIfExists 幂等跳过。
                    def publishEnv = []
                    def artifactRootOverride = params.ARTIFACT_ROOT_OVERRIDE?.trim()
                    if (artifactRootOverride) {
                        publishEnv << "PANDORA_ARTIFACT_ROOT=${artifactRootOverride}"
                    }
                    // -PushRegistry 只推本机 dev registry:离线 tar 是给无网机器 docker load 的,
                    // k8s 拉不了。线上发布【不走这条链】—— start.ps1 -Mode online 的 -BuildPush 因
                    // TOCTOU 主动阻断("预检 tag 不存在"证明不了不可变,须 registry 原生 immutable-tag
                    // 策略),线上按 digest 部署。publish_offline_images.ps1 内有护栏拒绝非本机 registry。
                    def publishCmd = 'pwsh -NoProfile -ExecutionPolicy Bypass -File tools\\scripts\\publish_offline_images.ps1 -SkipIfExists'
                    if (params.PUSH_REGISTRY) {
                        publishCmd += " -PushRegistry -RegistryHost ${params.REGISTRY_HOST.trim()}"
                    }
                    if (publishEnv) {
                        withEnv(publishEnv) {
                            bat publishCmd
                        }
                    } else {
                        bat publishCmd
                    }
                }
            }
        }
    }

    post {
        always {
            // 只按 ci_db.ps1 生成且带 pandora-ci-db- 前缀的本轮 project 清理；
            // 状态不存在时脚本明确 SKIP，不会碰本机 dev MySQL/TiDB。
            bat 'pwsh -NoProfile -ExecutionPolicy Bypass -File tools\\scripts\\ci_db.ps1 -Action Down -StateFile run\\ci-db-state.json'
        }
    }
}
