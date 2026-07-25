# Pandora Jenkins controller —— 在官方 LTS 上预装插件，保证「另一台机器」结果一致。
# 只做编排：真正的 UE cook / Go 编译在宿主机 agent 上跑（controller numExecutors=0）。
FROM jenkins/jenkins:lts

# 预装插件（离线一致性：插件版本随镜像固化，不在首启时联网现装）
#
# ⚠️ 不要加 `--latest false`：实测它把插件钉在 LTS 基线版本后，
#    pipeline-model-definition → workflow-multibranch 这条依赖链解析不全，
#    结果是 73 个插件装上但声明式 Pipeline 缺失。后果极隐蔽——
#    Jenkinsfile 里的 `pipeline { }` 块不被识别，构建"成功"但一个 stage 都不执行
#    （控制台只有 Start of Pipeline / End of Pipeline / Finished: SUCCESS）。
#    默认（解析最新兼容版本）能把依赖树拉全。
COPY plugins.txt /usr/share/jenkins/ref/plugins.txt
RUN jenkins-plugin-cli --plugin-file /usr/share/jenkins/ref/plugins.txt
