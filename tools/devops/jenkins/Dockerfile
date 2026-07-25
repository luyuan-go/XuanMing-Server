# Pandora Jenkins controller —— 在官方 LTS 上预装插件，保证「另一台机器」结果一致。
# 只做编排：真正的 UE cook / Go 编译在宿主机 agent 上跑（controller numExecutors=0）。
FROM jenkins/jenkins:lts

# 预装插件（离线一致性：插件版本随镜像固化，不在首启时联网现装）
COPY plugins.txt /usr/share/jenkins/ref/plugins.txt
RUN jenkins-plugin-cli --plugin-file /usr/share/jenkins/ref/plugins.txt --latest false
