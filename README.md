# 用途
解决在对接Oracle RAC的时候. 使用传统的NAT端口映射和NAT都不能对接的情况.

```
###################################################
# 0      8       16            31
# +--------------+--------------+
# | Packet Length| Packet Chksm |
# +------+-------+--------------+   8 byte header
# | Type | Rsrvd | Header Chksm |
# +------+-------+--------------+
# |        P A Y L O A D        |
# +-----------------------------+
#
# 字段说明：
# Packet Length: 包长度字段
# Packet Chksm:  检测包
# Type:          包类型
# Rsrvd:         未使用
# Header Chksm:  检测头
###################################################
```
