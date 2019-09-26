# flinkExample

说明：
	本仓库主要用于存放flink相关的demo
—————————————————————————————————————

module：hotTopN2Hbase

	实现将kafka中的数据通过Flink处理后，写入到HBase中。其中，Flink处理数据的逻辑是参考了阿里吴翀博文《Flink 零基础实战教程：如何计算实时热门商品》一文。

说明：程序的正确性，待验证，近期会在集群上验证。