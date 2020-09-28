1.sparkstreaming 案例
数仓有行为表t_event_detail ,数据源是kafka,现模仿消费kafka 数据实现以下功能,要求3s刷新1次。
应用1: 实时消费kafka 数据，且将数据按字段1对1 保存到MySQL数据库中。
应用2: 实时消费kafka 数据，且求出每天、每个主题的每个事件累计被点击的次数,将结果保存到MySQL数据库中。
应用3: 实时消费kafka 数据，且求出每天用户的累计登录次数、注册次数,将结果保存到MySQL数据库中
2.sparksql 案例，读取hdfs数据，创建两个dataframe，用spark.sql("select * from t1 left join t2 on t1...")这种写法关联两个表，得到想要的结果集
3.sparkstreaming 采用direct 模式拉去kafka数据，但是偏移量不是spark 保存，实现自己手动保存，并将偏移量保存到MySQL中
