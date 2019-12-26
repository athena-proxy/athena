# 动机:

增加接口实现类,用于具体的检测代码的实现

# 代码解释:

`SlowDetectorInstance`为可直接实例化并根据jar包变化而直接替换掉的对于检测逻辑的入口类(implements `me.ele.jarch.athena.detector.Detector`)

`SlowDetector`为间隔的两个时间窗口的数据存储类

`DetectStrategy` 为具体的计算逻辑

# 具体实现

## 检测模式为

每隔10秒,创建一个SlowDetector对象,用于存储10秒内所有SQL Pattern和所有group的请求信息, 包括:

> SQLPattern的Server duration,count等

> group的count,队列拥堵数等

## 计算出结果
* SQL的qps,upper90,average duration,请求/结果的字节数,影响行数等信息 (`SQLSample`.java)
* group的qps,upper90,队列拥堵数等信息 (`DalGroupSample.java`)

## 检测策略
存在一些检测策略`DetectStrategy.java`,并通过每个策略算出一个该策略的分值

最后选取分数的最大值的策略告警(日志,ETrace等)
