# Microbenchmark for athena

需要调研性能优化效果的时候，可以使用[JMH](http://openjdk.java.net/projects/code-tools/jmh/)写一些测试量化效果

### quick start

参考`EleMetaParserBenchmark`, 继承`AbstractMicrobenchmark`， 任意编写方法并以`@Benchmark`标识，然后右键以单元测试的方式运行即可。