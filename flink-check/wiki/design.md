# Design 

## Tenets

- Test subjects are functions that transform `DataStream` objects, that can 
be used in actual production code, not models of production code.
- During a test, test subjects are executed by the Flink runtime.
- Assertions can use the Flink API to trigger distributed computations. Even
though we won't validate test execution in a distributed setting in the first
iteration, the design is not impediment for supporting distributed computing 
in the future
- Parallel sources and operations are used in the tests.  
 
