package unit.benchmark.dataset

import org.apache.spark.secco.benchmark.dataset.SyntheticDatasets
import util.SeccoFunSuite

class SyntheticDatasetsSuite extends SeccoFunSuite {

  test("gen_WorkloadExpDatasets") {

    //write workloadExpDatasets to outputPath.
    val outputPath = "./datasets/workload_exp"
    SyntheticDatasets.workloadExpDatasets.genDatasets(
      workloadNames =
        Seq("W1", "W2", "W3", "W4", "W5", "W6", "W7", "W8", "W9", "W10"),
      outputPath = Some(outputPath)
    )
  }

  test("gen_WorkloadExpDatasets_demo") {

    //    write demo of workloadExpDatasets to outputPath of demo.
    val demoOutputPath = "./datasets/workload_exp_demo"

    SyntheticDatasets.workloadExpDatasets.genDatasets(
      isDemo = true,
      workloadNames =
        Seq("W1", "W2", "W3", "W4", "W5", "W6", "W7", "W8", "W9", "W10"),
      outputPath = Some(demoOutputPath)
    )
  }

}
