window.BENCHMARK_DATA = {
  "lastUpdate": 1764588995887,
  "repoUrl": "https://github.com/gingermike/pytemporal",
  "entries": {
    "Rust Benchmarks": [
      {
        "commit": {
          "author": {
            "email": "mikerobertlewis@gmail.com",
            "name": "Mike Lewis",
            "username": "gingermike"
          },
          "committer": {
            "email": "mikerobertlewis@gmail.com",
            "name": "Mike Lewis",
            "username": "gingermike"
          },
          "distinct": true,
          "id": "58833cabc045ccbadba4dedff4d916d2490f08cc",
          "message": "Fix build",
          "timestamp": "2025-12-01T11:19:28Z",
          "tree_id": "0b4372aac43b654a9133329838bf70620534b8cf",
          "url": "https://github.com/gingermike/pytemporal/commit/58833cabc045ccbadba4dedff4d916d2490f08cc"
        },
        "date": 1764588994338,
        "tool": "cargo",
        "benches": [
          {
            "name": "small_dataset",
            "value": 82027,
            "range": "± 278",
            "unit": "ns/iter"
          },
          {
            "name": "medium_dataset",
            "value": 355151,
            "range": "± 1078",
            "unit": "ns/iter"
          },
          {
            "name": "conflation_effectiveness",
            "value": 65895,
            "range": "± 223",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/10",
            "value": 42945,
            "range": "± 96",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/50",
            "value": 184427,
            "range": "± 1570",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/100",
            "value": 356829,
            "range": "± 2569",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/500",
            "value": 1254770,
            "range": "± 3043",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/500000",
            "value": 1579346253,
            "range": "± 45449446",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_effectiveness/scenario/few_ids_many_records",
            "value": 6517033,
            "range": "± 132735",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_effectiveness/scenario/many_ids_few_records",
            "value": 24400747,
            "range": "± 295704",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_effectiveness/scenario/balanced_workload",
            "value": 6468158,
            "range": "± 237663",
            "unit": "ns/iter"
          }
        ]
      }
    ],
    "Python Benchmarks": [
      {
        "commit": {
          "author": {
            "email": "mikerobertlewis@gmail.com",
            "name": "Mike Lewis",
            "username": "gingermike"
          },
          "committer": {
            "email": "mikerobertlewis@gmail.com",
            "name": "Mike Lewis",
            "username": "gingermike"
          },
          "distinct": true,
          "id": "58833cabc045ccbadba4dedff4d916d2490f08cc",
          "message": "Fix build",
          "timestamp": "2025-12-01T11:19:28Z",
          "tree_id": "0b4372aac43b654a9133329838bf70620534b8cf",
          "url": "https://github.com/gingermike/pytemporal/commit/58833cabc045ccbadba4dedff4d916d2490f08cc"
        },
        "date": 1764588995523,
        "tool": "pytest",
        "benches": [
          {
            "name": "benches/test_python_benchmarks.py::TestSmallDataset::test_small_dataset",
            "value": 105.20918690023682,
            "unit": "iter/sec",
            "range": "stddev: 0.00021214962028180752",
            "extra": "mean: 9.504873380955186 msec\nrounds: 21"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestMediumDataset::test_medium_dataset",
            "value": 99.2264941288072,
            "unit": "iter/sec",
            "range": "stddev: 0.00012945257133660013",
            "extra": "mean: 10.077953562502037 msec\nrounds: 96"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[10]",
            "value": 106.15939385687041,
            "unit": "iter/sec",
            "range": "stddev: 0.000153698348487759",
            "extra": "mean: 9.41979756730951 msec\nrounds: 104"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[50]",
            "value": 104.26844081080345,
            "unit": "iter/sec",
            "range": "stddev: 0.0001249571759575702",
            "extra": "mean: 9.590629650006122 msec\nrounds: 100"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[100]",
            "value": 99.264363135028,
            "unit": "iter/sec",
            "range": "stddev: 0.00012606747536480587",
            "extra": "mean: 10.074108858581132 msec\nrounds: 99"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[500]",
            "value": 88.61129848001389,
            "unit": "iter/sec",
            "range": "stddev: 0.0003033938030284076",
            "extra": "mean: 11.285242594944572 msec\nrounds: 79"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[500000]",
            "value": 0.5467576207640376,
            "unit": "iter/sec",
            "range": "stddev: 0.013754130868645813",
            "extra": "mean: 1.8289639906666555 sec\nrounds: 3"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestParallelEffectiveness::test_parallel_scenarios[few_ids_many_records-10-1000]",
            "value": 41.644517732468536,
            "unit": "iter/sec",
            "range": "stddev: 0.0003650264282270877",
            "extra": "mean: 24.012764571417783 msec\nrounds: 7"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestParallelEffectiveness::test_parallel_scenarios[many_ids_few_records-1000-10]",
            "value": 23.76908076387688,
            "unit": "iter/sec",
            "range": "stddev: 0.0015728728475857092",
            "extra": "mean: 42.07146292000289 msec\nrounds: 25"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestParallelEffectiveness::test_parallel_scenarios[balanced_workload-100-100]",
            "value": 40.55597832078294,
            "unit": "iter/sec",
            "range": "stddev: 0.00027547438879736615",
            "extra": "mean: 24.65727721053518 msec\nrounds: 38"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestWideDatasets::test_wide_dataset[1000-10]",
            "value": 45.63501485827791,
            "unit": "iter/sec",
            "range": "stddev: 0.0034705143820222247",
            "extra": "mean: 21.91299823404366 msec\nrounds: 47"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestWideDatasets::test_wide_dataset[5000-20]",
            "value": 11.084405893315353,
            "unit": "iter/sec",
            "range": "stddev: 0.0008552902207365632",
            "extra": "mean: 90.21683341667124 msec\nrounds: 12"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestWideDatasets::test_wide_dataset[10000-40]",
            "value": 3.8986255341878313,
            "unit": "iter/sec",
            "range": "stddev: 0.0019614163031455275",
            "extra": "mean: 256.5006541999992 msec\nrounds: 5"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestWideDatasets::test_wide_dataset[50000-80]",
            "value": 0.42731947310695656,
            "unit": "iter/sec",
            "range": "stddev: 0.044876563288467355",
            "extra": "mean: 2.3401695053333165 sec\nrounds: 3"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestUpdateModes::test_delta_mode",
            "value": 98.83131642622726,
            "unit": "iter/sec",
            "range": "stddev: 0.00015766247071686372",
            "extra": "mean: 10.118250329554712 msec\nrounds: 88"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestUpdateModes::test_full_state_mode",
            "value": 133.71205311996115,
            "unit": "iter/sec",
            "range": "stddev: 0.00013353574250001536",
            "extra": "mean: 7.478757349592409 msec\nrounds: 123"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestConflationEffectiveness::test_conflation_effectiveness",
            "value": 105.3358534391037,
            "unit": "iter/sec",
            "range": "stddev: 0.00013876182354645214",
            "extra": "mean: 9.493443754914043 msec\nrounds: 102"
          }
        ]
      }
    ]
  }
}