window.BENCHMARK_DATA = {
  "lastUpdate": 1764593465847,
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
          "id": "3b2ed99cafdfd7c53bc0c7343276f507def05482",
          "message": "Publish to root, not use /bench slug",
          "timestamp": "2025-12-01T12:33:13Z",
          "tree_id": "b1515de7e5f4a266e4090988f8d8cb00b83ae64d",
          "url": "https://github.com/gingermike/pytemporal/commit/3b2ed99cafdfd7c53bc0c7343276f507def05482"
        },
        "date": 1764593464642,
        "tool": "cargo",
        "benches": [
          {
            "name": "small_dataset",
            "value": 81565,
            "range": "± 1497",
            "unit": "ns/iter"
          },
          {
            "name": "medium_dataset",
            "value": 356049,
            "range": "± 1075",
            "unit": "ns/iter"
          },
          {
            "name": "conflation_effectiveness",
            "value": 65398,
            "range": "± 289",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/10",
            "value": 42422,
            "range": "± 214",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/50",
            "value": 183157,
            "range": "± 506",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/100",
            "value": 356406,
            "range": "± 663",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/500",
            "value": 1229886,
            "range": "± 10099",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/500000",
            "value": 1662801985,
            "range": "± 83423164",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_effectiveness/scenario/few_ids_many_records",
            "value": 7015492,
            "range": "± 193406",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_effectiveness/scenario/many_ids_few_records",
            "value": 25679975,
            "range": "± 954111",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_effectiveness/scenario/balanced_workload",
            "value": 6717152,
            "range": "± 123906",
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
          "id": "3b2ed99cafdfd7c53bc0c7343276f507def05482",
          "message": "Publish to root, not use /bench slug",
          "timestamp": "2025-12-01T12:33:13Z",
          "tree_id": "b1515de7e5f4a266e4090988f8d8cb00b83ae64d",
          "url": "https://github.com/gingermike/pytemporal/commit/3b2ed99cafdfd7c53bc0c7343276f507def05482"
        },
        "date": 1764593465595,
        "tool": "pytest",
        "benches": [
          {
            "name": "benches/test_python_benchmarks.py::TestSmallDataset::test_small_dataset",
            "value": 103.98800714621956,
            "unit": "iter/sec",
            "range": "stddev: 0.0002440386408385757",
            "extra": "mean: 9.616493550009864 msec\nrounds: 20"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestMediumDataset::test_medium_dataset",
            "value": 96.65603585669443,
            "unit": "iter/sec",
            "range": "stddev: 0.0003649927995310251",
            "extra": "mean: 10.345965372329509 msec\nrounds: 94"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[10]",
            "value": 100.69329941535554,
            "unit": "iter/sec",
            "range": "stddev: 0.0003677936881121371",
            "extra": "mean: 9.931147413047247 msec\nrounds: 92"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[50]",
            "value": 101.70115856669373,
            "unit": "iter/sec",
            "range": "stddev: 0.0002554699688473669",
            "extra": "mean: 9.832729676763893 msec\nrounds: 99"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[100]",
            "value": 98.33024028445398,
            "unit": "iter/sec",
            "range": "stddev: 0.00023505576215148073",
            "extra": "mean: 10.169811414140316 msec\nrounds: 99"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[500]",
            "value": 87.30614244718285,
            "unit": "iter/sec",
            "range": "stddev: 0.00042539415557583943",
            "extra": "mean: 11.453947820509477 msec\nrounds: 78"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[500000]",
            "value": 0.5139481372608171,
            "unit": "iter/sec",
            "range": "stddev: 0.088929715729298",
            "extra": "mean: 1.9457216156666846 sec\nrounds: 3"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestParallelEffectiveness::test_parallel_scenarios[few_ids_many_records-10-1000]",
            "value": 38.94037780346467,
            "unit": "iter/sec",
            "range": "stddev: 0.0005000939791926164",
            "extra": "mean: 25.68028499998339 msec\nrounds: 9"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestParallelEffectiveness::test_parallel_scenarios[many_ids_few_records-1000-10]",
            "value": 22.393018456159016,
            "unit": "iter/sec",
            "range": "stddev: 0.0012373279912709961",
            "extra": "mean: 44.65677559091897 msec\nrounds: 22"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestParallelEffectiveness::test_parallel_scenarios[balanced_workload-100-100]",
            "value": 38.9967543326253,
            "unit": "iter/sec",
            "range": "stddev: 0.0005648758568129686",
            "extra": "mean: 25.643159722228066 msec\nrounds: 36"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestWideDatasets::test_wide_dataset[1000-10]",
            "value": 44.59528923600929,
            "unit": "iter/sec",
            "range": "stddev: 0.0008512972543691272",
            "extra": "mean: 22.423893131576136 msec\nrounds: 38"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestWideDatasets::test_wide_dataset[5000-20]",
            "value": 10.236858521592366,
            "unit": "iter/sec",
            "range": "stddev: 0.005079677030760803",
            "extra": "mean: 97.68621866666649 msec\nrounds: 12"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestWideDatasets::test_wide_dataset[10000-40]",
            "value": 3.6832760467115877,
            "unit": "iter/sec",
            "range": "stddev: 0.0018005632157591565",
            "extra": "mean: 271.49743525001213 msec\nrounds: 4"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestWideDatasets::test_wide_dataset[50000-80]",
            "value": 0.42610190228186157,
            "unit": "iter/sec",
            "range": "stddev: 0.015118777126018106",
            "extra": "mean: 2.346856455333333 sec\nrounds: 3"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestUpdateModes::test_delta_mode",
            "value": 96.6674468254902,
            "unit": "iter/sec",
            "range": "stddev: 0.00031138394282801036",
            "extra": "mean: 10.344744097827052 msec\nrounds: 92"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestUpdateModes::test_full_state_mode",
            "value": 132.61938364027685,
            "unit": "iter/sec",
            "range": "stddev: 0.0002130029656630964",
            "extra": "mean: 7.54037586777245 msec\nrounds: 121"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestConflationEffectiveness::test_conflation_effectiveness",
            "value": 100.55657436678207,
            "unit": "iter/sec",
            "range": "stddev: 0.00036486980535109417",
            "extra": "mean: 9.944650623762106 msec\nrounds: 101"
          }
        ]
      }
    ]
  }
}