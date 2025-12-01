window.BENCHMARK_DATA = {
  "lastUpdate": 1764630973434,
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
      },
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
          "id": "ddd80bc2d2fe0255eb71454034fdaba3e77bca91",
          "message": "Chart fix, add all criterion reports to dashboard",
          "timestamp": "2025-12-01T13:17:58Z",
          "tree_id": "dfd455936e4f2c800a89372012e2d13fe6d8823f",
          "url": "https://github.com/gingermike/pytemporal/commit/ddd80bc2d2fe0255eb71454034fdaba3e77bca91"
        },
        "date": 1764596181161,
        "tool": "cargo",
        "benches": [
          {
            "name": "small_dataset",
            "value": 82301,
            "range": "± 445",
            "unit": "ns/iter"
          },
          {
            "name": "medium_dataset",
            "value": 361537,
            "range": "± 2063",
            "unit": "ns/iter"
          },
          {
            "name": "conflation_effectiveness",
            "value": 66078,
            "range": "± 253",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/10",
            "value": 42543,
            "range": "± 446",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/50",
            "value": 185877,
            "range": "± 406",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/100",
            "value": 358745,
            "range": "± 1069",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/500",
            "value": 1242595,
            "range": "± 31819",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/500000",
            "value": 1608728701,
            "range": "± 119810881",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_effectiveness/scenario/few_ids_many_records",
            "value": 6829811,
            "range": "± 328526",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_effectiveness/scenario/many_ids_few_records",
            "value": 28937821,
            "range": "± 1918631",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_effectiveness/scenario/balanced_workload",
            "value": 6766736,
            "range": "± 116240",
            "unit": "ns/iter"
          }
        ]
      },
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
          "id": "0972cb195c715f4e5680f6260abd19ec09ae88eb",
          "message": "Have a consistent y axis",
          "timestamp": "2025-12-01T15:43:22Z",
          "tree_id": "881a5938b7276a762d92ed16b91b78e4eb851ffc",
          "url": "https://github.com/gingermike/pytemporal/commit/0972cb195c715f4e5680f6260abd19ec09ae88eb"
        },
        "date": 1764604905636,
        "tool": "cargo",
        "benches": [
          {
            "name": "small_dataset",
            "value": 81636,
            "range": "± 274",
            "unit": "ns/iter"
          },
          {
            "name": "medium_dataset",
            "value": 356662,
            "range": "± 2904",
            "unit": "ns/iter"
          },
          {
            "name": "conflation_effectiveness",
            "value": 65070,
            "range": "± 961",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/10",
            "value": 42673,
            "range": "± 136",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/50",
            "value": 183731,
            "range": "± 605",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/100",
            "value": 356022,
            "range": "± 611",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/500",
            "value": 1232856,
            "range": "± 12244",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/500000",
            "value": 1712782811,
            "range": "± 98730988",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_effectiveness/scenario/few_ids_many_records",
            "value": 6724862,
            "range": "± 177378",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_effectiveness/scenario/many_ids_few_records",
            "value": 26094095,
            "range": "± 2113332",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_effectiveness/scenario/balanced_workload",
            "value": 6892484,
            "range": "± 141851",
            "unit": "ns/iter"
          }
        ]
      },
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
          "id": "3e37ff4c527fcfa6009ea78023dfee46959f7f5c",
          "message": "Ensure column orders are consistent",
          "timestamp": "2025-12-01T20:03:21Z",
          "tree_id": "23ca243c41c1d7051c2c917ef5ac4ce5c7906cbb",
          "url": "https://github.com/gingermike/pytemporal/commit/3e37ff4c527fcfa6009ea78023dfee46959f7f5c"
        },
        "date": 1764620514319,
        "tool": "cargo",
        "benches": [
          {
            "name": "small_dataset",
            "value": 82276,
            "range": "± 351",
            "unit": "ns/iter"
          },
          {
            "name": "medium_dataset",
            "value": 357800,
            "range": "± 2045",
            "unit": "ns/iter"
          },
          {
            "name": "conflation_effectiveness",
            "value": 65973,
            "range": "± 670",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/10",
            "value": 42692,
            "range": "± 144",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/50",
            "value": 184167,
            "range": "± 450",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/100",
            "value": 358315,
            "range": "± 1432",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/500",
            "value": 1278669,
            "range": "± 5332",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/500000",
            "value": 1562607510,
            "range": "± 72734925",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_effectiveness/scenario/few_ids_many_records",
            "value": 6685138,
            "range": "± 130106",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_effectiveness/scenario/many_ids_few_records",
            "value": 26466592,
            "range": "± 610273",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_effectiveness/scenario/balanced_workload",
            "value": 6901689,
            "range": "± 77240",
            "unit": "ns/iter"
          }
        ]
      },
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
          "id": "1f6749dec87440dfa41800cf0081589e93713885",
          "message": "Don't do schema validation, just reorder",
          "timestamp": "2025-12-01T20:24:56Z",
          "tree_id": "3f2ed04ee60fc3c31b2c35602fc5f13244676195",
          "url": "https://github.com/gingermike/pytemporal/commit/1f6749dec87440dfa41800cf0081589e93713885"
        },
        "date": 1764621788806,
        "tool": "cargo",
        "benches": [
          {
            "name": "small_dataset",
            "value": 86304,
            "range": "± 347",
            "unit": "ns/iter"
          },
          {
            "name": "medium_dataset",
            "value": 376524,
            "range": "± 1150",
            "unit": "ns/iter"
          },
          {
            "name": "conflation_effectiveness",
            "value": 74834,
            "range": "± 186",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/10",
            "value": 46244,
            "range": "± 836",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/50",
            "value": 194588,
            "range": "± 1755",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/100",
            "value": 375894,
            "range": "± 696",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/500",
            "value": 1507634,
            "range": "± 7514",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/500000",
            "value": 1903580561,
            "range": "± 56920210",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_effectiveness/scenario/few_ids_many_records",
            "value": 7006182,
            "range": "± 240137",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_effectiveness/scenario/many_ids_few_records",
            "value": 31400493,
            "range": "± 727865",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_effectiveness/scenario/balanced_workload",
            "value": 7400429,
            "range": "± 450846",
            "unit": "ns/iter"
          }
        ]
      },
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
          "id": "46125034726f16660dce4a8fecdadf2dbe896b5a",
          "message": "Ensure empty ranges can't be emitted and fix tests",
          "timestamp": "2025-12-01T22:58:38Z",
          "tree_id": "edc2a6e9feea2e21381b4f1ab8bfd5f038925d19",
          "url": "https://github.com/gingermike/pytemporal/commit/46125034726f16660dce4a8fecdadf2dbe896b5a"
        },
        "date": 1764630971596,
        "tool": "cargo",
        "benches": [
          {
            "name": "small_dataset",
            "value": 85137,
            "range": "± 383",
            "unit": "ns/iter"
          },
          {
            "name": "medium_dataset",
            "value": 375236,
            "range": "± 1059",
            "unit": "ns/iter"
          },
          {
            "name": "conflation_effectiveness",
            "value": 66812,
            "range": "± 187",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/10",
            "value": 43898,
            "range": "± 81",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/50",
            "value": 192101,
            "range": "± 429",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/100",
            "value": 375719,
            "range": "± 924",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/500",
            "value": 1313412,
            "range": "± 10471",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/500000",
            "value": 1724327292,
            "range": "± 72386114",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_effectiveness/scenario/few_ids_many_records",
            "value": 6863265,
            "range": "± 219275",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_effectiveness/scenario/many_ids_few_records",
            "value": 25685057,
            "range": "± 593159",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_effectiveness/scenario/balanced_workload",
            "value": 6733492,
            "range": "± 57032",
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
      },
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
          "id": "ddd80bc2d2fe0255eb71454034fdaba3e77bca91",
          "message": "Chart fix, add all criterion reports to dashboard",
          "timestamp": "2025-12-01T13:17:58Z",
          "tree_id": "dfd455936e4f2c800a89372012e2d13fe6d8823f",
          "url": "https://github.com/gingermike/pytemporal/commit/ddd80bc2d2fe0255eb71454034fdaba3e77bca91"
        },
        "date": 1764596182320,
        "tool": "pytest",
        "benches": [
          {
            "name": "benches/test_python_benchmarks.py::TestSmallDataset::test_small_dataset",
            "value": 95.81220612492176,
            "unit": "iter/sec",
            "range": "stddev: 0.0005583214540006801",
            "extra": "mean: 10.43708354545329 msec\nrounds: 22"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestMediumDataset::test_medium_dataset",
            "value": 89.59212494284488,
            "unit": "iter/sec",
            "range": "stddev: 0.0006830421522446645",
            "extra": "mean: 11.161695301210324 msec\nrounds: 83"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[10]",
            "value": 100.63372393816763,
            "unit": "iter/sec",
            "range": "stddev: 0.0004479691140499613",
            "extra": "mean: 9.937026683166668 msec\nrounds: 101"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[50]",
            "value": 90.18224885125967,
            "unit": "iter/sec",
            "range": "stddev: 0.0019181407473025517",
            "extra": "mean: 11.088656722780671 msec\nrounds: 101"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[100]",
            "value": 97.08030492522546,
            "unit": "iter/sec",
            "range": "stddev: 0.0003871713596806262",
            "extra": "mean: 10.30075050516409 msec\nrounds: 97"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[500]",
            "value": 85.52962838911306,
            "unit": "iter/sec",
            "range": "stddev: 0.000828748672261457",
            "extra": "mean: 11.691854844154665 msec\nrounds: 77"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[500000]",
            "value": 0.5102102300997837,
            "unit": "iter/sec",
            "range": "stddev: 0.03902373635354158",
            "extra": "mean: 1.9599763803333115 sec\nrounds: 3"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestParallelEffectiveness::test_parallel_scenarios[few_ids_many_records-10-1000]",
            "value": 39.18547613839162,
            "unit": "iter/sec",
            "range": "stddev: 0.0005141629459940149",
            "extra": "mean: 25.519659285708126 msec\nrounds: 7"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestParallelEffectiveness::test_parallel_scenarios[many_ids_few_records-1000-10]",
            "value": 19.006880062927014,
            "unit": "iter/sec",
            "range": "stddev: 0.011012341915788287",
            "extra": "mean: 52.61252750000267 msec\nrounds: 18"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestParallelEffectiveness::test_parallel_scenarios[balanced_workload-100-100]",
            "value": 38.747472797125674,
            "unit": "iter/sec",
            "range": "stddev: 0.0006084637382484249",
            "extra": "mean: 25.808134771416135 msec\nrounds: 35"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestWideDatasets::test_wide_dataset[1000-10]",
            "value": 46.130808172769314,
            "unit": "iter/sec",
            "range": "stddev: 0.003668599187153753",
            "extra": "mean: 21.677487119991383 msec\nrounds: 50"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestWideDatasets::test_wide_dataset[5000-20]",
            "value": 10.291711094153419,
            "unit": "iter/sec",
            "range": "stddev: 0.0016034438570351629",
            "extra": "mean: 97.16557245452475 msec\nrounds: 11"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestWideDatasets::test_wide_dataset[10000-40]",
            "value": 3.797790661866937,
            "unit": "iter/sec",
            "range": "stddev: 0.0027221529773522038",
            "extra": "mean: 263.31098500000394 msec\nrounds: 4"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestWideDatasets::test_wide_dataset[50000-80]",
            "value": 0.4227176923630072,
            "unit": "iter/sec",
            "range": "stddev: 0.029670860153479885",
            "extra": "mean: 2.3656450110000455 sec\nrounds: 3"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestUpdateModes::test_delta_mode",
            "value": 94.93123699188901,
            "unit": "iter/sec",
            "range": "stddev: 0.00048053311067695583",
            "extra": "mean: 10.533940478258392 msec\nrounds: 92"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestUpdateModes::test_full_state_mode",
            "value": 127.83971847339572,
            "unit": "iter/sec",
            "range": "stddev: 0.0004684903171016819",
            "extra": "mean: 7.822295073405583 msec\nrounds: 109"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestConflationEffectiveness::test_conflation_effectiveness",
            "value": 103.05801308024421,
            "unit": "iter/sec",
            "range": "stddev: 0.0005017210695856828",
            "extra": "mean: 9.703272653057736 msec\nrounds: 98"
          }
        ]
      },
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
          "id": "0972cb195c715f4e5680f6260abd19ec09ae88eb",
          "message": "Have a consistent y axis",
          "timestamp": "2025-12-01T15:43:22Z",
          "tree_id": "881a5938b7276a762d92ed16b91b78e4eb851ffc",
          "url": "https://github.com/gingermike/pytemporal/commit/0972cb195c715f4e5680f6260abd19ec09ae88eb"
        },
        "date": 1764604906870,
        "tool": "pytest",
        "benches": [
          {
            "name": "benches/test_python_benchmarks.py::TestSmallDataset::test_small_dataset",
            "value": 102.62247859048286,
            "unit": "iter/sec",
            "range": "stddev: 0.000226442575905764",
            "extra": "mean: 9.744453785710252 msec\nrounds: 14"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestMediumDataset::test_medium_dataset",
            "value": 95.86159964904802,
            "unit": "iter/sec",
            "range": "stddev: 0.00031694997899541313",
            "extra": "mean: 10.431705747254664 msec\nrounds: 91"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[10]",
            "value": 103.97964638931184,
            "unit": "iter/sec",
            "range": "stddev: 0.000277372752915284",
            "extra": "mean: 9.617266789462663 msec\nrounds: 95"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[50]",
            "value": 100.79824813728058,
            "unit": "iter/sec",
            "range": "stddev: 0.00030083535971733014",
            "extra": "mean: 9.920807340203629 msec\nrounds: 97"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[100]",
            "value": 96.23753788692186,
            "unit": "iter/sec",
            "range": "stddev: 0.00035746788732418465",
            "extra": "mean: 10.390955774190628 msec\nrounds: 93"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[500]",
            "value": 86.1324588968965,
            "unit": "iter/sec",
            "range": "stddev: 0.00041514767155763924",
            "extra": "mean: 11.610024987177416 msec\nrounds: 78"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[500000]",
            "value": 0.5044318523330427,
            "unit": "iter/sec",
            "range": "stddev: 0.02016308020814842",
            "extra": "mean: 1.9824283406666534 sec\nrounds: 3"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestParallelEffectiveness::test_parallel_scenarios[few_ids_many_records-10-1000]",
            "value": 41.58153799673886,
            "unit": "iter/sec",
            "range": "stddev: 0.0003061773958240499",
            "extra": "mean: 24.049134499989577 msec\nrounds: 8"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestParallelEffectiveness::test_parallel_scenarios[many_ids_few_records-1000-10]",
            "value": 21.25038571109646,
            "unit": "iter/sec",
            "range": "stddev: 0.0026932616239022",
            "extra": "mean: 47.05796937501342 msec\nrounds: 24"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestParallelEffectiveness::test_parallel_scenarios[balanced_workload-100-100]",
            "value": 38.393416645026555,
            "unit": "iter/sec",
            "range": "stddev: 0.0008418131443690671",
            "extra": "mean: 26.04613205554705 msec\nrounds: 36"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestWideDatasets::test_wide_dataset[1000-10]",
            "value": 47.48637014708717,
            "unit": "iter/sec",
            "range": "stddev: 0.0031712551625975188",
            "extra": "mean: 21.05867424489468 msec\nrounds: 49"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestWideDatasets::test_wide_dataset[5000-20]",
            "value": 10.106234643827651,
            "unit": "iter/sec",
            "range": "stddev: 0.0032738395003987955",
            "extra": "mean: 98.94882072728706 msec\nrounds: 11"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestWideDatasets::test_wide_dataset[10000-40]",
            "value": 3.7443090644356745,
            "unit": "iter/sec",
            "range": "stddev: 0.001527393334524048",
            "extra": "mean: 267.07197050004083 msec\nrounds: 4"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestWideDatasets::test_wide_dataset[50000-80]",
            "value": 0.42111370039435314,
            "unit": "iter/sec",
            "range": "stddev: 0.007080790454837307",
            "extra": "mean: 2.3746555836667085 sec\nrounds: 3"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestUpdateModes::test_delta_mode",
            "value": 98.36869558285113,
            "unit": "iter/sec",
            "range": "stddev: 0.00018362573875713422",
            "extra": "mean: 10.165835727258871 msec\nrounds: 11"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestUpdateModes::test_full_state_mode",
            "value": 132.0657217918984,
            "unit": "iter/sec",
            "range": "stddev: 0.00016199260531413838",
            "extra": "mean: 7.571987540989197 msec\nrounds: 122"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestConflationEffectiveness::test_conflation_effectiveness",
            "value": 102.40460355632256,
            "unit": "iter/sec",
            "range": "stddev: 0.0002651934739238306",
            "extra": "mean: 9.765185990393487 msec\nrounds: 104"
          }
        ]
      },
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
          "id": "3e37ff4c527fcfa6009ea78023dfee46959f7f5c",
          "message": "Ensure column orders are consistent",
          "timestamp": "2025-12-01T20:03:21Z",
          "tree_id": "23ca243c41c1d7051c2c917ef5ac4ce5c7906cbb",
          "url": "https://github.com/gingermike/pytemporal/commit/3e37ff4c527fcfa6009ea78023dfee46959f7f5c"
        },
        "date": 1764620516020,
        "tool": "pytest",
        "benches": [
          {
            "name": "benches/test_python_benchmarks.py::TestSmallDataset::test_small_dataset",
            "value": 102.33315334743669,
            "unit": "iter/sec",
            "range": "stddev: 0.000286427726686983",
            "extra": "mean: 9.772004157878799 msec\nrounds: 19"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestMediumDataset::test_medium_dataset",
            "value": 95.75992308251375,
            "unit": "iter/sec",
            "range": "stddev: 0.0002606225141934153",
            "extra": "mean: 10.442781988643901 msec\nrounds: 88"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[10]",
            "value": 103.9685064287305,
            "unit": "iter/sec",
            "range": "stddev: 0.00015778374564679934",
            "extra": "mean: 9.618297255097064 msec\nrounds: 98"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[50]",
            "value": 100.32839788099061,
            "unit": "iter/sec",
            "range": "stddev: 0.00023572140361727075",
            "extra": "mean: 9.967267704067181 msec\nrounds: 98"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[100]",
            "value": 95.43136578396016,
            "unit": "iter/sec",
            "range": "stddev: 0.0002862235217349958",
            "extra": "mean: 10.478735076094628 msec\nrounds: 92"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[500]",
            "value": 84.75922906246652,
            "unit": "iter/sec",
            "range": "stddev: 0.00023634683290990757",
            "extra": "mean: 11.798125243246519 msec\nrounds: 74"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[500000]",
            "value": 0.5015043058799424,
            "unit": "iter/sec",
            "range": "stddev: 0.031186876655872716",
            "extra": "mean: 1.994000825666679 sec\nrounds: 3"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestParallelEffectiveness::test_parallel_scenarios[few_ids_many_records-10-1000]",
            "value": 38.94516324340634,
            "unit": "iter/sec",
            "range": "stddev: 0.0005219173274895474",
            "extra": "mean: 25.677129500010665 msec\nrounds: 8"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestParallelEffectiveness::test_parallel_scenarios[many_ids_few_records-1000-10]",
            "value": 21.874794124304607,
            "unit": "iter/sec",
            "range": "stddev: 0.0009314526551600142",
            "extra": "mean: 45.714715956522845 msec\nrounds: 23"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestParallelEffectiveness::test_parallel_scenarios[balanced_workload-100-100]",
            "value": 37.717666336663335,
            "unit": "iter/sec",
            "range": "stddev: 0.0003675355162097742",
            "extra": "mean: 26.512774970596556 msec\nrounds: 34"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestWideDatasets::test_wide_dataset[1000-10]",
            "value": 41.99995599522317,
            "unit": "iter/sec",
            "range": "stddev: 0.0009428737887315704",
            "extra": "mean: 23.80954875556856 msec\nrounds: 45"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestWideDatasets::test_wide_dataset[5000-20]",
            "value": 10.177559705118897,
            "unit": "iter/sec",
            "range": "stddev: 0.008279242338268199",
            "extra": "mean: 98.25538036362889 msec\nrounds: 11"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestWideDatasets::test_wide_dataset[10000-40]",
            "value": 3.7554830521996325,
            "unit": "iter/sec",
            "range": "stddev: 0.0028395027147142256",
            "extra": "mean: 266.2773300000083 msec\nrounds: 4"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestWideDatasets::test_wide_dataset[50000-80]",
            "value": 0.42405789800101035,
            "unit": "iter/sec",
            "range": "stddev: 0.0246718512814605",
            "extra": "mean: 2.3581685536667387 sec\nrounds: 3"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestUpdateModes::test_delta_mode",
            "value": 96.16455171823989,
            "unit": "iter/sec",
            "range": "stddev: 0.00026648672533604326",
            "extra": "mean: 10.398842215060482 msec\nrounds: 93"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestUpdateModes::test_full_state_mode",
            "value": 128.91165331189205,
            "unit": "iter/sec",
            "range": "stddev: 0.0002993112085178956",
            "extra": "mean: 7.757250599994828 msec\nrounds: 120"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestConflationEffectiveness::test_conflation_effectiveness",
            "value": 99.96992614707321,
            "unit": "iter/sec",
            "range": "stddev: 0.0011246534167767375",
            "extra": "mean: 10.00300829000139 msec\nrounds: 100"
          }
        ]
      },
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
          "id": "1f6749dec87440dfa41800cf0081589e93713885",
          "message": "Don't do schema validation, just reorder",
          "timestamp": "2025-12-01T20:24:56Z",
          "tree_id": "3f2ed04ee60fc3c31b2c35602fc5f13244676195",
          "url": "https://github.com/gingermike/pytemporal/commit/1f6749dec87440dfa41800cf0081589e93713885"
        },
        "date": 1764621791357,
        "tool": "pytest",
        "benches": [
          {
            "name": "benches/test_python_benchmarks.py::TestSmallDataset::test_small_dataset",
            "value": 104.79836491579479,
            "unit": "iter/sec",
            "range": "stddev: 0.0005489277859399563",
            "extra": "mean: 9.54213360870179 msec\nrounds: 23"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestMediumDataset::test_medium_dataset",
            "value": 97.62965247192581,
            "unit": "iter/sec",
            "range": "stddev: 0.0002997718761042093",
            "extra": "mean: 10.242789712762299 msec\nrounds: 94"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[10]",
            "value": 106.37276549191242,
            "unit": "iter/sec",
            "range": "stddev: 0.0010398400255172185",
            "extra": "mean: 9.400902527781236 msec\nrounds: 108"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[50]",
            "value": 102.69384612779649,
            "unit": "iter/sec",
            "range": "stddev: 0.0003276439234656186",
            "extra": "mean: 9.737681834952003 msec\nrounds: 103"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[100]",
            "value": 94.9281201385414,
            "unit": "iter/sec",
            "range": "stddev: 0.002242065276639242",
            "extra": "mean: 10.53428634782365 msec\nrounds: 92"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[500]",
            "value": 83.34425283825693,
            "unit": "iter/sec",
            "range": "stddev: 0.00033890517658258354",
            "extra": "mean: 11.998427797303103 msec\nrounds: 74"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[500000]",
            "value": 0.45102596228718544,
            "unit": "iter/sec",
            "range": "stddev: 0.02887946310364497",
            "extra": "mean: 2.2171672666667064 sec\nrounds: 3"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestParallelEffectiveness::test_parallel_scenarios[few_ids_many_records-10-1000]",
            "value": 39.01947340957853,
            "unit": "iter/sec",
            "range": "stddev: 0.0004358831536829946",
            "extra": "mean: 25.62822899999771 msec\nrounds: 7"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestParallelEffectiveness::test_parallel_scenarios[many_ids_few_records-1000-10]",
            "value": 20.15867207569335,
            "unit": "iter/sec",
            "range": "stddev: 0.0006951571663217312",
            "extra": "mean: 49.60644214287142 msec\nrounds: 21"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestParallelEffectiveness::test_parallel_scenarios[balanced_workload-100-100]",
            "value": 36.98683232760213,
            "unit": "iter/sec",
            "range": "stddev: 0.0004532574014974349",
            "extra": "mean: 27.036648911773145 msec\nrounds: 34"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestWideDatasets::test_wide_dataset[1000-10]",
            "value": 39.45216675060611,
            "unit": "iter/sec",
            "range": "stddev: 0.0005112285520484214",
            "extra": "mean: 25.347150292693033 msec\nrounds: 41"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestWideDatasets::test_wide_dataset[5000-20]",
            "value": 9.406312975088632,
            "unit": "iter/sec",
            "range": "stddev: 0.0033482322777977436",
            "extra": "mean: 106.31158060000416 msec\nrounds: 10"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestWideDatasets::test_wide_dataset[10000-40]",
            "value": 3.272382790478449,
            "unit": "iter/sec",
            "range": "stddev: 0.0019431847863815282",
            "extra": "mean: 305.5877212499922 msec\nrounds: 4"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestWideDatasets::test_wide_dataset[50000-80]",
            "value": 0.3702572323763315,
            "unit": "iter/sec",
            "range": "stddev: 0.06323758255005914",
            "extra": "mean: 2.700825027999978 sec\nrounds: 3"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestUpdateModes::test_delta_mode",
            "value": 90.65804914132387,
            "unit": "iter/sec",
            "range": "stddev: 0.008420425672818943",
            "extra": "mean: 11.030460168419602 msec\nrounds: 95"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestUpdateModes::test_full_state_mode",
            "value": 131.92561921123644,
            "unit": "iter/sec",
            "range": "stddev: 0.00022764661802231997",
            "extra": "mean: 7.580028852461338 msec\nrounds: 122"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestConflationEffectiveness::test_conflation_effectiveness",
            "value": 105.82867750864027,
            "unit": "iter/sec",
            "range": "stddev: 0.0003096556810015601",
            "extra": "mean: 9.449234588784842 msec\nrounds: 107"
          }
        ]
      }
    ]
  }
}