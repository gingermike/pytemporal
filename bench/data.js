window.BENCHMARK_DATA = {
  "lastUpdate": 1764588994843,
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
    ]
  }
}