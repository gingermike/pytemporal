window.BENCHMARK_DATA = {
  "lastUpdate": 1764593464964,
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
    ]
  }
}