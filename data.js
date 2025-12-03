window.BENCHMARK_DATA = {
  "lastUpdate": 1764768628647,
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
          "id": "a39287b7d5c094c258f91c11a8b87ac489c7f897",
          "message": "Fix adjacency bug",
          "timestamp": "2025-12-02T21:17:16Z",
          "tree_id": "b413ceffe2a9fffef7969f1283f40822215944d2",
          "url": "https://github.com/gingermike/pytemporal/commit/a39287b7d5c094c258f91c11a8b87ac489c7f897"
        },
        "date": 1764711303308,
        "tool": "cargo",
        "benches": [
          {
            "name": "small_dataset",
            "value": 85323,
            "range": "± 400",
            "unit": "ns/iter"
          },
          {
            "name": "medium_dataset",
            "value": 367006,
            "range": "± 4756",
            "unit": "ns/iter"
          },
          {
            "name": "conflation_effectiveness",
            "value": 66669,
            "range": "± 262",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/10",
            "value": 43209,
            "range": "± 101",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/50",
            "value": 188292,
            "range": "± 563",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/100",
            "value": 368030,
            "range": "± 1190",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/500",
            "value": 1308398,
            "range": "± 6293",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/500000",
            "value": 1685136066,
            "range": "± 90461678",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_effectiveness/scenario/few_ids_many_records",
            "value": 6534342,
            "range": "± 210719",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_effectiveness/scenario/many_ids_few_records",
            "value": 24615195,
            "range": "± 583156",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_effectiveness/scenario/balanced_workload",
            "value": 6549134,
            "range": "± 36923",
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
          "id": "134c4a0eb20b20278f6db977c7a567d09c56af9d",
          "message": "Stop inserts when update is not extending beyond eff to",
          "timestamp": "2025-12-02T22:46:39Z",
          "tree_id": "2250002faf6c50334d07fed71c5ebd918a1ba5d5",
          "url": "https://github.com/gingermike/pytemporal/commit/134c4a0eb20b20278f6db977c7a567d09c56af9d"
        },
        "date": 1764716694913,
        "tool": "cargo",
        "benches": [
          {
            "name": "small_dataset",
            "value": 85852,
            "range": "± 339",
            "unit": "ns/iter"
          },
          {
            "name": "medium_dataset",
            "value": 374466,
            "range": "± 1057",
            "unit": "ns/iter"
          },
          {
            "name": "conflation_effectiveness",
            "value": 67873,
            "range": "± 177",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/10",
            "value": 44142,
            "range": "± 96",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/50",
            "value": 192376,
            "range": "± 553",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/100",
            "value": 376182,
            "range": "± 707",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/500",
            "value": 1347498,
            "range": "± 7558",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/500000",
            "value": 1784650074,
            "range": "± 91680022",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_effectiveness/scenario/few_ids_many_records",
            "value": 6688544,
            "range": "± 209102",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_effectiveness/scenario/many_ids_few_records",
            "value": 27246580,
            "range": "± 940185",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_effectiveness/scenario/balanced_workload",
            "value": 6728351,
            "range": "± 75057",
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
          "id": "f692f8161a7984cc0a8ffce901bda4b7099eb7f6",
          "message": "Fix bug with full state extension of tombstone record",
          "timestamp": "2025-12-03T13:12:14Z",
          "tree_id": "15f70b5efd46cb007396f23544848a4e993a9fbd",
          "url": "https://github.com/gingermike/pytemporal/commit/f692f8161a7984cc0a8ffce901bda4b7099eb7f6"
        },
        "date": 1764768627806,
        "tool": "cargo",
        "benches": [
          {
            "name": "small_dataset",
            "value": 86328,
            "range": "± 293",
            "unit": "ns/iter"
          },
          {
            "name": "medium_dataset",
            "value": 376418,
            "range": "± 1874",
            "unit": "ns/iter"
          },
          {
            "name": "conflation_effectiveness",
            "value": 66979,
            "range": "± 1050",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/10",
            "value": 43745,
            "range": "± 81",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/50",
            "value": 191622,
            "range": "± 414",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/100",
            "value": 375223,
            "range": "± 551",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/500",
            "value": 1341035,
            "range": "± 8204",
            "unit": "ns/iter"
          },
          {
            "name": "scaling_by_dataset_size/records/500000",
            "value": 1729491284,
            "range": "± 138895913",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_effectiveness/scenario/few_ids_many_records",
            "value": 6535888,
            "range": "± 131449",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_effectiveness/scenario/many_ids_few_records",
            "value": 26670745,
            "range": "± 1037836",
            "unit": "ns/iter"
          },
          {
            "name": "parallel_effectiveness/scenario/balanced_workload",
            "value": 6613164,
            "range": "± 71454",
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
        "date": 1764630974287,
        "tool": "pytest",
        "benches": [
          {
            "name": "benches/test_python_benchmarks.py::TestSmallDataset::test_small_dataset",
            "value": 96.67957761215949,
            "unit": "iter/sec",
            "range": "stddev: 0.0002648805336555221",
            "extra": "mean: 10.343446099977882 msec\nrounds: 20"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestMediumDataset::test_medium_dataset",
            "value": 91.05733399646907,
            "unit": "iter/sec",
            "range": "stddev: 0.0001457123311264768",
            "extra": "mean: 10.982091788880805 msec\nrounds: 90"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[10]",
            "value": 96.98934599151066,
            "unit": "iter/sec",
            "range": "stddev: 0.0002032768165646955",
            "extra": "mean: 10.310410795919056 msec\nrounds: 98"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[50]",
            "value": 94.11534804467651,
            "unit": "iter/sec",
            "range": "stddev: 0.0003098799580645275",
            "extra": "mean: 10.625259543483816 msec\nrounds: 92"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[100]",
            "value": 88.91659468503288,
            "unit": "iter/sec",
            "range": "stddev: 0.002059854790496266",
            "extra": "mean: 11.246494577781302 msec\nrounds: 90"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[500]",
            "value": 81.05072023234163,
            "unit": "iter/sec",
            "range": "stddev: 0.00021772540780352274",
            "extra": "mean: 12.337953285712697 msec\nrounds: 70"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[500000]",
            "value": 0.49724310381607306,
            "unit": "iter/sec",
            "range": "stddev: 0.03953984741941284",
            "extra": "mean: 2.011088725666658 sec\nrounds: 3"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestParallelEffectiveness::test_parallel_scenarios[few_ids_many_records-10-1000]",
            "value": 37.2595490928221,
            "unit": "iter/sec",
            "range": "stddev: 0.0013822615866836358",
            "extra": "mean: 26.838757428566037 msec\nrounds: 7"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestParallelEffectiveness::test_parallel_scenarios[many_ids_few_records-1000-10]",
            "value": 21.60058676854242,
            "unit": "iter/sec",
            "range": "stddev: 0.000620500873683985",
            "extra": "mean: 46.29503868183478 msec\nrounds: 22"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestParallelEffectiveness::test_parallel_scenarios[balanced_workload-100-100]",
            "value": 37.95456354874587,
            "unit": "iter/sec",
            "range": "stddev: 0.0004428378607884813",
            "extra": "mean: 26.347292828586433 msec\nrounds: 35"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestWideDatasets::test_wide_dataset[1000-10]",
            "value": 44.637384398443835,
            "unit": "iter/sec",
            "range": "stddev: 0.00041916862952020315",
            "extra": "mean: 22.40274634090931 msec\nrounds: 44"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestWideDatasets::test_wide_dataset[5000-20]",
            "value": 10.483683641648106,
            "unit": "iter/sec",
            "range": "stddev: 0.0014761628984401901",
            "extra": "mean: 95.38631974999134 msec\nrounds: 12"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestWideDatasets::test_wide_dataset[10000-40]",
            "value": 3.6154901038443676,
            "unit": "iter/sec",
            "range": "stddev: 0.001580789066038317",
            "extra": "mean: 276.5876744999787 msec\nrounds: 4"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestWideDatasets::test_wide_dataset[50000-80]",
            "value": 0.4164490841088159,
            "unit": "iter/sec",
            "range": "stddev: 0.017710573338176314",
            "extra": "mean: 2.4012539303333065 sec\nrounds: 3"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestUpdateModes::test_delta_mode",
            "value": 89.04591912687106,
            "unit": "iter/sec",
            "range": "stddev: 0.00020386383582299064",
            "extra": "mean: 11.230160908050346 msec\nrounds: 87"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestUpdateModes::test_full_state_mode",
            "value": 118.07854248728272,
            "unit": "iter/sec",
            "range": "stddev: 0.00019620797528310338",
            "extra": "mean: 8.468939224141439 msec\nrounds: 116"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestConflationEffectiveness::test_conflation_effectiveness",
            "value": 94.82273810244104,
            "unit": "iter/sec",
            "range": "stddev: 0.00020641275007539085",
            "extra": "mean: 10.54599371428884 msec\nrounds: 91"
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
          "id": "a39287b7d5c094c258f91c11a8b87ac489c7f897",
          "message": "Fix adjacency bug",
          "timestamp": "2025-12-02T21:17:16Z",
          "tree_id": "b413ceffe2a9fffef7969f1283f40822215944d2",
          "url": "https://github.com/gingermike/pytemporal/commit/a39287b7d5c094c258f91c11a8b87ac489c7f897"
        },
        "date": 1764711305191,
        "tool": "pytest",
        "benches": [
          {
            "name": "benches/test_python_benchmarks.py::TestSmallDataset::test_small_dataset",
            "value": 99.5748724420237,
            "unit": "iter/sec",
            "range": "stddev: 0.00023600169861418195",
            "extra": "mean: 10.042694260866247 msec\nrounds: 23"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestMediumDataset::test_medium_dataset",
            "value": 94.19859357258937,
            "unit": "iter/sec",
            "range": "stddev: 0.00015641289037477736",
            "extra": "mean: 10.615869750002167 msec\nrounds: 92"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[10]",
            "value": 100.94062822214805,
            "unit": "iter/sec",
            "range": "stddev: 0.0001306384789966208",
            "extra": "mean: 9.906813714287775 msec\nrounds: 98"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[50]",
            "value": 98.97938561282281,
            "unit": "iter/sec",
            "range": "stddev: 0.00011742357996878336",
            "extra": "mean: 10.103113833335916 msec\nrounds: 96"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[100]",
            "value": 92.51095683108497,
            "unit": "iter/sec",
            "range": "stddev: 0.0019307588276921606",
            "extra": "mean: 10.80953039785214 msec\nrounds: 93"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[500]",
            "value": 83.73565889093081,
            "unit": "iter/sec",
            "range": "stddev: 0.00020861238712377312",
            "extra": "mean: 11.942343480004638 msec\nrounds: 75"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[500000]",
            "value": 0.5183606095069007,
            "unit": "iter/sec",
            "range": "stddev: 0.012641070907107163",
            "extra": "mean: 1.9291589323333558 sec\nrounds: 3"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestParallelEffectiveness::test_parallel_scenarios[few_ids_many_records-10-1000]",
            "value": 40.320985922296394,
            "unit": "iter/sec",
            "range": "stddev: 0.0012615420768177976",
            "extra": "mean: 24.800980857142868 msec\nrounds: 7"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestParallelEffectiveness::test_parallel_scenarios[many_ids_few_records-1000-10]",
            "value": 22.4256759934098,
            "unit": "iter/sec",
            "range": "stddev: 0.0014734203128546478",
            "extra": "mean: 44.591743869565775 msec\nrounds: 23"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestParallelEffectiveness::test_parallel_scenarios[balanced_workload-100-100]",
            "value": 38.97633193844802,
            "unit": "iter/sec",
            "range": "stddev: 0.000448929113172946",
            "extra": "mean: 25.656595945950333 msec\nrounds: 37"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestWideDatasets::test_wide_dataset[1000-10]",
            "value": 45.13656402535413,
            "unit": "iter/sec",
            "range": "stddev: 0.0005720186242806095",
            "extra": "mean: 22.154987239132325 msec\nrounds: 46"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestWideDatasets::test_wide_dataset[5000-20]",
            "value": 10.38762521754729,
            "unit": "iter/sec",
            "range": "stddev: 0.001313312251768141",
            "extra": "mean: 96.2683942727112 msec\nrounds: 11"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestWideDatasets::test_wide_dataset[10000-40]",
            "value": 3.558288906576506,
            "unit": "iter/sec",
            "range": "stddev: 0.006982254737913633",
            "extra": "mean: 281.0339537500113 msec\nrounds: 4"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestWideDatasets::test_wide_dataset[50000-80]",
            "value": 0.4194772336957308,
            "unit": "iter/sec",
            "range": "stddev: 0.019835274884937477",
            "extra": "mean: 2.3839196020000295 sec\nrounds: 3"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestUpdateModes::test_delta_mode",
            "value": 93.69076742816029,
            "unit": "iter/sec",
            "range": "stddev: 0.00021126422774921647",
            "extra": "mean: 10.673410277771229 msec\nrounds: 90"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestUpdateModes::test_full_state_mode",
            "value": 124.23665743210302,
            "unit": "iter/sec",
            "range": "stddev: 0.00016335611166861413",
            "extra": "mean: 8.049154095654202 msec\nrounds: 115"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestConflationEffectiveness::test_conflation_effectiveness",
            "value": 99.60610549936261,
            "unit": "iter/sec",
            "range": "stddev: 0.00018961209606873597",
            "extra": "mean: 10.039545216496785 msec\nrounds: 97"
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
          "id": "134c4a0eb20b20278f6db977c7a567d09c56af9d",
          "message": "Stop inserts when update is not extending beyond eff to",
          "timestamp": "2025-12-02T22:46:39Z",
          "tree_id": "2250002faf6c50334d07fed71c5ebd918a1ba5d5",
          "url": "https://github.com/gingermike/pytemporal/commit/134c4a0eb20b20278f6db977c7a567d09c56af9d"
        },
        "date": 1764716697767,
        "tool": "pytest",
        "benches": [
          {
            "name": "benches/test_python_benchmarks.py::TestSmallDataset::test_small_dataset",
            "value": 90.94947682832628,
            "unit": "iter/sec",
            "range": "stddev: 0.0005829294448681697",
            "extra": "mean: 10.995115473699451 msec\nrounds: 19"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestMediumDataset::test_medium_dataset",
            "value": 87.79077211564802,
            "unit": "iter/sec",
            "range": "stddev: 0.0004672493946419542",
            "extra": "mean: 11.390718818176992 msec\nrounds: 88"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[10]",
            "value": 94.3470886212858,
            "unit": "iter/sec",
            "range": "stddev: 0.00031487871656390624",
            "extra": "mean: 10.599161188895323 msec\nrounds: 90"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[50]",
            "value": 92.48727025993279,
            "unit": "iter/sec",
            "range": "stddev: 0.00035861936567373404",
            "extra": "mean: 10.812298786519799 msec\nrounds: 89"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[100]",
            "value": 88.82855161333423,
            "unit": "iter/sec",
            "range": "stddev: 0.0003267191355162638",
            "extra": "mean: 11.257641623528263 msec\nrounds: 85"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[500]",
            "value": 76.29528970931169,
            "unit": "iter/sec",
            "range": "stddev: 0.00036330255143247523",
            "extra": "mean: 13.106969038456274 msec\nrounds: 26"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestScalingBySize::test_scaling[500000]",
            "value": 0.47842969995253454,
            "unit": "iter/sec",
            "range": "stddev: 0.03454523187569915",
            "extra": "mean: 2.090171241666667 sec\nrounds: 3"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestParallelEffectiveness::test_parallel_scenarios[few_ids_many_records-10-1000]",
            "value": 37.85223682870736,
            "unit": "iter/sec",
            "range": "stddev: 0.001530457662923716",
            "extra": "mean: 26.418518000014046 msec\nrounds: 8"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestParallelEffectiveness::test_parallel_scenarios[many_ids_few_records-1000-10]",
            "value": 20.458745176600335,
            "unit": "iter/sec",
            "range": "stddev: 0.0017306913175981363",
            "extra": "mean: 48.87885309524011 msec\nrounds: 21"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestParallelEffectiveness::test_parallel_scenarios[balanced_workload-100-100]",
            "value": 36.652267782546744,
            "unit": "iter/sec",
            "range": "stddev: 0.0006584367485899285",
            "extra": "mean: 27.283441393937018 msec\nrounds: 33"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestWideDatasets::test_wide_dataset[1000-10]",
            "value": 40.42466713420712,
            "unit": "iter/sec",
            "range": "stddev: 0.0007354331964868846",
            "extra": "mean: 24.737371285707034 msec\nrounds: 42"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestWideDatasets::test_wide_dataset[5000-20]",
            "value": 9.760068597453596,
            "unit": "iter/sec",
            "range": "stddev: 0.0026979060917448464",
            "extra": "mean: 102.45829627272295 msec\nrounds: 11"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestWideDatasets::test_wide_dataset[10000-40]",
            "value": 3.462459361200644,
            "unit": "iter/sec",
            "range": "stddev: 0.0034103394166942465",
            "extra": "mean: 288.81205400003296 msec\nrounds: 4"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestWideDatasets::test_wide_dataset[50000-80]",
            "value": 0.4035162138708043,
            "unit": "iter/sec",
            "range": "stddev: 0.017657192302061014",
            "extra": "mean: 2.4782151636666945 sec\nrounds: 3"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestUpdateModes::test_delta_mode",
            "value": 85.81875774975111,
            "unit": "iter/sec",
            "range": "stddev: 0.0023798165254611237",
            "extra": "mean: 11.652464172413403 msec\nrounds: 87"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestUpdateModes::test_full_state_mode",
            "value": 119.20856905142007,
            "unit": "iter/sec",
            "range": "stddev: 0.0002007152595087739",
            "extra": "mean: 8.388658700941663 msec\nrounds: 107"
          },
          {
            "name": "benches/test_python_benchmarks.py::TestConflationEffectiveness::test_conflation_effectiveness",
            "value": 94.94655831959379,
            "unit": "iter/sec",
            "range": "stddev: 0.00023044878954541227",
            "extra": "mean: 10.532240638296349 msec\nrounds: 94"
          }
        ]
      }
    ]
  }
}