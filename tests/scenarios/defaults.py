from dataclasses import dataclass
from typing import Tuple, List, Callable, Literal

import pandas as pd

pd_max = pd.Timestamp.max
pdt_past = pd.Timestamp.now() - pd.Timedelta(hours=1)
pdt_now = pd.Timestamp.now()

default_id_columns = ["id", "field"]
default_value_columns = ["mv", "price"]
default_columns = (default_id_columns +
                   default_value_columns +
                   ["effective_from", "effective_to", "as_of_from", "as_of_to"])


@dataclass
class BitemporalScenario:

    id: str
    data: Callable[[], Tuple[List, List, Tuple]]
    update_mode: Literal["delta", "full_state"]
