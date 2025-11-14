import numpy as np
import pandas as pd
import mlflow

print("Setup OK!")
mlflow.start_run()
mlflow.log_param("test", 1)
mlflow.end_run()