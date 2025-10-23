import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy import stats

# Set style for professional plots
plt.style.use('seaborn-v0_8-darkgrid')
np.random.seed(42)

def visualize_distributions():
    """
    Generate and visualize the three key distributions used in manufacturing
    """
    sample_size = 1000

    # Generate samples
    normal_temp = np.random.normal(loc=75, scale=5, size=sample_size)
    uniform_yield = np.random.uniform(low=0.85, high=0.99, size=sample_size)
    poisson_defects = np.random.poisson(lam=2, size=sample_size)

    