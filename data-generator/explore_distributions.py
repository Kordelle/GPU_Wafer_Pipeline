# ===================================
# Simple Exploration of Statistical Distributions
# ===================================


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
    normal_temp = np.random.normal(loc=350, scale=5, size=sample_size)
    uniform_yield = np.random.uniform(low=0.85, high=0.99, size=sample_size)
    poisson_defects = np.random.poisson(lam=2, size=sample_size)

    # Create figure with 3 subplots
    fig, axes = plt.subplots(2, 3, figsize=(18,10))
    fig.suptitle('Manufacturing Telemetry: Statistical Distributions',
                 fontsize=16, fontweight='bold')
    
    # ==============================
    # ROW 1: HISTOGRAMS (Shape of Distribution)
    # ==============================

    # Normal Distribution: Temperature
    axes[0, 0].hist(normal_temp, bins=50, edgecolor='black', 
                    color='steelblue', alpha=0.7)
    axes[0, 0].axvline(350, color='red', linestyle='--', 
                       linewidth=2, label='Target (350°C)')
    axes[0, 0].axvline(345, color='orange', linestyle=':', 
                       linewidth=1.5, label='-1σ (345°C)')
    axes[0, 0].axvline(355, color='orange', linestyle=':', 
                       linewidth=1.5, label='+1σ (355°C)')
    axes[0, 0].set_title('Normal Distribution: Temperature', fontweight='bold')
    axes[0, 0].set_xlabel('Temperature (°C)')
    axes[0, 0].set_ylabel('Frequency')
    axes[0, 0].legend()
    axes[0, 0].grid(True, alpha=0.3)
    
    # Uniform Distribution: Yield Rate
    axes[0, 1].hist(uniform_yield, bins=50, edgecolor='black', 
                    color='green', alpha=0.7)
    axes[0, 1].set_title('Uniform Distribution: Yield Rate', fontweight='bold')
    axes[0, 1].set_xlabel('Yield Rate')
    axes[0, 1].set_ylabel('Frequency')
    axes[0, 1].grid(True, alpha=0.3)

    # Poisson Distribution: Defect Count
    axes[0, 2].hist(poisson_defects, bins=range(0, 12), edgecolor='black', 
                    color='coral', alpha=0.7, rwidth=0.8)
    axes[0, 2].axvline(2, color='red', linestyle='--', 
                       linewidth=2, label='Expected (λ=2)')
    axes[0, 2].set_title('Poisson Distribution: Defect Count', fontweight='bold')
    axes[0, 2].set_xlabel('Number of Defects')
    axes[0, 2].set_ylabel('Frequency')
    axes[0, 2].legend()
    axes[0, 2].grid(True, alpha=0.3)

    # ==============================
    # ROW 2: TIME SERIES (How It Changes Over Time)
    # ==============================

    # Temperature over time (with anamoly)
    time_series_temp = normal_temp[:200]
    # Inject an anomaly (equipment failure simulation)
    time_series_temp[150:160] = np.random.normal(370, 3, 10) # Spike!

    axes[1, 0].plot(time_series_temp, linewidth=1, color='steelblue')
    axes[1, 0].axhline(350, color='green', linestyle='--', 
                       linewidth=2, alpha=0.5, label='Target')
    axes[1, 0].axhline(365, color='red', linestyle='--', 
                       linewidth=2, alpha=0.5, label='Upper Control Limit (3σ)')
    axes[1, 0].axhline(335, color='red', linestyle='--', 
                       linewidth=2, alpha=0.5, label='Lower Control Limit (3σ)')
    axes[1, 0].fill_between(range(200), 345, 355, color='green', alpha=0.1)
    axes[1, 0].set_title('Temperature Time Series (with Anomaly)', fontweight='bold')
    axes[1, 0].set_xlabel('Wafer Number')
    axes[1, 0].set_ylabel('Temperature (°C)')
    axes[1, 0].legend(fontsize=8)
    axes[1, 0].grid(True, alpha=0.3)
    
    # Yield rate over time
    axes[1, 1].plot(uniform_yield[:200], linewidth=1, color='green', alpha=0.7)
    axes[1, 1].axhline(0.92, color='orange', linestyle='--', 
                       linewidth=2, label='Target (92%)')
    axes[1, 1].set_title('Yield Rate Time Series', fontweight='bold')
    axes[1, 1].set_xlabel('Wafer Number')
    axes[1, 1].set_ylabel('Yield Rate')
    axes[1, 1].legend()
    axes[1, 1].grid(True, alpha=0.3)
    
    # Defect count over time
    axes[1, 2].bar(range(200), poisson_defects[:200], color='coral', alpha=0.7)
    axes[1, 2].axhline(2, color='red', linestyle='--', 
                       linewidth=2, label='Expected (2 defects)')
    axes[1, 2].set_title('Defect Count Time Series', fontweight='bold')
    axes[1, 2].set_xlabel('Wafer Number')
    axes[1, 2].set_ylabel('Defect Count')
    axes[1, 2].legend()
    axes[1, 2].grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig('/output/distribution_analysis.png', dpi=300, bbox_inches='tight')
    print("✅ Saved visualization to /output/distribution_analysis.png")
    
    # Print statistics
    print("\n" + "="*60)
    print("STATISTICAL SUMMARY")
    print("="*60)
    
    print("\n📊 NORMAL DISTRIBUTION (Temperature)")
    print(f"   Mean: {normal_temp.mean():.2f}°C (target: 350°C)")
    print(f"   Std Dev: {normal_temp.std():.2f}°C (expected: 5°C)")
    print(f"   68% range: [{normal_temp.mean()-normal_temp.std():.2f}, "
          f"{normal_temp.mean()+normal_temp.std():.2f}]")
    print(f"   95% range: [{normal_temp.mean()-2*normal_temp.std():.2f}, "
          f"{normal_temp.mean()+2*normal_temp.std():.2f}]")
    print(f"   Min: {normal_temp.min():.2f}°C | Max: {normal_temp.max():.2f}°C")
    
    print("\n📊 UNIFORM DISTRIBUTION (Yield Rate)")
    print(f"   Mean: {uniform_yield.mean():.4f} (expected: 0.92)")
    print(f"   Std Dev: {uniform_yield.std():.4f}")
    print(f"   Min: {uniform_yield.min():.4f} | Max: {uniform_yield.max():.4f}")
    print(f"   Range: [{0.85}, {0.99}] (perfectly flat distribution)")
    
    print("\n📊 POISSON DISTRIBUTION (Defect Count)")
    print(f"   Mean: {poisson_defects.mean():.2f} (lambda: 2)")
    print(f"   Std Dev: {poisson_defects.std():.2f} (for Poisson: std ≈ √λ)")
    print(f"   Min: {poisson_defects.min()} | Max: {poisson_defects.max()}")
    print(f"   Mode: {stats.mode(poisson_defects, keepdims=True).mode[0]} (most common)")
    
    # Distribution of defect counts
    unique, counts = np.unique(poisson_defects, return_counts=True)
    print("\n   Defect Distribution:")
    for defect_count, frequency in zip(unique, counts):
        percentage = (frequency / sample_size) * 100
        print(f"   {defect_count} defects: {frequency} wafers ({percentage:.1f}%)")

def demonstrate_anomaly_detection():
    """
    Show how to detect anomalies using statistical methods
    """
    print("\n" + "="*60)
    print("ANOMALY DETECTION DEMONSTRATION")
    print("="*60)
    
    # Generate normal temperature data
    normal_temps = np.random.normal(350, 5, 1000)
    
    # Inject anomalies (equipment failure)
    anomalous_temps = normal_temps.copy()
    anomalous_temps[500:510] = np.random.normal(370, 2, 10)  # Hot spike
    anomalous_temps[750:760] = np.random.normal(330, 2, 10)  # Cold spike
    
    # Calculate control limits
    mean = normal_temps.mean()
    std = normal_temps.std()
    upper_control = mean + 3 * std
    lower_control = mean - 3 * std
    
    # Detect anomalies
    anomalies = (anomalous_temps > upper_control) | (anomalous_temps < lower_control)
    
    print(f"\n📈 Control Limits (3-Sigma Rule):")
    print(f"   Mean: {mean:.2f}°C")
    print(f"   Standard Deviation: {std:.2f}°C")
    print(f"   Upper Control Limit: {upper_control:.2f}°C")
    print(f"   Lower Control Limit: {lower_control:.2f}°C")
    
    print(f"\n🚨 Anomaly Detection Results:")
    print(f"   Total measurements: {len(anomalous_temps)}")
    print(f"   Anomalies detected: {anomalies.sum()}")
    print(f"   Anomaly rate: {(anomalies.sum()/len(anomalous_temps)*100):.2f}%")
    print(f"   Expected rate: 0.3% (for 3-sigma)")
    
    if anomalies.sum() > 0:
        print(f"\n   Anomalous temperature range: "
              f"{anomalous_temps[anomalies].min():.2f}°C - "
              f"{anomalous_temps[anomalies].max():.2f}°C")

if __name__ == "__main__":
    print("🔬 Manufacturing Data: Statistical Distribution Analysis")
    print("="*60)
    
    visualize_distributions()
    demonstrate_anomaly_detection()
    
    print("\n✅ Analysis complete! Check /output/distribution_analysis.png")
    