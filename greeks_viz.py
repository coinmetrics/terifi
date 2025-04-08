import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
from datetime import datetime

# Load the data
call_file = './market-greeks/deribit-BTC-13DEC24-100000-C-option.csv'
put_file = './market-greeks/deribit-BTC-13DEC24-100000-P-option.csv'

# Read CSV files
call_data = pd.read_csv(call_file)
put_data = pd.read_csv(put_file)

# Print column information for debugging
print("Call data sample:")
print(call_data.head(2))
print("\nCall data types:")
print(call_data.dtypes)

# Convert time to datetime
call_data['time'] = pd.to_datetime(call_data['time'])
put_data['time'] = pd.to_datetime(put_data['time'])

# Drop unnecessary columns to avoid issues with aggregation
columns_to_keep = ['time', 'vega', 'theta', 'rho', 'delta', 'gamma']
call_data = call_data[columns_to_keep]
put_data = put_data[columns_to_keep]

# Convert string values to numeric if needed
for col in ['vega', 'theta', 'rho', 'delta', 'gamma']:
    if call_data[col].dtype == 'object':
        call_data[col] = pd.to_numeric(call_data[col].replace('"', '', regex=True), errors='coerce')
        put_data[col] = pd.to_numeric(put_data[col].replace('"', '', regex=True), errors='coerce')

# Sample data at regular intervals (daily) for clearer visualization
call_daily = call_data.set_index('time').resample('D').mean().reset_index()
put_daily = put_data.set_index('time').resample('D').mean().reset_index()

# Set plot style
plt.style.use('ggplot')
plt.rcParams.update({'font.size': 11})

# Function to create plots
def create_plot(metric, call_data, put_data, ylabel, title_prefix, filename, ylim=None):
    plt.figure(figsize=(14, 7))
    
    # Plot call option data
    plt.plot(call_data['time'], call_data[metric], 'b-', label=f'Call {metric}', linewidth=2)
    
    # Plot put option data if provided
    if put_data is not None:
        plt.plot(put_data['time'], put_data[metric], 'r-', label=f'Put {metric}', linewidth=2)
    
    plt.title(f"{title_prefix} for BTC-13DEC24-100000 Options")
    plt.xlabel('Date')
    plt.ylabel(ylabel)
    plt.legend()
    
    # Format x-axis to show dates nicely
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=2))
    plt.xticks(rotation=45)
    
    if ylim:
        plt.ylim(ylim)
    
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"analysis/greeks_viz/{filename}.png")
    plt.close()

# Delta Visualization
create_plot('delta', call_daily, put_daily, 'Delta', 'Delta Evolution', 'delta')

# Gamma Visualization
create_plot('gamma', call_daily, put_daily, 'Gamma', 'Gamma Evolution', 'gamma')

# Vega Visualization
create_plot('vega', call_daily, put_daily, 'Vega', 'Vega Evolution', 'vega')

# Theta Visualization
create_plot('theta', call_daily, put_daily, 'Theta', 'Theta Evolution', 'theta')

# Rho Visualization
create_plot('rho', call_daily, put_daily, 'Rho', 'Rho Evolution', 'rho')

# Create a dashboard with all Greeks for call option
plt.figure(figsize=(18, 15))

# Delta subplot
plt.subplot(3, 2, 1)
plt.plot(call_daily['time'], call_daily['delta'], 'b-', linewidth=2)
plt.title('Call Option Delta')
plt.ylabel('Delta')
plt.xticks(rotation=45)
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=3))
plt.grid(True, alpha=0.3)

# Gamma subplot
plt.subplot(3, 2, 2)
plt.plot(call_daily['time'], call_daily['gamma'], 'g-', linewidth=2)
plt.title('Call Option Gamma')
plt.ylabel('Gamma')
plt.xticks(rotation=45)
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=3))
plt.grid(True, alpha=0.3)

# Vega subplot
plt.subplot(3, 2, 3)
plt.plot(call_daily['time'], call_daily['vega'], 'c-', linewidth=2)
plt.title('Call Option Vega')
plt.ylabel('Vega')
plt.xticks(rotation=45)
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=3))
plt.grid(True, alpha=0.3)

# Theta subplot
plt.subplot(3, 2, 4)
plt.plot(call_daily['time'], call_daily['theta'], 'r-', linewidth=2)
plt.title('Call Option Theta')
plt.ylabel('Theta')
plt.xticks(rotation=45)
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=3))
plt.grid(True, alpha=0.3)

# Rho subplot
plt.subplot(3, 2, 5)
plt.plot(call_daily['time'], call_daily['rho'], 'm-', linewidth=2)
plt.title('Call Option Rho')
plt.ylabel('Rho')
plt.xticks(rotation=45)
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=3))
plt.grid(True, alpha=0.3)

plt.suptitle('BTC-13DEC24-100000 Call Option Greeks Evolution', fontsize=16)
plt.tight_layout(rect=[0, 0, 1, 0.97])
plt.savefig("analysis/greeks_viz/call_option_dashboard.png")

# Create a combined dashboard with put-call comparison
greeks = ['delta', 'gamma', 'vega', 'theta', 'rho']
fig, axes = plt.subplots(len(greeks), 1, figsize=(16, 20))

for i, greek in enumerate(greeks):
    axes[i].plot(call_daily['time'], call_daily[greek], 'b-', label=f'Call {greek}', linewidth=2)
    axes[i].plot(put_daily['time'], put_daily[greek], 'r-', label=f'Put {greek}', linewidth=2)
    axes[i].set_title(f'{greek.capitalize()} Comparison')
    axes[i].set_ylabel(greek.capitalize())
    axes[i].legend()
    axes[i].grid(True, alpha=0.3)
    axes[i].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    axes[i].xaxis.set_major_locator(mdates.DayLocator(interval=3))
    axes[i].tick_params(axis='x', rotation=45)

plt.suptitle('BTC-13DEC24-100000 Options: Put vs Call Greeks Comparison', fontsize=16)
plt.tight_layout(rect=[0, 0, 1, 0.97])
plt.savefig("analysis/greeks_viz/put_call_comparison.png")

print("Visualizations created in analysis/greeks_viz/ directory")