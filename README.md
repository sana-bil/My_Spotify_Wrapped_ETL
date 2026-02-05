# My Spotify Wrapped ETL Pipeline

A Python-based ETL (Extract, Transform, Load) pipeline that processes your Spotify Extended Streaming History data and transforms it into structured, analytics-ready tables for comprehensive music listening insights.

## 📊 Project Overview

This project automates the transformation of raw Spotify JSON streaming history into multi-dimensional analytics tables, enabling detailed analysis of your music listening habits through Power BI dashboards. Unlike waiting for Spotify's annual Wrapped, this pipeline lets you analyze your listening data anytime with custom metrics and visualizations.

## ✨ Features

- **Automated Data Processing**: Converts complex JSON streaming history into clean, structured CSV files
- **Multi-dimensional Analytics**: Generates separate dimension and fact tables optimized for business intelligence
- **Power BI Ready**: Outputs are pre-formatted for seamless integration with Power BI dashboards
- **Extended History Support**: Works with Spotify's Extended Streaming History (not just the standard account data)
- **Comprehensive Metrics**: Enables analysis of listening patterns, artist preferences, time-based trends, and more

## 🏗️ Architecture

The pipeline follows a classic ETL architecture:

1. **Extract**: Reads Spotify Extended Streaming History JSON files
2. **Transform**: 
   - Cleans and validates data
   - Creates dimensional models (artists, tracks, time dimensions)
   - Calculates aggregate metrics
   - Handles data quality issues
3. **Load**: Outputs structured CSV files ready for analysis

## 📁 Project Structure

```
My_Spotify_Wrapped_ETL/
│
├── spotify_etl.py                              # Main ETL script
├── Streaming_History_Audio_2024-2025_1.json   # Sample streaming data
├── my_spotify_data.zip                         # Complete data archive
│
├── Spotify Extended Streaming History/         # Source data folder
│   └── [JSON files from Spotify]
│
├── spotify_analytics_output/                   # Generated analytics tables
│   └── [CSV output files]
│
└── Spotify_2025_Analytics.pbix                 # Power BI dashboard file
```

## 🚀 Getting Started

### Prerequisites

- Python 3.7+
- pandas
- json (standard library)
- Your Spotify Extended Streaming History data

### Requesting Your Spotify Data

1. Log into your Spotify account at [spotify.com/account](https://www.spotify.com/account)
2. Navigate to "Privacy Settings"
3. Scroll to "Download your data"
4. Request **Extended Streaming History** (not just Account Data)
5. Wait 5-30 days for Spotify to prepare your data
6. Download the ZIP file when ready

### Installation

1. Clone the repository:
```bash
git clone https://github.com/sana-bil/My_Spotify_Wrapped_ETL.git
cd My_Spotify_Wrapped_ETL
```

2. Install required dependencies:
```bash
pip install pandas
```

3. Place your Spotify Extended Streaming History JSON files in the appropriate directory

### Usage

1. Extract your Spotify data and place JSON files in the `Spotify Extended Streaming History/` folder

2. Run the ETL pipeline:
```bash
python spotify_etl.py
```

3. Find the generated analytics tables in the `spotify_analytics_output/` directory

4. Open `Spotify_2025_Analytics.pbix` in Power BI Desktop to visualize your data

## 📈 Output Tables

The pipeline generates several analytics tables (examples may include):

- **Fact Tables**: 
  - Streaming facts with timestamps, duration, and context
  - Play counts and listening metrics

- **Dimension Tables**:
  - Artists dimension with metadata
  - Tracks dimension with audio features
  - Time dimension (hour, day, month, year)
  - Platform/device dimension

## 🎯 Use Cases

Analyze your music listening habits to discover:

- Most played artists and tracks
- Listening patterns by time of day, day of week, or season
- Evolution of music taste over time
- Platform usage (mobile, desktop, web)
- Skip rates and track completion metrics
- Podcast vs. music consumption breakdown

## 🔍 Data Privacy

This project processes your personal Spotify data **locally on your machine**. Your listening history never leaves your computer unless you choose to share it. The pipeline respects your privacy by:

- Processing data entirely offline
- Not requiring API keys or authentication
- Not uploading data to any external services

## 📊 Power BI Dashboard

The included `Spotify_2025_Analytics.pbix` file contains pre-built visualizations:

- Top artists and tracks leaderboards
- Listening trends over time
- Heatmaps of listening by hour/day
- Genre and artist diversity metrics
- Monthly and yearly comparisons

## 🛠️ Customization

You can modify `spotify_etl.py` to:

- Add custom metrics or calculations
- Filter data by date ranges or specific artists
- Create additional dimension tables
- Export to different formats (SQL, Parquet, etc.)

## 🤝 Contributing

Contributions are welcome! Feel free to:

- Report bugs or issues
- Suggest new features or metrics
- Submit pull requests with improvements
- Share your dashboard designs

## 📝 License

This project is open source and available under the MIT License.

## 🙏 Acknowledgments

- Spotify for providing Extended Streaming History data
- The Python data analysis community
- Power BI for visualization capabilities

## 📧 Contact

For questions or feedback, please open an issue on GitHub.

---

**Note**: This is an independent project and is not affiliated with or endorsed by Spotify.
