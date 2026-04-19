# 🏋️ FitData Pipeline: Health Tracking ETL Engine

<div align="center">

![Python Version](https://img.shields.io/badge/python-3.11+-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)
![Data Stack](https://img.shields.io/badge/Data-Looker%20Studio-orange)
![APIs](https://img.shields.io/badge/Integrations-Renpho%20%7C%20Trainerize-red)

**An automated ETL (Extract, Transform, Load) pipeline that bridges Renpho Smart Scales and Trainerize compliance data into a robust centralized business intelligence dashboard.**

</div>

## 📖 Overview

FitData Pipeline solves the classic fragmented data problem for large-scale fitness coaching programs. It intelligently extracts historical weight trends from the **Renpho Health App**, extracts user compliance from **Trainerize**, and handles intelligent fuzzy-matching to merge client profiles into a clean, analytical **Google Sheets** format for enterprise reporting via **Looker Studio**.

## ✨ Key Features

* **📱 Multi-Platform Aggregation:** Pulls from Renpho (weights/body fat) and Trainerize (workouts/habits/cardio compliance).
* **🧠 Fuzzy-Matching Engine:** Automatically resolves spelling discrepancies and name mismatches across platforms (e.g., *Fatema* vs *Fatemah*).
* **⚙️ Dynamic Analytics:** Calculates starting weights, current losses, and progress percentages dynamically. 
* **🔄 Seamless Cloud Deployment:** Built for containerized or serverless execution (cron jobs, PythonAnywhere, AWS Lambda).
* **🔒 Secure & Encrypted:** Handles API traffic securely without exposing end-user passwords.

## 🛠️ Architecture

1. **Extractors (`src/extractors/`)**: Fetches paginated API requests from respective platforms.
2. **Utils (`src/utils/`)**: Authenticates via MITM-captured endpoints and connects to Google Service Accounts.
3. **Processors (`src/processors/`)**: The `merge_master.py` engine takes manual coach notes (Target Weights, Start Dates) and merges them with live API data.

## 🚀 Quick Start

### 1. Requirements
* Python 3.11+
* Google Cloud Platform (GCP) Service Account JSON (for Sheets API access)
* Valid Trainerize Group ID & API Token
* Centralized Renpho 'Friends List' master account.

### 2. Installation
```bash
git clone https://github.com/YourUsername/FitData-Pipeline.git
cd FitData-Pipeline
pip install -r requirements.txt
```

### 3. Configuration
Copy the sample environment file and fill in your credentials:
```bash
cp .env.example .env
```
Ensure your `credentials.json` (Google Service Account) is placed in the project root.

### 4. Run the Pipeline
Execute the master orchestrator to run the full Extractor-Processor sequence:
```bash
python run_pipeline.py
```

## 📊 Dashboard Display
*The output from this pipeline is designed specifically to power Looker Studio dashboards. Simply connect your output Google Sheet to Looker Studio to visualize active client lists, weight loss charts, and low-compliance warning indicators.*

## 🤝 Contributing
Contributions, issues, and feature requests are welcome! 
Feel free to check [issues page](https://github.com/YourUsername/FitData-Pipeline/issues).

## 📄 License
This project is [MIT](LICENSE) licensed.
