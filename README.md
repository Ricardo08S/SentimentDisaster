# **SentimentDisaster: Spatio-Temporal Disaster Sentiment Analysis**

**SentimentDisaster** is a comprehensive research framework and toolset for scraping, analyzing, and visualizing disaster-related news data. Beyond simple data collection, it integrates **Large Language Models (LLMs)** for entity extraction and sentiment interpretation, using a **Knowledge Distillation** pipeline to train efficient, lightweight machine-learning models.

While this project focuses on disaster mitigation insights for the Bali region (Klungkung, Tabanan, Bangli), its methodology is fully scalable for national or global use.

---

## 🚀 **Features**

### **🔍 Multi-Source Web Scraping**
Robust, modular scrapers supporting major Indonesian news platforms:

- **Detik**
- **CNN Indonesia**
- **Mongabay**

Includes **smart chronological pagination** to retrieve historical data within a specified timeframe (e.g., Nov 2022 – Nov 2025).

---

### **🧠 LLM-Powered Entity Extraction**
Extracts structured disaster information using Google Gemini or DeepSeek:

- Disaster Type (*banjir, gempa, tanah longsor*, etc.)
- Village/District-level locations
- Casualties and infrastructure damage
- Government and community response details

---

### **💬 Sentiment Analysis via Knowledge Distillation**
A hybrid LLM + ML pipeline:

- **Teacher Model:**  
  LLM generates silver-standard sentiment labels (*Positive / Negative / Neutral*) focusing on government response and community mood.

- **Student Models:**  
  Trains efficient offline models using LLM-labeled data:  
  - **SVM (Linear Kernel)**
  - **Random Forest (Ensemble)**

This enables fast, reproducible sentiment classification without continuous LLM usage.

---

### **🗺️ Spatio-Temporal Visualization**
- Converts extracted locations into Geographic Coordinates (Lat/Lon)
- Exports **GeoJSON** for interactive mapping (e.g., **Kepler.gl**)

---

## 🛠️ **Installation**

This project uses **Poetry** for environment and dependency management.